using System.Runtime.CompilerServices;

namespace SpillFlow;

/// <summary>Recoverable external-memory grouped aggregation operations.</summary>
public static class RecoverableGroupAggregateExtensions
{
    private const int CheckpointVersion = 1;

    /// <summary>
    /// Aggregates a resumable source by key while checkpointing input, partitioning, and at-least-once output.
    /// The selector and accumulator callbacks must be deterministic and free of externally visible side effects.
    /// </summary>
    public static async IAsyncEnumerable<TResult> GroupAggregateRecoverableSpill<TSource, TKey, TAccumulator, TResult>(
        this SpillSession session,
        Func<long, CancellationToken, IAsyncEnumerable<TSource>> sourceFactory,
        string operationId,
        Func<TSource, TKey> keySelector,
        Func<TAccumulator> seedFactory,
        Func<TAccumulator, TSource, TAccumulator> accumulate,
        Func<TKey, TAccumulator, TResult> resultSelector,
        IEqualityComparer<TKey>? comparer = null,
        int outputCheckpointInterval = 1_024,
        Action<SpillOperationProgress>? checkpointed = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(sourceFactory);
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(seedFactory);
        ArgumentNullException.ThrowIfNull(accumulate);
        ArgumentNullException.ThrowIfNull(resultSelector);
        ArgumentOutOfRangeException.ThrowIfLessThan(outputCheckpointInterval, 1);
        if (operationId.Any(character => !(char.IsAsciiLetterOrDigit(character) || character is '-' or '_')))
            throw new ArgumentException("Operation IDs may contain only ASCII letters, digits, '-' and '_'.", nameof(operationId));

        comparer ??= EqualityComparer<TKey>.Default;
        var checkpointName = $"aggregate-{operationId}";
        var state = session.HasCheckpoint(checkpointName)
            ? await session.LoadCheckpointAsync<RecoverableGroupAggregateState>(checkpointName, cancellationToken)
                .ConfigureAwait(false)
            : RecoverableGroupAggregateState.New;
        ValidateState(state, session, operationId);
        CleanupOrphanFiles(session, operationId, ReferencedFiles(state));

        async ValueTask SaveAsync(RecoverableGroupAggregateState next)
        {
            await session.SaveCheckpointAsync(checkpointName, next, cancellationToken).ConfigureAwait(false);
            state = next;
            checkpointed?.Invoke(new SpillOperationProgress(
                state.Phase,
                state.RecordsConsumed,
                state.InputRuns.Length + Math.Max(0, state.Leaves.Length - state.OutputLeafIndex),
                state.OutputRecordsCheckpointed));
            cancellationToken.ThrowIfCancellationRequested();
        }

        string NewInputRun() => session.NewFile($".aggregate-{operationId}.input");
        var partitionExtension = $".aggregate-{operationId}.partition";

        if (state.Phase == SpillOperationPhase.Completed)
            yield break;

        if (state.Phase == SpillOperationPhase.Reading)
        {
            var buffer = new List<TSource>();
            var consumed = state.RecordsConsumed;
            long bytes = 0;

            async ValueTask FlushInputRunAsync()
            {
                if (buffer.Count == 0)
                    return;
                var path = NewInputRun();
                await using (var writer = new PooledRecordWriter<TSource>(path, session))
                    foreach (var item in buffer)
                        await writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
                await SaveAsync(state with
                {
                    RecordsConsumed = consumed,
                    InputRuns = state.InputRuns.Append(Path.GetFileName(path)).ToArray()
                }).ConfigureAwait(false);
                buffer.Clear();
                bytes = 0;
            }

            var source = sourceFactory(state.RecordsConsumed, cancellationToken)
                ?? throw new InvalidOperationException("The resumable source factory returned null.");
            await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                session.ReadRecord();
                buffer.Add(item);
                consumed++;
                bytes += RecordFile.EstimateSize(item, session) + 40;
                if (bytes >= session.Options.MemoryLimitBytes * 3 / 4)
                    await FlushInputRunAsync().ConfigureAwait(false);
            }
            await FlushInputRunAsync().ConfigureAwait(false);
            await SaveAsync(state with { Phase = SpillOperationPhase.Partitioning }).ConfigureAwait(false);
        }

        if (state.Phase == SpillOperationPhase.Partitioning)
        {
            await using var partitions = new PartitionWriter<TSource>(session, extension: partitionExtension);
            foreach (var inputName in state.InputRuns)
                await foreach (var item in RecordFile.ReadAsync<TSource>(
                                   session.ResolveSessionFile(inputName), session, cancellationToken).ConfigureAwait(false))
                    await partitions.WriteAsync(
                        StableHash.Partition(keySelector(item), session.Options.PartitionCount, comparer),
                        item, cancellationToken).ConfigureAwait(false);
            await partitions.DisposeAsync().ConfigureAwait(false);

            var leaves = new List<string>();
            foreach (var path in partitions.Paths)
            {
                if (!File.Exists(path))
                    continue;
                var refined = await PartitionRefiner.RefineAsync<TSource, TKey>(
                    path, session, keySelector, comparer, nameof(GroupAggregateRecoverableSpill), cancellationToken,
                    extension: partitionExtension).ConfigureAwait(false);
                leaves.AddRange(refined.Select(path => Path.GetFileName(path)!));
            }

            var inputs = state.InputRuns.Select(session.ResolveSessionFile).ToArray();
            await SaveAsync(state with
            {
                Phase = SpillOperationPhase.Output,
                InputRuns = [],
                Leaves = leaves.ToArray(),
                OutputLeafIndex = 0,
                OutputRecordsCheckpointed = 0
            }).ConfigureAwait(false);
            foreach (var path in inputs)
                session.DeleteFile(path);
        }

        while (state.Phase == SpillOperationPhase.Output && state.OutputLeafIndex < state.Leaves.Length)
        {
            var leafIndex = state.OutputLeafIndex;
            var path = session.ResolveSessionFile(state.Leaves[leafIndex]);
            using var lease = session.AcquirePartitionLease(path, nameof(GroupAggregateRecoverableSpill));
            var accumulators = new Dictionary<TKey, TAccumulator>(comparer);
            var keyOrder = new List<TKey>();
            await foreach (var item in RecordFile.ReadAsync<TSource>(path, session, cancellationToken)
                               .ConfigureAwait(false))
            {
                var key = keySelector(item);
                if (!accumulators.TryGetValue(key, out var current))
                {
                    current = seedFactory();
                    keyOrder.Add(key);
                }
                accumulators[key] = accumulate(current, item);
            }

            var outputPosition = 0L;
            var delivered = state.OutputRecordsCheckpointed;
            foreach (var key in keyOrder)
            {
                if (outputPosition++ < state.OutputRecordsCheckpointed)
                    continue;
                yield return resultSelector(key, accumulators[key]);
                delivered++;
                if (delivered % outputCheckpointInterval == 0)
                    await SaveAsync(state with { OutputRecordsCheckpointed = delivered }).ConfigureAwait(false);
            }

            if (outputPosition < state.OutputRecordsCheckpointed)
                throw new InvalidDataException("The recoverable aggregation output checkpoint exceeds its partition output.");
            await SaveAsync(state with
            {
                OutputLeafIndex = leafIndex + 1,
                OutputRecordsCheckpointed = 0
            }).ConfigureAwait(false);
            session.DeleteFile(path);
        }

        if (state.Phase == SpillOperationPhase.Output)
            await SaveAsync(state with { Phase = SpillOperationPhase.Completed, Leaves = [] }).ConfigureAwait(false);
    }

    private static IEnumerable<string> ReferencedFiles(RecoverableGroupAggregateState state) =>
        state.InputRuns.Concat(state.Leaves.Skip(state.OutputLeafIndex));

    private static void ValidateState(
        RecoverableGroupAggregateState state,
        SpillSession session,
        string operationId)
    {
        if (state.Version != CheckpointVersion ||
            state.Phase is not (SpillOperationPhase.Reading or SpillOperationPhase.Partitioning or
                SpillOperationPhase.Output or SpillOperationPhase.Completed) ||
            state.RecordsConsumed < 0 || state.OutputLeafIndex < 0 || state.OutputLeafIndex > state.Leaves.Length ||
            state.OutputRecordsCheckpointed < 0)
            throw new InvalidDataException("Invalid recoverable aggregation checkpoint.");
        if (state.Phase == SpillOperationPhase.Reading && (state.Leaves.Length != 0 || state.OutputLeafIndex != 0))
            throw new InvalidDataException("A reading-phase aggregation checkpoint cannot reference output partitions.");
        if (state.Phase == SpillOperationPhase.Partitioning && state.Leaves.Length != 0)
            throw new InvalidDataException("A partitioning-phase aggregation checkpoint cannot reference output partitions.");
        if (state.Phase == SpillOperationPhase.Output && state.InputRuns.Length != 0)
            throw new InvalidDataException("An output-phase aggregation checkpoint cannot reference input runs.");
        if (state.Phase == SpillOperationPhase.Completed &&
            (state.InputRuns.Length != 0 || state.Leaves.Length != 0))
            throw new InvalidDataException("A completed recoverable aggregation checkpoint cannot reference files.");

        var inputSuffix = $".aggregate-{operationId}.input";
        var partitionSuffix = $".aggregate-{operationId}.partition";
        if (state.InputRuns.Any(name => !name.EndsWith(inputSuffix, StringComparison.OrdinalIgnoreCase)) ||
            state.Leaves.Any(name => !name.EndsWith(partitionSuffix, StringComparison.OrdinalIgnoreCase)))
            throw new InvalidDataException("A recoverable aggregation checkpoint contains an invalid file name.");
        var allFiles = state.InputRuns.Concat(state.Leaves).ToArray();
        if (allFiles.Distinct(StringComparer.OrdinalIgnoreCase).Count() != allFiles.Length)
            throw new InvalidDataException("A recoverable aggregation checkpoint contains duplicate file names.");
        foreach (var fileName in ReferencedFiles(state))
            if (!File.Exists(session.ResolveSessionFile(fileName)))
                throw new InvalidDataException($"Recoverable aggregation file '{fileName}' is missing.");
    }

    private static void CleanupOrphanFiles(
        SpillSession session,
        string operationId,
        IEnumerable<string> referencedFiles)
    {
        var referenced = referencedFiles.ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var path in Directory.EnumerateFiles(session.WorkingDirectory, $"*.aggregate-{operationId}.*"))
            if (!referenced.Contains(Path.GetFileName(path)))
                session.DeleteFile(path);
    }

    private sealed record RecoverableGroupAggregateState(
        int Version,
        SpillOperationPhase Phase,
        long RecordsConsumed,
        string[] InputRuns,
        string[] Leaves,
        int OutputLeafIndex,
        long OutputRecordsCheckpointed)
    {
        public static RecoverableGroupAggregateState New { get; } =
            new(CheckpointVersion, SpillOperationPhase.Reading, 0, [], [], 0, 0);
    }
}
