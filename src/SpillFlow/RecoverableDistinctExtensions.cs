using System.Runtime.CompilerServices;

namespace SpillFlow;

/// <summary>Recoverable external-memory distinct operations.</summary>
public static class RecoverableDistinctExtensions
{
    private const int CheckpointVersion = 1;

    /// <summary>
    /// Returns the first source value for each distinct key while checkpointing input, partitioning, and output.
    /// Output recovery is at-least-once and may replay up to <paramref name="outputCheckpointInterval"/> records.
    /// </summary>
    public static async IAsyncEnumerable<TSource> DistinctRecoverableSpill<TSource, TKey>(
        this SpillSession session,
        Func<long, CancellationToken, IAsyncEnumerable<TSource>> sourceFactory,
        string operationId,
        Func<TSource, TKey> keySelector,
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
        ArgumentOutOfRangeException.ThrowIfLessThan(outputCheckpointInterval, 1);
        if (operationId.Any(character => !(char.IsAsciiLetterOrDigit(character) || character is '-' or '_')))
            throw new ArgumentException("Operation IDs may contain only ASCII letters, digits, '-' and '_'.", nameof(operationId));

        comparer ??= EqualityComparer<TKey>.Default;
        var checkpointName = $"distinct-{operationId}";
        var state = session.HasCheckpoint(checkpointName)
            ? await session.LoadCheckpointAsync<RecoverableDistinctState>(checkpointName, cancellationToken)
                .ConfigureAwait(false)
            : RecoverableDistinctState.New;
        ValidateState(state, session, operationId);
        CleanupOrphanFiles(session, operationId, ReferencedFiles(state));

        async ValueTask SaveAsync(RecoverableDistinctState next)
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

        string NewInputRun() => session.NewFile($".distinct-{operationId}.input");
        var partitionExtension = $".distinct-{operationId}.partition";

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
            {
                var inputPath = session.ResolveSessionFile(inputName);
                await foreach (var item in RecordFile.ReadAsync<TSource>(inputPath, session, cancellationToken)
                                   .ConfigureAwait(false))
                {
                    var partition = StableHash.Partition(
                        keySelector(item), session.Options.PartitionCount, comparer);
                    await partitions.WriteAsync(partition, item, cancellationToken).ConfigureAwait(false);
                }
            }
            await partitions.DisposeAsync().ConfigureAwait(false);

            var leaves = new List<string>();
            foreach (var path in partitions.Paths)
            {
                if (!File.Exists(path))
                    continue;
                var refined = await PartitionRefiner.RefineAsync<TSource, TKey>(
                    path, session, keySelector, comparer, nameof(DistinctRecoverableSpill), cancellationToken,
                    extension: partitionExtension).ConfigureAwait(false);
                leaves.AddRange(refined.Select(Path.GetFileName)!);
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
            using var lease = session.AcquirePartitionLease(path, nameof(DistinctRecoverableSpill));
            var seen = new HashSet<TKey>(comparer);
            var uniquePosition = 0L;
            var delivered = state.OutputRecordsCheckpointed;

            await foreach (var item in RecordFile.ReadAsync<TSource>(path, session, cancellationToken)
                               .ConfigureAwait(false))
            {
                if (!seen.Add(keySelector(item)))
                    continue;
                if (uniquePosition++ < state.OutputRecordsCheckpointed)
                    continue;
                yield return item;
                delivered++;
                if (delivered % outputCheckpointInterval == 0)
                    await SaveAsync(state with { OutputRecordsCheckpointed = delivered }).ConfigureAwait(false);
            }

            if (uniquePosition < state.OutputRecordsCheckpointed)
                throw new InvalidDataException("The recoverable Distinct output checkpoint exceeds its partition output.");

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

    private static IEnumerable<string> ReferencedFiles(RecoverableDistinctState state) =>
        state.InputRuns.Concat(state.Leaves.Skip(state.OutputLeafIndex));

    private static void ValidateState(RecoverableDistinctState state, SpillSession session, string operationId)
    {
        if (state.Version != CheckpointVersion ||
            state.Phase is not (SpillOperationPhase.Reading or SpillOperationPhase.Partitioning or
                SpillOperationPhase.Output or SpillOperationPhase.Completed) ||
            state.RecordsConsumed < 0 ||
            state.OutputLeafIndex < 0 || state.OutputLeafIndex > state.Leaves.Length ||
            state.OutputRecordsCheckpointed < 0)
            throw new InvalidDataException("Invalid recoverable Distinct checkpoint.");
        if (state.Phase == SpillOperationPhase.Reading && (state.Leaves.Length != 0 || state.OutputLeafIndex != 0))
            throw new InvalidDataException("A reading-phase Distinct checkpoint cannot reference output partitions.");
        if (state.Phase == SpillOperationPhase.Partitioning && state.Leaves.Length != 0)
            throw new InvalidDataException("A partitioning-phase Distinct checkpoint cannot reference output partitions.");
        if (state.Phase == SpillOperationPhase.Output && state.InputRuns.Length != 0)
            throw new InvalidDataException("An output-phase Distinct checkpoint cannot reference input runs.");
        if (state.Phase == SpillOperationPhase.Completed &&
            (state.InputRuns.Length != 0 || state.Leaves.Length != 0))
            throw new InvalidDataException("A completed recoverable Distinct checkpoint cannot reference files.");

        var inputSuffix = $".distinct-{operationId}.input";
        var partitionSuffix = $".distinct-{operationId}.partition";
        if (state.InputRuns.Any(name => !name.EndsWith(inputSuffix, StringComparison.OrdinalIgnoreCase)) ||
            state.Leaves.Any(name => !name.EndsWith(partitionSuffix, StringComparison.OrdinalIgnoreCase)))
            throw new InvalidDataException("A recoverable Distinct checkpoint contains an invalid file name.");
        var allFiles = state.InputRuns.Concat(state.Leaves).ToArray();
        if (allFiles.Distinct(StringComparer.OrdinalIgnoreCase).Count() != allFiles.Length)
            throw new InvalidDataException("A recoverable Distinct checkpoint contains duplicate file names.");
        var referenced = ReferencedFiles(state).ToArray();
        foreach (var fileName in referenced)
            if (!File.Exists(session.ResolveSessionFile(fileName)))
                throw new InvalidDataException($"Recoverable Distinct file '{fileName}' is missing.");
    }

    private static void CleanupOrphanFiles(
        SpillSession session,
        string operationId,
        IEnumerable<string> referencedFiles)
    {
        var referenced = referencedFiles.ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var path in Directory.EnumerateFiles(session.WorkingDirectory, $"*.distinct-{operationId}.*"))
            if (!referenced.Contains(Path.GetFileName(path)))
                session.DeleteFile(path);
    }

    private sealed record RecoverableDistinctState(
        int Version,
        SpillOperationPhase Phase,
        long RecordsConsumed,
        string[] InputRuns,
        string[] Leaves,
        int OutputLeafIndex,
        long OutputRecordsCheckpointed)
    {
        public static RecoverableDistinctState New { get; } =
            new(CheckpointVersion, SpillOperationPhase.Reading, 0, [], [], 0, 0);
    }
}
