using System.Runtime.CompilerServices;

namespace SpillFlow;

/// <summary>Reports a durable checkpoint for a recoverable two-input ExceptBy operation.</summary>
/// <param name="Phase">The durable operation phase.</param>
/// <param name="SourceRecordsConsumed">The durable offset of the primary source.</param>
/// <param name="ExcludedKeysConsumed">The durable offset of the exclusion source.</param>
/// <param name="FileCount">The number of input runs or unconsumed partition pairs.</param>
/// <param name="OutputRecordsCheckpointed">The output records acknowledged inside the active pair.</param>
public sealed record SpillExceptByProgress(
    SpillOperationPhase Phase,
    long SourceRecordsConsumed,
    long ExcludedKeysConsumed,
    int FileCount,
    long OutputRecordsCheckpointed);

/// <summary>Recoverable external-memory ExceptBy operations.</summary>
public static class RecoverableExceptByExtensions
{
    private const int CheckpointVersion = 1;

    /// <summary>
    /// Returns one source value per key absent from a resumable exclusion source while checkpointing both inputs,
    /// paired partition construction, and at-least-once output progress.
    /// </summary>
    public static async IAsyncEnumerable<TSource> ExceptByRecoverableSpill<TSource, TKey>(
        this SpillSession session,
        Func<long, CancellationToken, IAsyncEnumerable<TSource>> sourceFactory,
        Func<long, CancellationToken, IAsyncEnumerable<TKey>> excludedKeysFactory,
        string operationId,
        Func<TSource, TKey> keySelector,
        IEqualityComparer<TKey>? comparer = null,
        int outputCheckpointInterval = 1_024,
        Action<SpillExceptByProgress>? checkpointed = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(sourceFactory);
        ArgumentNullException.ThrowIfNull(excludedKeysFactory);
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentOutOfRangeException.ThrowIfLessThan(outputCheckpointInterval, 1);
        if (operationId.Any(character => !(char.IsAsciiLetterOrDigit(character) || character is '-' or '_')))
            throw new ArgumentException("Operation IDs may contain only ASCII letters, digits, '-' and '_'.", nameof(operationId));

        comparer ??= EqualityComparer<TKey>.Default;
        var checkpointName = $"exceptby-{operationId}";
        var state = session.HasCheckpoint(checkpointName)
            ? await session.LoadCheckpointAsync<RecoverableExceptByState>(checkpointName, cancellationToken)
                .ConfigureAwait(false)
            : RecoverableExceptByState.New;
        ValidateState(state, session, operationId);
        CleanupOrphanFiles(session, operationId, ReferencedFiles(state));

        async ValueTask SaveAsync(RecoverableExceptByState next)
        {
            await session.SaveCheckpointAsync(checkpointName, next, cancellationToken).ConfigureAwait(false);
            state = next;
            checkpointed?.Invoke(new SpillExceptByProgress(
                state.Phase,
                state.SourceRecordsConsumed,
                state.ExcludedKeysConsumed,
                state.SourceRuns.Length + state.ExcludedRuns.Length +
                Math.Max(0, state.Pairs.Length - state.OutputPairIndex),
                state.OutputRecordsCheckpointed));
            cancellationToken.ThrowIfCancellationRequested();
        }

        string NewSourceRun() => session.NewFile($".exceptby-{operationId}.source-input");
        string NewExcludedRun() => session.NewFile($".exceptby-{operationId}.excluded-input");
        var leftPartitionExtension = $".exceptby-{operationId}.source-partition";
        var rightPartitionExtension = $".exceptby-{operationId}.excluded-partition";

        if (state.Phase == SpillOperationPhase.Completed)
            yield break;

        if (state.Phase == SpillOperationPhase.Reading)
        {
            var buffer = new List<TSource>();
            var consumed = state.SourceRecordsConsumed;
            long bytes = 0;

            async ValueTask FlushSourceRunAsync()
            {
                if (buffer.Count == 0)
                    return;
                var path = NewSourceRun();
                await using (var writer = new PooledRecordWriter<TSource>(path, session))
                    foreach (var item in buffer)
                        await writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
                await SaveAsync(state with
                {
                    SourceRecordsConsumed = consumed,
                    SourceRuns = state.SourceRuns.Append(Path.GetFileName(path)).ToArray()
                }).ConfigureAwait(false);
                buffer.Clear();
                bytes = 0;
            }

            var source = sourceFactory(state.SourceRecordsConsumed, cancellationToken)
                ?? throw new InvalidOperationException("The resumable source factory returned null.");
            await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                session.ReadRecord();
                buffer.Add(item);
                consumed++;
                bytes += RecordFile.EstimateSize(item, session) + 40;
                if (bytes >= session.Options.MemoryLimitBytes * 3 / 4)
                    await FlushSourceRunAsync().ConfigureAwait(false);
            }
            await FlushSourceRunAsync().ConfigureAwait(false);
            await SaveAsync(state with { Phase = SpillOperationPhase.ReadingSecondary }).ConfigureAwait(false);
        }

        if (state.Phase == SpillOperationPhase.ReadingSecondary)
        {
            var buffer = new List<TKey>();
            var consumed = state.ExcludedKeysConsumed;
            long bytes = 0;

            async ValueTask FlushExcludedRunAsync()
            {
                if (buffer.Count == 0)
                    return;
                var path = NewExcludedRun();
                await using (var writer = new PooledRecordWriter<TKey>(path, session))
                    foreach (var key in buffer)
                        await writer.WriteAsync(key, cancellationToken).ConfigureAwait(false);
                await SaveAsync(state with
                {
                    ExcludedKeysConsumed = consumed,
                    ExcludedRuns = state.ExcludedRuns.Append(Path.GetFileName(path)).ToArray()
                }).ConfigureAwait(false);
                buffer.Clear();
                bytes = 0;
            }

            var excludedKeys = excludedKeysFactory(state.ExcludedKeysConsumed, cancellationToken)
                ?? throw new InvalidOperationException("The resumable exclusion source factory returned null.");
            await foreach (var key in excludedKeys.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                session.ReadRecord();
                buffer.Add(key);
                consumed++;
                bytes += RecordFile.EstimateSize(key, session) + 40;
                if (bytes >= session.Options.MemoryLimitBytes * 3 / 4)
                    await FlushExcludedRunAsync().ConfigureAwait(false);
            }
            await FlushExcludedRunAsync().ConfigureAwait(false);
            await SaveAsync(state with { Phase = SpillOperationPhase.Partitioning }).ConfigureAwait(false);
        }

        if (state.Phase == SpillOperationPhase.Partitioning)
        {
            await using var leftPartitions = new PartitionWriter<TSource>(
                session, extension: leftPartitionExtension);
            await using var rightPartitions = new PartitionWriter<TKey>(
                session, extension: rightPartitionExtension);
            foreach (var inputName in state.SourceRuns)
                await foreach (var item in RecordFile.ReadAsync<TSource>(
                                   session.ResolveSessionFile(inputName), session, cancellationToken).ConfigureAwait(false))
                    await leftPartitions.WriteAsync(
                        StableHash.Partition(keySelector(item), session.Options.PartitionCount, comparer),
                        item, cancellationToken).ConfigureAwait(false);
            foreach (var inputName in state.ExcludedRuns)
                await foreach (var key in RecordFile.ReadAsync<TKey>(
                                   session.ResolveSessionFile(inputName), session, cancellationToken).ConfigureAwait(false))
                    await rightPartitions.WriteAsync(
                        StableHash.Partition(key, session.Options.PartitionCount, comparer),
                        key, cancellationToken).ConfigureAwait(false);
            await leftPartitions.DisposeAsync().ConfigureAwait(false);
            await rightPartitions.DisposeAsync().ConfigureAwait(false);

            var pairs = new List<RecoverableExceptByPair>();
            for (var index = 0; index < session.Options.PartitionCount; index++)
            {
                var left = File.Exists(leftPartitions.Paths[index]) ? leftPartitions.Paths[index] : null;
                var right = File.Exists(rightPartitions.Paths[index]) ? rightPartitions.Paths[index] : null;
                if (left is null && right is null)
                    continue;
                var refined = await PartitionPairRefiner.RefineAsync<TSource, TKey, TKey>(
                    left, right, session, keySelector, key => key, comparer,
                    nameof(ExceptByRecoverableSpill), materializeLeft: true, cancellationToken,
                    leftExtension: leftPartitionExtension, rightExtension: rightPartitionExtension)
                    .ConfigureAwait(false);
                pairs.AddRange(refined.Select(pair => new RecoverableExceptByPair(
                    pair.Left is null ? null : Path.GetFileName(pair.Left),
                    pair.Right is null ? null : Path.GetFileName(pair.Right))));
            }

            var inputs = state.SourceRuns.Concat(state.ExcludedRuns).Select(session.ResolveSessionFile).ToArray();
            await SaveAsync(state with
            {
                Phase = SpillOperationPhase.Output,
                SourceRuns = [],
                ExcludedRuns = [],
                Pairs = pairs.ToArray(),
                OutputPairIndex = 0,
                OutputRecordsCheckpointed = 0
            }).ConfigureAwait(false);
            foreach (var path in inputs)
                session.DeleteFile(path);
        }

        while (state.Phase == SpillOperationPhase.Output && state.OutputPairIndex < state.Pairs.Length)
        {
            var pairIndex = state.OutputPairIndex;
            var pair = state.Pairs[pairIndex];
            var left = pair.Left is null ? null : session.ResolveSessionFile(pair.Left);
            var right = pair.Right is null ? null : session.ResolveSessionFile(pair.Right);
            using var lease = session.AcquirePartitionsLease([left, right], nameof(ExceptByRecoverableSpill));
            var excluded = new HashSet<TKey>(comparer);
            var emitted = new HashSet<TKey>(comparer);
            if (right is not null)
                await foreach (var key in RecordFile.ReadAsync<TKey>(right, session, cancellationToken)
                                   .ConfigureAwait(false))
                    excluded.Add(key);

            var outputPosition = 0L;
            var delivered = state.OutputRecordsCheckpointed;
            if (left is not null)
                await foreach (var item in RecordFile.ReadAsync<TSource>(left, session, cancellationToken)
                                   .ConfigureAwait(false))
                {
                    var key = keySelector(item);
                    if (excluded.Contains(key) || !emitted.Add(key))
                        continue;
                    if (outputPosition++ < state.OutputRecordsCheckpointed)
                        continue;
                    yield return item;
                    delivered++;
                    if (delivered % outputCheckpointInterval == 0)
                        await SaveAsync(state with { OutputRecordsCheckpointed = delivered }).ConfigureAwait(false);
                }

            if (outputPosition < state.OutputRecordsCheckpointed)
                throw new InvalidDataException("The recoverable ExceptBy output checkpoint exceeds its partition output.");
            await SaveAsync(state with
            {
                OutputPairIndex = pairIndex + 1,
                OutputRecordsCheckpointed = 0
            }).ConfigureAwait(false);
            if (left is not null) session.DeleteFile(left);
            if (right is not null) session.DeleteFile(right);
        }

        if (state.Phase == SpillOperationPhase.Output)
            await SaveAsync(state with { Phase = SpillOperationPhase.Completed, Pairs = [] }).ConfigureAwait(false);
    }

    private static IEnumerable<string> ReferencedFiles(RecoverableExceptByState state) =>
        state.SourceRuns.Concat(state.ExcludedRuns).Concat(
            state.Pairs.Skip(state.OutputPairIndex).SelectMany(pair =>
                new[] { pair.Left, pair.Right }.Where(name => name is not null).Cast<string>()));

    private static void ValidateState(RecoverableExceptByState state, SpillSession session, string operationId)
    {
        if (state.Version != CheckpointVersion ||
            state.Phase is not (SpillOperationPhase.Reading or SpillOperationPhase.ReadingSecondary or
                SpillOperationPhase.Partitioning or SpillOperationPhase.Output or SpillOperationPhase.Completed) ||
            state.SourceRecordsConsumed < 0 || state.ExcludedKeysConsumed < 0 ||
            state.OutputPairIndex < 0 || state.OutputPairIndex > state.Pairs.Length ||
            state.OutputRecordsCheckpointed < 0)
            throw new InvalidDataException("Invalid recoverable ExceptBy checkpoint.");
        if (state.Phase == SpillOperationPhase.Reading &&
            (state.ExcludedKeysConsumed != 0 || state.ExcludedRuns.Length != 0 || state.Pairs.Length != 0))
            throw new InvalidDataException("A primary-reading ExceptBy checkpoint contains later-phase state.");
        if (state.Phase is SpillOperationPhase.ReadingSecondary or SpillOperationPhase.Partitioning &&
            state.Pairs.Length != 0)
            throw new InvalidDataException("A pre-output ExceptBy checkpoint cannot reference partition pairs.");
        if (state.Phase == SpillOperationPhase.Output &&
            (state.SourceRuns.Length != 0 || state.ExcludedRuns.Length != 0))
            throw new InvalidDataException("An output-phase ExceptBy checkpoint cannot reference input runs.");
        if (state.Phase == SpillOperationPhase.Completed &&
            (state.SourceRuns.Length != 0 || state.ExcludedRuns.Length != 0 || state.Pairs.Length != 0))
            throw new InvalidDataException("A completed recoverable ExceptBy checkpoint cannot reference files.");

        var sourceInputSuffix = $".exceptby-{operationId}.source-input";
        var excludedInputSuffix = $".exceptby-{operationId}.excluded-input";
        var sourcePartitionSuffix = $".exceptby-{operationId}.source-partition";
        var excludedPartitionSuffix = $".exceptby-{operationId}.excluded-partition";
        if (state.SourceRuns.Any(name => !name.EndsWith(sourceInputSuffix, StringComparison.OrdinalIgnoreCase)) ||
            state.ExcludedRuns.Any(name => !name.EndsWith(excludedInputSuffix, StringComparison.OrdinalIgnoreCase)) ||
            state.Pairs.Any(pair =>
                (pair.Left is not null && !pair.Left.EndsWith(sourcePartitionSuffix, StringComparison.OrdinalIgnoreCase)) ||
                (pair.Right is not null && !pair.Right.EndsWith(excludedPartitionSuffix, StringComparison.OrdinalIgnoreCase)) ||
                pair is { Left: null, Right: null }))
            throw new InvalidDataException("A recoverable ExceptBy checkpoint contains an invalid file name.");
        var allFiles = state.SourceRuns.Concat(state.ExcludedRuns).Concat(state.Pairs.SelectMany(pair =>
            new[] { pair.Left, pair.Right }.Where(name => name is not null).Cast<string>())).ToArray();
        if (allFiles.Distinct(StringComparer.OrdinalIgnoreCase).Count() != allFiles.Length)
            throw new InvalidDataException("A recoverable ExceptBy checkpoint contains duplicate file names.");
        foreach (var fileName in ReferencedFiles(state))
            if (!File.Exists(session.ResolveSessionFile(fileName)))
                throw new InvalidDataException($"Recoverable ExceptBy file '{fileName}' is missing.");
    }

    private static void CleanupOrphanFiles(
        SpillSession session,
        string operationId,
        IEnumerable<string> referencedFiles)
    {
        var referenced = referencedFiles.ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var path in Directory.EnumerateFiles(session.WorkingDirectory, $"*.exceptby-{operationId}.*"))
            if (!referenced.Contains(Path.GetFileName(path)))
                session.DeleteFile(path);
    }

    private sealed record RecoverableExceptByPair(string? Left, string? Right);

    private sealed record RecoverableExceptByState(
        int Version,
        SpillOperationPhase Phase,
        long SourceRecordsConsumed,
        long ExcludedKeysConsumed,
        string[] SourceRuns,
        string[] ExcludedRuns,
        RecoverableExceptByPair[] Pairs,
        int OutputPairIndex,
        long OutputRecordsCheckpointed)
    {
        public static RecoverableExceptByState New { get; } =
            new(CheckpointVersion, SpillOperationPhase.Reading, 0, 0, [], [], [], 0, 0);
    }
}
