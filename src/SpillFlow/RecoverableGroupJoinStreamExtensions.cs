using System.Runtime.CompilerServices;

namespace SpillFlow;

/// <summary>Reports a durable checkpoint for a recoverable two-input streaming group join.</summary>
public sealed record SpillGroupJoinProgress(
    SpillOperationPhase Phase,
    long LeftRecordsConsumed,
    long RightRecordsConsumed,
    int FileCount,
    long OutputRecordsCheckpointed);

/// <summary>Recoverable external-memory streaming group joins.</summary>
public static class RecoverableGroupJoinStreamExtensions
{
    private const int CheckpointVersion = 1;

    /// <summary>
    /// Correlates a resumable left source with a resumable right source while keeping oversized match groups on disk.
    /// The result selector must consume its match stream before returning and must tolerate at-least-once replay.
    /// </summary>
    public static async IAsyncEnumerable<TResult> GroupJoinStreamRecoverableSpill<TLeft, TRight, TKey, TResult>(
        this SpillSession session,
        Func<long, CancellationToken, IAsyncEnumerable<TLeft>> leftSourceFactory,
        Func<long, CancellationToken, IAsyncEnumerable<TRight>> rightSourceFactory,
        string operationId,
        Func<TLeft, TKey> leftKeySelector,
        Func<TRight, TKey> rightKeySelector,
        Func<TLeft, IAsyncEnumerable<TRight>, CancellationToken, ValueTask<TResult>> resultSelector,
        IEqualityComparer<TKey>? comparer = null,
        int outputCheckpointInterval = 1_024,
        Action<SpillGroupJoinProgress>? checkpointed = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(leftSourceFactory);
        ArgumentNullException.ThrowIfNull(rightSourceFactory);
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentNullException.ThrowIfNull(leftKeySelector);
        ArgumentNullException.ThrowIfNull(rightKeySelector);
        ArgumentNullException.ThrowIfNull(resultSelector);
        ArgumentOutOfRangeException.ThrowIfLessThan(outputCheckpointInterval, 1);
        if (operationId.Any(character => !(char.IsAsciiLetterOrDigit(character) || character is '-' or '_')))
            throw new ArgumentException("Operation IDs may contain only ASCII letters, digits, '-' and '_'.", nameof(operationId));

        comparer ??= EqualityComparer<TKey>.Default;
        var checkpointName = $"groupjoin-{operationId}";
        var state = session.HasCheckpoint(checkpointName)
            ? await session.LoadCheckpointAsync<RecoverableGroupJoinState>(checkpointName, cancellationToken)
                .ConfigureAwait(false)
            : RecoverableGroupJoinState.New;
        ValidateState(state, session, operationId);
        CleanupOrphanFiles(session, operationId, ReferencedFiles(state));

        async ValueTask SaveAsync(RecoverableGroupJoinState next)
        {
            await session.SaveCheckpointAsync(checkpointName, next, cancellationToken).ConfigureAwait(false);
            state = next;
            checkpointed?.Invoke(new SpillGroupJoinProgress(
                state.Phase,
                state.LeftRecordsConsumed,
                state.RightRecordsConsumed,
                state.LeftRuns.Length + state.RightRuns.Length +
                Math.Max(0, state.Pairs.Length - state.OutputPairIndex),
                state.OutputRecordsCheckpointed));
            cancellationToken.ThrowIfCancellationRequested();
        }

        string NewLeftRun() => session.NewFile($".groupjoin-{operationId}.left-input");
        string NewRightRun() => session.NewFile($".groupjoin-{operationId}.right-input");
        var leftPartitionExtension = $".groupjoin-{operationId}.left-partition";
        var rightPartitionExtension = $".groupjoin-{operationId}.right-partition";

        if (state.Phase == SpillOperationPhase.Completed)
            yield break;

        if (state.Phase == SpillOperationPhase.Reading)
        {
            var buffer = new List<TLeft>();
            var consumed = state.LeftRecordsConsumed;
            long bytes = 0;

            async ValueTask FlushLeftRunAsync()
            {
                if (buffer.Count == 0)
                    return;
                var path = NewLeftRun();
                await using (var writer = new PooledRecordWriter<TLeft>(path, session))
                    foreach (var item in buffer)
                        await writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
                await SaveAsync(state with
                {
                    LeftRecordsConsumed = consumed,
                    LeftRuns = state.LeftRuns.Append(Path.GetFileName(path)).ToArray()
                }).ConfigureAwait(false);
                buffer.Clear();
                bytes = 0;
            }

            var leftSource = leftSourceFactory(state.LeftRecordsConsumed, cancellationToken)
                ?? throw new InvalidOperationException("The resumable left-source factory returned null.");
            await foreach (var item in leftSource.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                session.ReadRecord();
                buffer.Add(item);
                consumed++;
                bytes += RecordFile.EstimateSize(item, session) + 40;
                if (bytes >= session.Options.MemoryLimitBytes * 3 / 4)
                    await FlushLeftRunAsync().ConfigureAwait(false);
            }
            await FlushLeftRunAsync().ConfigureAwait(false);
            await SaveAsync(state with { Phase = SpillOperationPhase.ReadingSecondary }).ConfigureAwait(false);
        }

        if (state.Phase == SpillOperationPhase.ReadingSecondary)
        {
            var buffer = new List<TRight>();
            var consumed = state.RightRecordsConsumed;
            long bytes = 0;

            async ValueTask FlushRightRunAsync()
            {
                if (buffer.Count == 0)
                    return;
                var path = NewRightRun();
                await using (var writer = new PooledRecordWriter<TRight>(path, session))
                    foreach (var item in buffer)
                        await writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
                await SaveAsync(state with
                {
                    RightRecordsConsumed = consumed,
                    RightRuns = state.RightRuns.Append(Path.GetFileName(path)).ToArray()
                }).ConfigureAwait(false);
                buffer.Clear();
                bytes = 0;
            }

            var rightSource = rightSourceFactory(state.RightRecordsConsumed, cancellationToken)
                ?? throw new InvalidOperationException("The resumable right-source factory returned null.");
            await foreach (var item in rightSource.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                session.ReadRecord();
                buffer.Add(item);
                consumed++;
                bytes += RecordFile.EstimateSize(item, session) + 40;
                if (bytes >= session.Options.MemoryLimitBytes * 3 / 4)
                    await FlushRightRunAsync().ConfigureAwait(false);
            }
            await FlushRightRunAsync().ConfigureAwait(false);
            await SaveAsync(state with { Phase = SpillOperationPhase.Partitioning }).ConfigureAwait(false);
        }

        if (state.Phase == SpillOperationPhase.Partitioning)
        {
            await using var leftPartitions = new PartitionWriter<TLeft>(session, extension: leftPartitionExtension);
            await using var rightPartitions = new PartitionWriter<TRight>(session, extension: rightPartitionExtension);
            foreach (var inputName in state.LeftRuns)
                await foreach (var item in RecordFile.ReadAsync<TLeft>(
                                   session.ResolveSessionFile(inputName), session, cancellationToken).ConfigureAwait(false))
                    await leftPartitions.WriteAsync(
                        StableHash.Partition(leftKeySelector(item), session.Options.PartitionCount, comparer),
                        item, cancellationToken).ConfigureAwait(false);
            foreach (var inputName in state.RightRuns)
                await foreach (var item in RecordFile.ReadAsync<TRight>(
                                   session.ResolveSessionFile(inputName), session, cancellationToken).ConfigureAwait(false))
                    await rightPartitions.WriteAsync(
                        StableHash.Partition(rightKeySelector(item), session.Options.PartitionCount, comparer),
                        item, cancellationToken).ConfigureAwait(false);
            await leftPartitions.DisposeAsync().ConfigureAwait(false);
            await rightPartitions.DisposeAsync().ConfigureAwait(false);

            var pairs = new List<RecoverableGroupJoinPair>();
            for (var index = 0; index < session.Options.PartitionCount; index++)
            {
                var left = File.Exists(leftPartitions.Paths[index]) ? leftPartitions.Paths[index] : null;
                var right = File.Exists(rightPartitions.Paths[index]) ? rightPartitions.Paths[index] : null;
                if (left is null && right is null)
                    continue;
                var refined = await PartitionPairRefiner.RefineAsync<TLeft, TRight, TKey>(
                    left, right, session, leftKeySelector, rightKeySelector, comparer,
                    nameof(GroupJoinStreamRecoverableSpill), materializeLeft: false, cancellationToken,
                    allowOversizedLeaf: true,
                    leftExtension: leftPartitionExtension, rightExtension: rightPartitionExtension)
                    .ConfigureAwait(false);
                pairs.AddRange(refined.Select(pair => new RecoverableGroupJoinPair(
                    pair.Left is null ? null : Path.GetFileName(pair.Left),
                    pair.Right is null ? null : Path.GetFileName(pair.Right))));
            }

            var inputs = state.LeftRuns.Concat(state.RightRuns).Select(session.ResolveSessionFile).ToArray();
            await SaveAsync(state with
            {
                Phase = SpillOperationPhase.Output,
                LeftRuns = [],
                RightRuns = [],
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
            var materializeRight = right is null || session.CanPartitionFit(right);
            Dictionary<TKey, List<TRight>>? lookup = null;
            using var lease = materializeRight && right is not null
                ? session.AcquirePartitionLease(right, nameof(GroupJoinStreamRecoverableSpill))
                : null;
            if (materializeRight && right is not null)
            {
                lookup = new Dictionary<TKey, List<TRight>>(comparer);
                await foreach (var item in RecordFile.ReadAsync<TRight>(right, session, cancellationToken)
                                   .ConfigureAwait(false))
                {
                    var key = rightKeySelector(item);
                    if (!lookup.TryGetValue(key, out var values))
                        lookup[key] = values = [];
                    values.Add(item);
                }
            }

            var delivered = state.OutputRecordsCheckpointed;
            if (left is not null)
            {
                IAsyncEnumerable<TResult> results;
                if (materializeRight)
                {
                    results = JoinMaterializedAsync(left, lookup, session, leftKeySelector, resultSelector,
                        state.OutputRecordsCheckpointed, cancellationToken);
                }
                else
                {
                    results = HotKeyJoinBatcher.JoinAsync(left, right!, session, leftKeySelector, rightKeySelector,
                        resultSelector, comparer, state.OutputRecordsCheckpointed, cancellationToken);
                }

                await foreach (var result in results.ConfigureAwait(false))
                {
                    yield return result;
                    delivered++;
                    if (delivered % outputCheckpointInterval == 0)
                        await SaveAsync(state with { OutputRecordsCheckpointed = delivered }).ConfigureAwait(false);
                }
            }

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

    private static async IAsyncEnumerable<T> EnumerateAsync<T>(
        IEnumerable<T> values,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var value in values)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return value;
        }
        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<TResult> JoinMaterializedAsync<TLeft, TRight, TKey, TResult>(
        string leftPath,
        Dictionary<TKey, List<TRight>>? lookup,
        SpillSession session,
        Func<TLeft, TKey> leftKeySelector,
        Func<TLeft, IAsyncEnumerable<TRight>, CancellationToken, ValueTask<TResult>> resultSelector,
        long skipLeftRecords,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where TKey : notnull
    {
        var leftPosition = 0L;
        await foreach (var item in RecordFile.ReadAsync<TLeft>(leftPath, session, cancellationToken)
                           .ConfigureAwait(false))
        {
            if (leftPosition++ < skipLeftRecords)
                continue;
            var key = leftKeySelector(item);
            var matches = EnumerateAsync(
                lookup is not null && lookup.TryGetValue(key, out var values) ? values : [], cancellationToken);
            yield return await resultSelector(item, matches, cancellationToken).ConfigureAwait(false);
        }
        if (leftPosition < skipLeftRecords)
            throw new InvalidDataException("The recoverable streaming-join checkpoint exceeds its left partition.");
    }

    private static async IAsyncEnumerable<TRight> ReadMatchesAsync<TRight, TKey>(
        string path,
        SpillSession session,
        TKey expectedKey,
        Func<TRight, TKey> keySelector,
        IEqualityComparer<TKey> comparer,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where TKey : notnull
    {
        await foreach (var item in RecordFile.ReadAsync<TRight>(path, session, cancellationToken).ConfigureAwait(false))
            if (comparer.Equals(expectedKey, keySelector(item)))
                yield return item;
    }

    private static IEnumerable<string> ReferencedFiles(RecoverableGroupJoinState state) =>
        state.LeftRuns.Concat(state.RightRuns).Concat(
            state.Pairs.Skip(state.OutputPairIndex).SelectMany(pair =>
                new[] { pair.Left, pair.Right }.Where(name => name is not null).Cast<string>()));

    private static void ValidateState(RecoverableGroupJoinState state, SpillSession session, string operationId)
    {
        if (state.Version != CheckpointVersion ||
            state.Phase is not (SpillOperationPhase.Reading or SpillOperationPhase.ReadingSecondary or
                SpillOperationPhase.Partitioning or SpillOperationPhase.Output or SpillOperationPhase.Completed) ||
            state.LeftRecordsConsumed < 0 || state.RightRecordsConsumed < 0 ||
            state.OutputPairIndex < 0 || state.OutputPairIndex > state.Pairs.Length ||
            state.OutputRecordsCheckpointed < 0)
            throw new InvalidDataException("Invalid recoverable streaming-join checkpoint.");
        if (state.Phase == SpillOperationPhase.Reading &&
            (state.RightRecordsConsumed != 0 || state.RightRuns.Length != 0 || state.Pairs.Length != 0))
            throw new InvalidDataException("A left-reading join checkpoint contains later-phase state.");
        if (state.Phase is SpillOperationPhase.ReadingSecondary or SpillOperationPhase.Partitioning &&
            state.Pairs.Length != 0)
            throw new InvalidDataException("A pre-output join checkpoint cannot reference partition pairs.");
        if (state.Phase == SpillOperationPhase.Output &&
            (state.LeftRuns.Length != 0 || state.RightRuns.Length != 0))
            throw new InvalidDataException("An output-phase join checkpoint cannot reference input runs.");
        if (state.Phase == SpillOperationPhase.Completed &&
            (state.LeftRuns.Length != 0 || state.RightRuns.Length != 0 || state.Pairs.Length != 0))
            throw new InvalidDataException("A completed recoverable join checkpoint cannot reference files.");

        var leftInputSuffix = $".groupjoin-{operationId}.left-input";
        var rightInputSuffix = $".groupjoin-{operationId}.right-input";
        var leftPartitionSuffix = $".groupjoin-{operationId}.left-partition";
        var rightPartitionSuffix = $".groupjoin-{operationId}.right-partition";
        if (state.LeftRuns.Any(name => !name.EndsWith(leftInputSuffix, StringComparison.OrdinalIgnoreCase)) ||
            state.RightRuns.Any(name => !name.EndsWith(rightInputSuffix, StringComparison.OrdinalIgnoreCase)) ||
            state.Pairs.Any(pair =>
                (pair.Left is not null && !pair.Left.EndsWith(leftPartitionSuffix, StringComparison.OrdinalIgnoreCase)) ||
                (pair.Right is not null && !pair.Right.EndsWith(rightPartitionSuffix, StringComparison.OrdinalIgnoreCase)) ||
                pair is { Left: null, Right: null }))
            throw new InvalidDataException("A recoverable streaming-join checkpoint contains an invalid file name.");
        var allFiles = state.LeftRuns.Concat(state.RightRuns).Concat(state.Pairs.SelectMany(pair =>
            new[] { pair.Left, pair.Right }.Where(name => name is not null).Cast<string>())).ToArray();
        if (allFiles.Distinct(StringComparer.OrdinalIgnoreCase).Count() != allFiles.Length)
            throw new InvalidDataException("A recoverable streaming-join checkpoint contains duplicate file names.");
        foreach (var fileName in ReferencedFiles(state))
            if (!File.Exists(session.ResolveSessionFile(fileName)))
                throw new InvalidDataException($"Recoverable streaming-join file '{fileName}' is missing.");
    }

    private static void CleanupOrphanFiles(
        SpillSession session,
        string operationId,
        IEnumerable<string> referencedFiles)
    {
        var referenced = referencedFiles.ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var path in Directory.EnumerateFiles(session.WorkingDirectory, $"*.groupjoin-{operationId}.*"))
            if (!referenced.Contains(Path.GetFileName(path)))
                session.DeleteFile(path);
    }

    private sealed record RecoverableGroupJoinPair(string? Left, string? Right);

    private sealed record RecoverableGroupJoinState(
        int Version,
        SpillOperationPhase Phase,
        long LeftRecordsConsumed,
        long RightRecordsConsumed,
        string[] LeftRuns,
        string[] RightRuns,
        RecoverableGroupJoinPair[] Pairs,
        int OutputPairIndex,
        long OutputRecordsCheckpointed)
    {
        public static RecoverableGroupJoinState New { get; } =
            new(CheckpointVersion, SpillOperationPhase.Reading, 0, 0, [], [], [], 0, 0);
    }
}
