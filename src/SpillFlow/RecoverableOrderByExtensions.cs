using System.Runtime.CompilerServices;

namespace SpillFlow;

/// <summary>Identifies a durable phase of a recoverable spill operation.</summary>
public enum SpillOperationPhase
{
    /// <summary>The operator is reading its resumable source and creating sorted runs.</summary>
    Reading,
    /// <summary>The operator is replacing groups of runs with durable merged runs.</summary>
    Merging,
    /// <summary>The operator is streaming its final durable output.</summary>
    Output,
    /// <summary>The operator completed and removed its temporary runs.</summary>
    Completed
}

/// <summary>Reports a durable recoverable-operation checkpoint.</summary>
/// <param name="Phase">The phase saved in the checkpoint.</param>
/// <param name="RecordsConsumed">The number of input records durably represented by runs.</param>
/// <param name="RunCount">The number of runs referenced by the checkpoint.</param>
/// <param name="OutputRecordsCheckpointed">The number of output records acknowledged in the checkpoint.</param>
public sealed record SpillOperationProgress(
    SpillOperationPhase Phase,
    long RecordsConsumed,
    int RunCount,
    long OutputRecordsCheckpointed);

/// <summary>Recoverable external-memory operators.</summary>
public static class RecoverableOrderByExtensions
{
    private const int CheckpointVersion = 1;

    /// <summary>
    /// Stably orders a resumable source and checkpoints run creation, merging, and output progress.
    /// Output recovery is at-least-once and may replay up to <paramref name="outputCheckpointInterval"/> records.
    /// </summary>
    /// <param name="session">A new session or a session opened with <see cref="SpillSession.Recover"/>.</param>
    /// <param name="sourceFactory">Opens the source at the supplied durable record offset.</param>
    /// <param name="operationId">A stable operation name containing letters, digits, '-' or '_'.</param>
    /// <param name="keySelector">Selects the ordering key.</param>
    /// <param name="comparer">Compares ordering keys.</param>
    /// <param name="outputCheckpointInterval">Maximum output replay window after interruption.</param>
    /// <param name="checkpointed">Receives synchronous notifications after durable checkpoints.</param>
    /// <param name="cancellationToken">Cancels input, merge, or output work.</param>
    public static async IAsyncEnumerable<TSource> OrderByRecoverableSpill<TSource, TKey>(
        this SpillSession session,
        Func<long, CancellationToken, IAsyncEnumerable<TSource>> sourceFactory,
        string operationId,
        Func<TSource, TKey> keySelector,
        IComparer<TKey>? comparer = null,
        int outputCheckpointInterval = 1_024,
        Action<SpillOperationProgress>? checkpointed = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(sourceFactory);
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentOutOfRangeException.ThrowIfLessThan(outputCheckpointInterval, 1);
        if (operationId.Any(character => !(char.IsAsciiLetterOrDigit(character) || character is '-' or '_')))
            throw new ArgumentException("Operation IDs may contain only ASCII letters, digits, '-' and '_'.", nameof(operationId));

        comparer ??= Comparer<TKey>.Default;
        var checkpointName = $"orderby-{operationId}";
        var state = session.HasCheckpoint(checkpointName)
            ? await session.LoadCheckpointAsync<RecoverableOrderByState>(checkpointName, cancellationToken)
                .ConfigureAwait(false)
            : RecoverableOrderByState.New;
        ValidateState(state, session, operationId);
        CleanupOrphanRuns(session, operationId, state.Runs.Concat(state.MergeOutputs));

        async ValueTask SaveAsync(RecoverableOrderByState next)
        {
            await session.SaveCheckpointAsync(checkpointName, next, cancellationToken).ConfigureAwait(false);
            state = next;
            checkpointed?.Invoke(new SpillOperationProgress(
                state.Phase, state.RecordsConsumed, state.Runs.Length + state.MergeOutputs.Length,
                state.OutputRecordsCheckpointed));
            cancellationToken.ThrowIfCancellationRequested();
        }

        string NewRun() => session.NewFile($".orderby-{operationId}.run");

        async ValueTask<string> WriteRunAsync(List<(TSource Item, long Sequence)> values)
        {
            values.Sort((a, b) =>
            {
                var keyComparison = comparer.Compare(keySelector(a.Item), keySelector(b.Item));
                return keyComparison != 0 ? keyComparison : a.Sequence.CompareTo(b.Sequence);
            });
            var path = NewRun();
            await using var writer = new PooledRecordWriter<TSource>(
                path, session, session.GetIoBlockSize(session.Options.MergeFanIn));
            foreach (var entry in values)
                await writer.WriteAsync(entry.Item, cancellationToken).ConfigureAwait(false);
            return path;
        }

        async IAsyncEnumerable<TSource> MergeRunsAsync(
            IReadOnlyList<string> fileNames,
            [EnumeratorCancellation] CancellationToken mergeCancellationToken)
        {
            var paths = fileNames.Select(session.ResolveSessionFile).ToArray();
            var readers = paths.Select(path => RecordFile.ReadAsync<TSource>(path, session, mergeCancellationToken)
                .GetAsyncEnumerator(mergeCancellationToken)).ToArray();
            var priorityComparer = Comparer<(TKey Key, int Run)>.Create((a, b) =>
            {
                var keyComparison = comparer.Compare(a.Key, b.Key);
                return keyComparison != 0 ? keyComparison : a.Run.CompareTo(b.Run);
            });
            var queue = new PriorityQueue<(TSource Item, int Reader), (TKey Key, int Run)>(priorityComparer);
            try
            {
                for (var index = 0; index < readers.Length; index++)
                    if (await readers[index].MoveNextAsync().ConfigureAwait(false))
                        queue.Enqueue((readers[index].Current, index),
                            (keySelector(readers[index].Current), index));
                while (queue.TryDequeue(out var entry, out _))
                {
                    yield return entry.Item;
                    if (await readers[entry.Reader].MoveNextAsync().ConfigureAwait(false))
                    {
                        var next = readers[entry.Reader].Current;
                        queue.Enqueue((next, entry.Reader), (keySelector(next), entry.Reader));
                    }
                }
            }
            finally
            {
                foreach (var reader in readers)
                    await reader.DisposeAsync().ConfigureAwait(false);
            }
        }

        async ValueTask<string> MergeToRunAsync(IReadOnlyList<string> fileNames)
        {
            var path = NewRun();
            await using var writer = new PooledRecordWriter<TSource>(
                path, session, session.GetIoBlockSize(session.Options.MergeFanIn));
            await foreach (var item in MergeRunsAsync(fileNames, cancellationToken).ConfigureAwait(false))
                await writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
            return path;
        }

        if (state.Phase == SpillOperationPhase.Completed)
            yield break;

        if (state.Phase == SpillOperationPhase.Reading)
        {
            var buffer = new List<(TSource Item, long Sequence)>();
            var consumed = state.RecordsConsumed;
            long bytes = 0;

            async ValueTask FlushInputRunAsync()
            {
                if (buffer.Count == 0)
                    return;
                var path = await WriteRunAsync(buffer).ConfigureAwait(false);
                var runs = state.Runs.Append(Path.GetFileName(path)).ToArray();
                await SaveAsync(state with { RecordsConsumed = consumed, Runs = runs }).ConfigureAwait(false);
                buffer.Clear();
                bytes = 0;
            }

            var source = sourceFactory(state.RecordsConsumed, cancellationToken)
                ?? throw new InvalidOperationException("The resumable source factory returned null.");
            await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                session.ReadRecord();
                buffer.Add((item, consumed));
                consumed++;
                bytes += RecordFile.EstimateSize(item, session) + 40;
                if (bytes >= session.Options.MemoryLimitBytes * 3 / 4)
                    await FlushInputRunAsync().ConfigureAwait(false);
            }
            await FlushInputRunAsync().ConfigureAwait(false);
            await SaveAsync(state with { Phase = SpillOperationPhase.Merging }).ConfigureAwait(false);
        }

        while (state.Phase == SpillOperationPhase.Merging)
        {
            if (state.Runs.Length == 0)
            {
                if (state.MergeOutputs.Length <= 1)
                {
                    await SaveAsync(state with
                    {
                        Phase = SpillOperationPhase.Output,
                        Runs = state.MergeOutputs,
                        MergeOutputs = []
                    }).ConfigureAwait(false);
                    break;
                }

                await SaveAsync(state with
                {
                    Runs = state.MergeOutputs,
                    MergeOutputs = []
                }).ConfigureAwait(false);
                continue;
            }

            var count = Math.Min(session.Options.MergeFanIn, state.Runs.Length);
            var inputs = state.Runs.Take(count).ToArray();
            var outputName = inputs.Length == 1
                ? inputs[0]
                : Path.GetFileName(await MergeToRunAsync(inputs).ConfigureAwait(false));
            await SaveAsync(state with
            {
                Runs = state.Runs.Skip(count).ToArray(),
                MergeOutputs = state.MergeOutputs.Append(outputName).ToArray()
            }).ConfigureAwait(false);
            if (inputs.Length > 1)
                foreach (var input in inputs)
                    session.DeleteFile(session.ResolveSessionFile(input));
        }

        if (state.Runs.Length == 0)
        {
            await SaveAsync(state with { Phase = SpillOperationPhase.Completed }).ConfigureAwait(false);
            yield break;
        }

        var finalPath = session.ResolveSessionFile(state.Runs[0]);
        var skipped = 0L;
        var delivered = state.OutputRecordsCheckpointed;
        await foreach (var item in RecordFile.ReadAsync<TSource>(finalPath, session, cancellationToken)
                           .ConfigureAwait(false))
        {
            if (skipped++ < state.OutputRecordsCheckpointed)
                continue;
            yield return item;
            delivered++;
            if (delivered % outputCheckpointInterval == 0)
                await SaveAsync(state with { OutputRecordsCheckpointed = delivered }).ConfigureAwait(false);
        }

        if (skipped < state.OutputRecordsCheckpointed)
            throw new InvalidDataException("The recoverable OrderBy output checkpoint exceeds the final run length.");

        await SaveAsync(state with
        {
            Phase = SpillOperationPhase.Completed,
            OutputRecordsCheckpointed = delivered,
            Runs = [],
            MergeOutputs = []
        }).ConfigureAwait(false);
        session.DeleteFile(finalPath);
    }

    private static void ValidateState(RecoverableOrderByState state, SpillSession session, string operationId)
    {
        if (state.Version != CheckpointVersion || !Enum.IsDefined(state.Phase) ||
            state.RecordsConsumed < 0 || state.OutputRecordsCheckpointed < 0)
            throw new InvalidDataException("Invalid recoverable OrderBy checkpoint.");
        var allRuns = state.Runs.Concat(state.MergeOutputs).ToArray();
        var suffix = $".orderby-{operationId}.run";
        if (allRuns.Distinct(StringComparer.OrdinalIgnoreCase).Count() != allRuns.Length ||
            allRuns.Any(fileName => !fileName.EndsWith(suffix, StringComparison.OrdinalIgnoreCase)))
            throw new InvalidDataException("A recoverable OrderBy checkpoint contains an invalid run name.");
        foreach (var fileName in allRuns)
        {
            var path = session.ResolveSessionFile(fileName);
            if (!File.Exists(path))
                throw new InvalidDataException($"Recoverable OrderBy run '{fileName}' is missing.");
        }
        if (state.Phase == SpillOperationPhase.Output && (state.Runs.Length > 1 || state.MergeOutputs.Length != 0))
            throw new InvalidDataException("An output-phase checkpoint may reference only one final run.");
        if (state.Phase == SpillOperationPhase.Completed && allRuns.Length != 0)
            throw new InvalidDataException("A completed recoverable OrderBy checkpoint cannot reference runs.");
    }

    private static void CleanupOrphanRuns(SpillSession session, string operationId, IEnumerable<string> runs)
    {
        var referenced = runs.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var suffix = $".orderby-{operationId}.run";
        foreach (var path in Directory.EnumerateFiles(session.WorkingDirectory, $"*{suffix}"))
            if (!referenced.Contains(Path.GetFileName(path)))
                session.DeleteFile(path);
    }

    private sealed record RecoverableOrderByState(
        int Version,
        SpillOperationPhase Phase,
        long RecordsConsumed,
        string[] Runs,
        string[] MergeOutputs,
        long OutputRecordsCheckpointed)
    {
        public static RecoverableOrderByState New { get; } =
            new(CheckpointVersion, SpillOperationPhase.Reading, 0, [], [], 0);
    }
}
