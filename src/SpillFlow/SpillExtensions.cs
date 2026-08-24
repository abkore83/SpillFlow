using System.Runtime.CompilerServices;

namespace SpillFlow;

/// <summary>Bounded-memory operations over asynchronous sequences.</summary>
public static class SpillExtensions
{
    /// <summary>Returns the first source value for each distinct key using bounded temporary storage.</summary>
    public static async IAsyncEnumerable<TSource> DistinctSpill<TSource, TKey>(
        this IAsyncEnumerable<TSource> source,
        SpillSession session,
        Func<TSource, TKey> keySelector,
        IEqualityComparer<TKey>? comparer = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        comparer ??= EqualityComparer<TKey>.Default;
        var seen = new HashSet<TKey>(comparer);
        var spillBatchSeen = new HashSet<TKey>(comparer);
        var buffered = new List<TSource>();
        long estimatedBytes = 0;
        var spillBatchKeyLimit = Math.Max(1_024, session.Options.MemoryLimitBytes / 64);
        PartitionWriter<TSource>? partitions = null;

        try
        {
            await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                session.ReadRecord();
                var key = keySelector(item);
                if (partitions is null)
                {
                    if (!seen.Add(key))
                        continue;
                    buffered.Add(item);
                    estimatedBytes += RecordFile.EstimateSize(item, session) + 48;
                    if (estimatedBytes < session.Options.MemoryLimitBytes * 3 / 4)
                        continue;

                    partitions = new PartitionWriter<TSource>(session);
                    foreach (var value in buffered)
                        await partitions.WriteAsync(StableHash.Partition(keySelector(value), session.Options.PartitionCount, comparer), value, cancellationToken);
                    buffered.Clear();
                    seen.Clear();
                    continue;
                }

                if (!spillBatchSeen.Add(key))
                    continue;
                await partitions.WriteAsync(StableHash.Partition(key, session.Options.PartitionCount, comparer), item, cancellationToken);
                if (spillBatchSeen.Count >= spillBatchKeyLimit)
                    spillBatchSeen.Clear();
            }

            if (partitions is null)
            {
                foreach (var item in buffered)
                    yield return item;
                yield break;
            }

            var paths = partitions.Paths.ToArray();
            await partitions.DisposeAsync();
            partitions = null;
            foreach (var path in paths)
            {
                if (!File.Exists(path))
                    continue;
                var leaves = await PartitionRefiner.RefineAsync<TSource, TKey>(path, session, keySelector, comparer,
                    nameof(DistinctSpill), cancellationToken);
                foreach (var leaf in leaves)
                {
                    using var lease = session.AcquirePartitionLease(leaf, nameof(DistinctSpill));
                    seen.Clear();
                    await foreach (var item in RecordFile.ReadAsync<TSource>(leaf, session, cancellationToken))
                        if (seen.Add(keySelector(item)))
                            yield return item;
                    session.DeleteFile(leaf);
                }
            }
        }
        finally
        {
            if (partitions is not null)
                await partitions.DisposeAsync();
        }
    }

    /// <summary>Returns one source value per key whose key is absent from the exclusion sequence.</summary>
    public static async IAsyncEnumerable<TSource> ExceptBySpill<TSource, TKey>(
        this IAsyncEnumerable<TSource> source,
        IAsyncEnumerable<TKey> excludedKeys,
        SpillSession session,
        Func<TSource, TKey> keySelector,
        IEqualityComparer<TKey>? comparer = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        comparer ??= EqualityComparer<TKey>.Default;
        await using var left = new PartitionWriter<TSource>(session);
        await using var right = new PartitionWriter<TKey>(session);
        await foreach (var item in source.WithCancellation(cancellationToken))
        {
            session.ReadRecord();
            await left.WriteAsync(StableHash.Partition(keySelector(item), session.Options.PartitionCount, comparer), item, cancellationToken);
        }
        await foreach (var key in excludedKeys.WithCancellation(cancellationToken))
        {
            session.ReadRecord();
            await right.WriteAsync(StableHash.Partition(key, session.Options.PartitionCount, comparer), key, cancellationToken);
        }
        await left.DisposeAsync();
        await right.DisposeAsync();

        for (var i = 0; i < session.Options.PartitionCount; i++)
        {
            var leaves = await PartitionPairRefiner.RefineAsync<TSource, TKey, TKey>(
                File.Exists(left.Paths[i]) ? left.Paths[i] : null,
                File.Exists(right.Paths[i]) ? right.Paths[i] : null,
                session, keySelector, key => key, comparer, nameof(ExceptBySpill), materializeLeft: true,
                cancellationToken);
            foreach (var leaf in leaves)
            {
                using var lease = session.AcquirePartitionsLease([leaf.Left, leaf.Right], nameof(ExceptBySpill));
                var excluded = new HashSet<TKey>(comparer);
                var emitted = new HashSet<TKey>(comparer);
                if (leaf.Right is not null)
                    await foreach (var key in RecordFile.ReadAsync<TKey>(leaf.Right, session, cancellationToken))
                        excluded.Add(key);
                if (leaf.Left is not null)
                    await foreach (var item in RecordFile.ReadAsync<TSource>(leaf.Left, session, cancellationToken))
                    {
                        var key = keySelector(item);
                        if (!excluded.Contains(key) && emitted.Add(key))
                            yield return item;
                    }
                if (leaf.Left is not null) session.DeleteFile(leaf.Left);
                if (leaf.Right is not null) session.DeleteFile(leaf.Right);
            }
        }
    }

    /// <summary>Correlates each left value with all right values having an equal key.</summary>
    public static async IAsyncEnumerable<TResult> GroupJoinSpill<TLeft, TRight, TKey, TResult>(
        this IAsyncEnumerable<TLeft> leftSource,
        IAsyncEnumerable<TRight> rightSource,
        SpillSession session,
        Func<TLeft, TKey> leftKeySelector,
        Func<TRight, TKey> rightKeySelector,
        Func<TLeft, IReadOnlyList<TRight>, TResult> resultSelector,
        IEqualityComparer<TKey>? comparer = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        comparer ??= EqualityComparer<TKey>.Default;
        await using var left = new PartitionWriter<TLeft>(session);
        await using var right = new PartitionWriter<TRight>(session);
        await foreach (var item in leftSource.WithCancellation(cancellationToken))
            await left.WriteAsync(StableHash.Partition(leftKeySelector(item), session.Options.PartitionCount, comparer), item, cancellationToken);
        await foreach (var item in rightSource.WithCancellation(cancellationToken))
            await right.WriteAsync(StableHash.Partition(rightKeySelector(item), session.Options.PartitionCount, comparer), item, cancellationToken);
        await left.DisposeAsync();
        await right.DisposeAsync();

        for (var i = 0; i < session.Options.PartitionCount; i++)
        {
            var leaves = await PartitionPairRefiner.RefineAsync<TLeft, TRight, TKey>(
                File.Exists(left.Paths[i]) ? left.Paths[i] : null,
                File.Exists(right.Paths[i]) ? right.Paths[i] : null,
                session, leftKeySelector, rightKeySelector, comparer, nameof(GroupJoinSpill), materializeLeft: false,
                cancellationToken);
            foreach (var leaf in leaves)
            {
                using var lease = leaf.Right is not null
                    ? session.AcquirePartitionLease(leaf.Right, nameof(GroupJoinSpill))
                    : null;
                var lookup = new Dictionary<TKey, List<TRight>>(comparer);
                if (leaf.Right is not null)
                {
                    await foreach (var item in RecordFile.ReadAsync<TRight>(leaf.Right, session, cancellationToken))
                    {
                        var key = rightKeySelector(item);
                        if (!lookup.TryGetValue(key, out var values))
                            lookup[key] = values = [];
                        values.Add(item);
                    }
                }
                if (leaf.Left is not null)
                    await foreach (var item in RecordFile.ReadAsync<TLeft>(leaf.Left, session, cancellationToken))
                    {
                        var key = leftKeySelector(item);
                        yield return resultSelector(item, lookup.TryGetValue(key, out var values) ? values : []);
                    }
                if (leaf.Left is not null) session.DeleteFile(leaf.Left);
                if (leaf.Right is not null) session.DeleteFile(leaf.Right);
            }
        }
    }

    /// <summary>
    /// Correlates left values with a match stream that can remain bounded when one key has more values than RAM.
    /// The result selector must consume the match stream before returning.
    /// </summary>
    public static async IAsyncEnumerable<TResult> GroupJoinStreamSpill<TLeft, TRight, TKey, TResult>(
        this IAsyncEnumerable<TLeft> leftSource,
        IAsyncEnumerable<TRight> rightSource,
        SpillSession session,
        Func<TLeft, TKey> leftKeySelector,
        Func<TRight, TKey> rightKeySelector,
        Func<TLeft, IAsyncEnumerable<TRight>, CancellationToken, ValueTask<TResult>> resultSelector,
        IEqualityComparer<TKey>? comparer = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(leftSource);
        ArgumentNullException.ThrowIfNull(rightSource);
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(leftKeySelector);
        ArgumentNullException.ThrowIfNull(rightKeySelector);
        ArgumentNullException.ThrowIfNull(resultSelector);

        comparer ??= EqualityComparer<TKey>.Default;
        await using var left = new PartitionWriter<TLeft>(session);
        await using var right = new PartitionWriter<TRight>(session);
        await foreach (var item in leftSource.WithCancellation(cancellationToken).ConfigureAwait(false))
            await left.WriteAsync(StableHash.Partition(leftKeySelector(item), session.Options.PartitionCount, comparer),
                item, cancellationToken).ConfigureAwait(false);
        await foreach (var item in rightSource.WithCancellation(cancellationToken).ConfigureAwait(false))
            await right.WriteAsync(StableHash.Partition(rightKeySelector(item), session.Options.PartitionCount, comparer),
                item, cancellationToken).ConfigureAwait(false);
        await left.DisposeAsync().ConfigureAwait(false);
        await right.DisposeAsync().ConfigureAwait(false);

        for (var i = 0; i < session.Options.PartitionCount; i++)
        {
            var leaves = await PartitionPairRefiner.RefineAsync<TLeft, TRight, TKey>(
                File.Exists(left.Paths[i]) ? left.Paths[i] : null,
                File.Exists(right.Paths[i]) ? right.Paths[i] : null,
                session, leftKeySelector, rightKeySelector, comparer, nameof(GroupJoinStreamSpill),
                materializeLeft: false, cancellationToken, allowOversizedLeaf: true).ConfigureAwait(false);

            foreach (var leaf in leaves)
            {
                var materializeRight = leaf.Right is null || session.CanPartitionFit(leaf.Right);
                Dictionary<TKey, List<TRight>>? lookup = null;
                using var lease = materializeRight && leaf.Right is not null
                    ? session.AcquirePartitionLease(leaf.Right, nameof(GroupJoinStreamSpill))
                    : null;
                if (materializeRight && leaf.Right is not null)
                {
                    lookup = new Dictionary<TKey, List<TRight>>(comparer);
                    await foreach (var item in RecordFile.ReadAsync<TRight>(leaf.Right, session, cancellationToken)
                                       .ConfigureAwait(false))
                    {
                        var key = rightKeySelector(item);
                        if (!lookup.TryGetValue(key, out var values))
                            lookup[key] = values = [];
                        values.Add(item);
                    }
                }

                if (leaf.Left is not null)
                {
                    await foreach (var item in RecordFile.ReadAsync<TLeft>(leaf.Left, session, cancellationToken)
                                       .ConfigureAwait(false))
                    {
                        var key = leftKeySelector(item);
                        IAsyncEnumerable<TRight> matches = materializeRight
                            ? EnumerateAsync(lookup is not null && lookup.TryGetValue(key, out var values) ? values : [],
                                cancellationToken)
                            : ReadMatchesAsync(leaf.Right!, session, key, rightKeySelector, comparer, cancellationToken);
                        yield return await resultSelector(item, matches, cancellationToken).ConfigureAwait(false);
                    }
                }

                if (leaf.Left is not null) session.DeleteFile(leaf.Left);
                if (leaf.Right is not null) session.DeleteFile(leaf.Right);
            }
        }
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

    /// <summary>Aggregates values by key without materializing every group member at once.</summary>
    public static async IAsyncEnumerable<TResult> GroupAggregateSpill<TSource, TKey, TAccumulator, TResult>(
        this IAsyncEnumerable<TSource> source,
        SpillSession session,
        Func<TSource, TKey> keySelector,
        Func<TAccumulator> seedFactory,
        Func<TAccumulator, TSource, TAccumulator> accumulate,
        Func<TKey, TAccumulator, TResult> resultSelector,
        IEqualityComparer<TKey>? comparer = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(seedFactory);
        ArgumentNullException.ThrowIfNull(accumulate);
        ArgumentNullException.ThrowIfNull(resultSelector);

        comparer ??= EqualityComparer<TKey>.Default;
        var accumulators = new Dictionary<TKey, TAccumulator>(comparer);
        var buffered = new List<TSource>();
        var inMemoryLimit = session.Options.MemoryLimitBytes * 3 / 4;
        long estimatedBytes = 0;
        PartitionWriter<TSource>? partitions = null;

        try
        {
            await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                session.ReadRecord();
                var key = keySelector(item);
                if (partitions is not null)
                {
                    await partitions.WriteAsync(StableHash.Partition(key, session.Options.PartitionCount, comparer), item, cancellationToken)
                        .ConfigureAwait(false);
                    continue;
                }

                buffered.Add(item);
                estimatedBytes += IntPtr.Size;
                if (!accumulators.TryGetValue(key, out var current))
                {
                    current = seedFactory();
                    estimatedBytes += RecordFile.EstimateSize(key, session) + 96;
                }
                accumulators[key] = accumulate(current, item);

                if (estimatedBytes < inMemoryLimit)
                    continue;

                partitions = new PartitionWriter<TSource>(session);
                foreach (var value in buffered)
                {
                    var bufferedKey = keySelector(value);
                    await partitions.WriteAsync(
                        StableHash.Partition(bufferedKey, session.Options.PartitionCount, comparer), value, cancellationToken)
                        .ConfigureAwait(false);
                }
                buffered.Clear();
                accumulators.Clear();
            }

            if (partitions is null)
            {
                foreach (var pair in accumulators)
                    yield return resultSelector(pair.Key, pair.Value);
                yield break;
            }

            var paths = partitions.Paths.ToArray();
            await partitions.DisposeAsync().ConfigureAwait(false);
            partitions = null;

            for (var i = 0; i < paths.Length; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!File.Exists(paths[i]))
                    continue;
                var leaves = await PartitionRefiner.RefineAsync<TSource, TKey>(paths[i], session, keySelector, comparer,
                    nameof(GroupAggregateSpill), cancellationToken).ConfigureAwait(false);
                foreach (var leaf in leaves)
                {
                    using var lease = session.AcquirePartitionLease(leaf, nameof(GroupAggregateSpill));
                    accumulators.Clear();
                    await foreach (var item in RecordFile.ReadAsync<TSource>(leaf, session, cancellationToken)
                                       .ConfigureAwait(false))
                    {
                        var key = keySelector(item);
                        var current = accumulators.TryGetValue(key, out var existing)
                            ? existing
                            : seedFactory();
                        accumulators[key] = accumulate(current, item);
                    }

                    foreach (var pair in accumulators)
                        yield return resultSelector(pair.Key, pair.Value);

                    session.DeleteFile(leaf);
                }
            }
        }
        finally
        {
            if (partitions is not null)
                await partitions.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Orders a sequence by key using bounded sorted runs and multi-pass merging.</summary>
    public static async IAsyncEnumerable<TSource> OrderBySpill<TSource, TKey>(
        this IAsyncEnumerable<TSource> source,
        SpillSession session,
        Func<TSource, TKey> keySelector,
        IComparer<TKey>? comparer = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        comparer ??= Comparer<TKey>.Default;
        var chunks = new List<string>();
        var buffer = new List<TSource>();
        long bytes = 0;

        async ValueTask FlushAsync()
        {
            if (buffer.Count == 0) return;
            buffer.Sort((a, b) => comparer.Compare(keySelector(a), keySelector(b)));
            var path = session.NewFile();
            await using var writer = new PooledRecordWriter<TSource>(
                path, session, session.GetIoBlockSize(session.Options.MergeFanIn));
            foreach (var item in buffer)
                await writer.WriteAsync(item, cancellationToken);
            chunks.Add(path);
            buffer.Clear();
            bytes = 0;
        }

        async IAsyncEnumerable<TSource> MergeRunsAsync(IReadOnlyList<string> paths)
        {
            var readers = paths.Select(path => RecordFile.ReadAsync<TSource>(path, session, cancellationToken)
                .GetAsyncEnumerator(cancellationToken)).ToArray();
            var queue = new PriorityQueue<(TSource Item, int Reader), TKey>(comparer);
            try
            {
                for (var i = 0; i < readers.Length; i++)
                    if (await readers[i].MoveNextAsync())
                        queue.Enqueue((readers[i].Current, i), keySelector(readers[i].Current));
                while (queue.TryDequeue(out var entry, out _))
                {
                    yield return entry.Item;
                    if (await readers[entry.Reader].MoveNextAsync())
                    {
                        var next = readers[entry.Reader].Current;
                        queue.Enqueue((next, entry.Reader), keySelector(next));
                    }
                }
            }
            finally
            {
                foreach (var reader in readers)
                    await reader.DisposeAsync();
            }
        }

        async ValueTask<string> MergeRunsToFileAsync(IReadOnlyList<string> paths)
        {
            var outputPath = session.NewFile();
            await using var output = new PooledRecordWriter<TSource>(
                outputPath, session, session.GetIoBlockSize(session.Options.MergeFanIn));
            await foreach (var item in MergeRunsAsync(paths).WithCancellation(cancellationToken))
                await output.WriteAsync(item, cancellationToken);
            return outputPath;
        }

        await foreach (var item in source.WithCancellation(cancellationToken))
        {
            session.ReadRecord();
            buffer.Add(item);
            bytes += RecordFile.EstimateSize(item, session) + 32;
            if (bytes >= session.Options.MemoryLimitBytes * 3 / 4)
                await FlushAsync();
        }

        if (chunks.Count == 0)
        {
            buffer.Sort((a, b) => comparer.Compare(keySelector(a), keySelector(b)));
            foreach (var item in buffer) yield return item;
            yield break;
        }
        await FlushAsync();

        while (chunks.Count > session.Options.MergeFanIn)
        {
            var nextPass = new List<string>((chunks.Count + session.Options.MergeFanIn - 1) / session.Options.MergeFanIn);
            for (var offset = 0; offset < chunks.Count; offset += session.Options.MergeFanIn)
            {
                var count = Math.Min(session.Options.MergeFanIn, chunks.Count - offset);
                var group = chunks.GetRange(offset, count);
                if (group.Count == 1)
                {
                    nextPass.Add(group[0]);
                    continue;
                }
                nextPass.Add(await MergeRunsToFileAsync(group));
                foreach (var path in group)
                    session.DeleteFile(path);
            }
            chunks = nextPass;
        }

        await foreach (var item in MergeRunsAsync(chunks).WithCancellation(cancellationToken))
            yield return item;
    }
}
