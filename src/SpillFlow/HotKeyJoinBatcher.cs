using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;

namespace SpillFlow;

internal static class HotKeyJoinBatcher
{
    private const int EstimatedConsumerBytes = 512;

    public static async IAsyncEnumerable<TResult> JoinAsync<TLeft, TRight, TKey, TResult>(
        string leftPath,
        string rightPath,
        SpillSession session,
        Func<TLeft, TKey> leftKeySelector,
        Func<TRight, TKey> rightKeySelector,
        Func<TLeft, IAsyncEnumerable<TRight>, CancellationToken, ValueTask<TResult>> resultSelector,
        IEqualityComparer<TKey> comparer,
        long skipLeftRecords,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where TKey : notnull
    {
        ArgumentOutOfRangeException.ThrowIfNegative(skipLeftRecords);
        var batch = new List<TLeft>(session.Options.HotKeyJoinBatchSize);
        var batchBudget = Math.Max(1, session.Options.MemoryLimitBytes / 4);
        long estimatedBytes = 0;

        async ValueTask<TResult[]> ProcessBatchAsync()
        {
            if (batch.Count == 0)
                return [];
            using var lease = session.AcquireMemoryLease(
                Math.Max(1, estimatedBytes), nameof(HotKeyJoinBatcher));
            session.ProcessedHotKeyJoinBatch();

            var consumers = new List<Consumer<TLeft, TRight, TKey, TResult>>(batch.Count);
            var byKey = new Dictionary<TKey, List<Consumer<TLeft, TRight, TKey, TResult>>>(comparer);
            foreach (var left in batch)
            {
                var channel = Channel.CreateBounded<TRight>(new BoundedChannelOptions(1)
                {
                    SingleReader = true,
                    SingleWriter = true,
                    FullMode = BoundedChannelFullMode.Wait
                });
                var key = leftKeySelector(left);
                var task = InvokeSelectorAsync(left, channel.Reader.ReadAllAsync(cancellationToken),
                    resultSelector, cancellationToken);
                var consumer = new Consumer<TLeft, TRight, TKey, TResult>(left, key, channel, task);
                consumers.Add(consumer);
                if (!byKey.TryGetValue(key, out var keyedConsumers))
                    byKey[key] = keyedConsumers = [];
                keyedConsumers.Add(consumer);
            }

            Exception? scanFailure = null;
            try
            {
                session.ScannedHotKeyPartition();
                await foreach (var right in RecordFile.ReadAsync<TRight>(rightPath, session, cancellationToken)
                                   .ConfigureAwait(false))
                {
                    if (!byKey.TryGetValue(rightKeySelector(right), out var matches))
                        continue;
                    foreach (var consumer in matches)
                        await WriteOrDetectAbandonedReaderAsync(consumer, right, cancellationToken)
                            .ConfigureAwait(false);
                }
            }
            catch (Exception exception)
            {
                scanFailure = exception;
            }

            foreach (var consumer in consumers)
                consumer.Channel.Writer.TryComplete(scanFailure);

            try
            {
                var results = await Task.WhenAll(consumers.Select(consumer => consumer.Result)).ConfigureAwait(false);
                if (scanFailure is not null)
                    ExceptionDispatchInfo.Capture(scanFailure).Throw();
                return results;
            }
            catch when (scanFailure is not null)
            {
                ExceptionDispatchInfo.Capture(scanFailure).Throw();
                throw;
            }
        }

        var leftPosition = 0L;
        await foreach (var left in RecordFile.ReadAsync<TLeft>(leftPath, session, cancellationToken)
                           .ConfigureAwait(false))
        {
            if (leftPosition++ < skipLeftRecords)
                continue;
            batch.Add(left);
            estimatedBytes = checked(estimatedBytes + RecordFile.EstimateSize(left, session) + EstimatedConsumerBytes);
            if (batch.Count < session.Options.HotKeyJoinBatchSize && estimatedBytes < batchBudget)
                continue;
            foreach (var result in await ProcessBatchAsync().ConfigureAwait(false))
                yield return result;
            batch.Clear();
            estimatedBytes = 0;
        }

        foreach (var result in await ProcessBatchAsync().ConfigureAwait(false))
            yield return result;
        if (leftPosition < skipLeftRecords)
            throw new InvalidDataException("The streaming-join checkpoint exceeds its left partition.");
    }

    private static async Task<TResult> InvokeSelectorAsync<TLeft, TRight, TResult>(
        TLeft left,
        IAsyncEnumerable<TRight> matches,
        Func<TLeft, IAsyncEnumerable<TRight>, CancellationToken, ValueTask<TResult>> resultSelector,
        CancellationToken cancellationToken) =>
        await resultSelector(left, matches, cancellationToken).ConfigureAwait(false);

    private static async ValueTask WriteOrDetectAbandonedReaderAsync<TLeft, TRight, TKey, TResult>(
        Consumer<TLeft, TRight, TKey, TResult> consumer,
        TRight value,
        CancellationToken cancellationToken)
        where TKey : notnull
    {
        var write = consumer.Channel.Writer.WriteAsync(value, cancellationToken);
        if (write.IsCompletedSuccessfully)
            return;
        var writeTask = write.AsTask();
        var completed = await Task.WhenAny(writeTask, consumer.Result).ConfigureAwait(false);
        if (completed == consumer.Result && !writeTask.IsCompleted)
        {
            await consumer.Result.ConfigureAwait(false);
            throw new InvalidOperationException(
                "A streaming-join result selector returned before consuming its match stream.");
        }
        await writeTask.ConfigureAwait(false);
    }

    private sealed record Consumer<TLeft, TRight, TKey, TResult>(
        TLeft Left,
        TKey Key,
        Channel<TRight> Channel,
        Task<TResult> Result)
        where TKey : notnull;
}
