using System.Runtime.CompilerServices;

namespace SpillFlow.Tests;

public sealed class TransactionalOutputTests
{
    [Fact]
    public async Task Replayed_acknowledged_output_is_not_applied_twice()
    {
        var sink = new TestTransactionalSink<int>();

        await Values(1, 2, 3).ProcessTransactionallyAsync(
            "daily-posting", value => $"transaction-{value}", sink);
        await Values(2, 3, 4).ProcessTransactionallyAsync(
            "daily-posting", value => $"transaction-{value}", sink);

        Assert.Equal([1, 2, 3, 4], sink.Applied);
        Assert.Equal(4, sink.Acknowledged.Count);
    }

    [Fact]
    public async Task Crash_before_atomic_commit_leaves_delivery_unacknowledged_for_retry()
    {
        var sink = new TestTransactionalSink<int> { FailDeliveryKeyOnce = "transaction-2" };

        await Assert.ThrowsAsync<SimulatedSinkInterruptionException>(async () =>
            await Values(1, 2, 3).ProcessTransactionallyAsync(
                "daily-posting", value => $"transaction-{value}", sink));

        await Values(1, 2, 3).ProcessTransactionallyAsync(
            "daily-posting", value => $"transaction-{value}", sink);

        Assert.Equal([1, 2, 3], sink.Applied);
        Assert.Equal(3, sink.Acknowledged.Count);
    }

    [Fact]
    public async Task Sink_must_make_duplicate_commit_attempt_idempotent()
    {
        var sink = new TestTransactionalSink<int>();
        var id = new SpillDeliveryId("posting", "transaction-42");
        var delivery = new SpillDelivery<int>(id, 42);

        await sink.CommitAndAcknowledgeAsync(delivery);
        await sink.CommitAndAcknowledgeAsync(delivery);

        Assert.Equal([42], sink.Applied);
    }

    [Fact]
    public async Task Recoverable_operator_and_durable_sink_complete_exactly_once_after_lost_commit_response()
    {
        var root = Path.Combine(Path.GetTempPath(), $"spillflow-transactional-output-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        string workingDirectory;
        var sink = new TestTransactionalSink<int> { ThrowAfterCommitKeyOnce = "transaction-4" };
        try
        {
            var first = new SpillSession(new SpillOptions
            {
                TemporaryDirectory = root,
                CleanupAbandonedSessions = false,
                MemoryLimitBytes = 64 * 1024,
                MergeFanIn = 2
            });
            workingDirectory = first.WorkingDirectory;
            try
            {
                await Assert.ThrowsAsync<SimulatedSinkInterruptionException>(async () =>
                    await first.OrderByRecoverableSpill(
                            (offset, token) => DescendingValues(offset, 10, token),
                            "transactional-sort", value => value,
                            outputCheckpointInterval: 100)
                        .ProcessTransactionallyAsync(
                            "post-sorted-transactions", value => $"transaction-{value}", sink));
                first.PreserveForRecovery();
            }
            finally
            {
                await first.DisposeAsync();
            }

            await using var recovered = SpillSession.Recover(workingDirectory, new SpillOptions
            {
                TemporaryDirectory = root,
                CleanupAbandonedSessions = false,
                MemoryLimitBytes = 64 * 1024,
                MergeFanIn = 2
            });
            await recovered.OrderByRecoverableSpill<int, int>(
                    (offset, token) => throw new InvalidOperationException("Output recovery must not reopen the source."),
                    "transactional-sort", value => value,
                    outputCheckpointInterval: 100)
                .ProcessTransactionallyAsync(
                    "post-sorted-transactions", value => $"transaction-{value}", sink);

            Assert.Equal(Enumerable.Range(0, 10), sink.Applied.Order());
            Assert.Equal(10, sink.Applied.Count);
            Assert.Equal(10, sink.Acknowledged.Count);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    [Theory]
    [InlineData("")]
    [InlineData("contains space")]
    [InlineData("bad/path")]
    public async Task Rejects_invalid_operation_ids(string operationId)
    {
        await Assert.ThrowsAnyAsync<ArgumentException>(async () =>
            await Values(1).ProcessTransactionallyAsync(operationId, value => value.ToString(),
                new TestTransactionalSink<int>()));
    }

    [Fact]
    public async Task Rejects_empty_delivery_keys()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await Values(1).ProcessTransactionallyAsync("posting", _ => " ", new TestTransactionalSink<int>()));
    }

    private static async IAsyncEnumerable<int> Values(
        params int[] values)
    {
        foreach (var value in values)
        {
            yield return value;
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<int> DescendingValues(
        long offset,
        int count,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        for (var position = offset; position < count; position++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return count - 1 - (int)position;
            await Task.Yield();
        }
    }

    private sealed class TestTransactionalSink<T> : ISpillTransactionalSink<T>
    {
        public HashSet<SpillDeliveryId> Acknowledged { get; } = [];
        public List<T> Applied { get; } = [];
        public string? FailDeliveryKeyOnce { get; set; }
        public string? ThrowAfterCommitKeyOnce { get; set; }

        public ValueTask<bool> IsAcknowledgedAsync(
            SpillDeliveryId deliveryId,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(Acknowledged.Contains(deliveryId));

        public ValueTask CommitAndAcknowledgeAsync(
            SpillDelivery<T> delivery,
            CancellationToken cancellationToken = default)
        {
            if (FailDeliveryKeyOnce == delivery.Id.DeliveryKey)
            {
                FailDeliveryKeyOnce = null;
                throw new SimulatedSinkInterruptionException();
            }

            // This lock models one database transaction with a unique acknowledgement constraint.
            lock (Acknowledged)
            {
                if (Acknowledged.Add(delivery.Id))
                    Applied.Add(delivery.Value);
            }
            if (ThrowAfterCommitKeyOnce == delivery.Id.DeliveryKey)
            {
                ThrowAfterCommitKeyOnce = null;
                throw new SimulatedSinkInterruptionException();
            }
            return ValueTask.CompletedTask;
        }
    }

    private sealed class SimulatedSinkInterruptionException : Exception;
}
