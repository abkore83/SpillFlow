using System.Runtime.CompilerServices;

namespace SpillFlow.Tests;

public sealed class RecoverableGroupAggregateTests
{
    [Fact]
    public async Task Resumes_input_and_preserves_exact_aggregate_values()
    {
        var root = NewRoot();
        string workingDirectory;
        try
        {
            var first = new SpillSession(Options(root));
            workingDirectory = first.WorkingDirectory;
            try
            {
                var operation = Operation(
                    first,
                    (offset, token) => Source(offset, 20_000, 7_500, token),
                    "input-resume");
                await Assert.ThrowsAsync<SimulatedInterruptionException>(() => CollectAsync(operation));
                first.PreserveForRecovery();
            }
            finally
            {
                await first.DisposeAsync();
            }

            long resumedOffset = -1;
            await using var recovered = SpillSession.Recover(workingDirectory, Options(root));
            var result = await CollectAsync(Operation(
                recovered,
                (offset, token) =>
                {
                    resumedOffset = offset;
                    return Source(offset, 20_000, null, token);
                },
                "input-resume"));

            Assert.InRange(resumedOffset, 1, 7_499);
            AssertExpected(result);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Rebuilds_interrupted_partitions_without_reopening_the_source()
    {
        var root = NewRoot();
        string workingDirectory;
        try
        {
            var keyCalls = 0;
            var first = new SpillSession(Options(root));
            workingDirectory = first.WorkingDirectory;
            try
            {
                var operation = first.GroupAggregateRecoverableSpill(
                    (offset, token) => Source(offset, 20_000, null, token),
                    "partition-resume",
                    value => ++keyCalls == 7_500 ? throw new SimulatedInterruptionException() : value % 5_000,
                    () => (Count: 0, Sum: 0L),
                    (state, value) => (state.Count + 1, state.Sum + value),
                    (key, state) => new Aggregate(key, state.Count, state.Sum));
                await Assert.ThrowsAsync<SimulatedInterruptionException>(() => CollectAsync(operation));
                first.PreserveForRecovery();
            }
            finally
            {
                await first.DisposeAsync();
            }

            var sourceReopened = false;
            await using var recovered = SpillSession.Recover(workingDirectory, Options(root));
            var result = await CollectAsync(Operation(
                recovered,
                (offset, token) =>
                {
                    sourceReopened = true;
                    return Source(offset, 20_000, null, token);
                },
                "partition-resume"));

            Assert.False(sourceReopened);
            AssertExpected(result);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Output_recovery_preserves_aggregates_with_bounded_at_least_once_replay()
    {
        var root = NewRoot();
        string workingDirectory;
        var delivered = new List<Aggregate>();
        const int replayWindow = 200;
        try
        {
            var first = new SpillSession(Options(root));
            workingDirectory = first.WorkingDirectory;
            try
            {
                var operation = Operation(
                    first,
                    (offset, token) => Source(offset, 20_000, null, token),
                    "output-resume",
                    replayWindow);
                await Assert.ThrowsAsync<SimulatedInterruptionException>(async () =>
                {
                    await foreach (var value in operation)
                    {
                        delivered.Add(value);
                        if (delivered.Count == 2_350)
                            throw new SimulatedInterruptionException();
                    }
                });
                first.PreserveForRecovery();
            }
            finally
            {
                await first.DisposeAsync();
            }

            await using var recovered = SpillSession.Recover(workingDirectory, Options(root));
            var replayed = await CollectAsync(Operation(
                recovered,
                (offset, token) => throw new InvalidOperationException("Output recovery must not reopen the source."),
                "output-resume",
                replayWindow));

            var overlap = delivered.Intersect(replayed).Count();
            Assert.InRange(overlap, 0, replayWindow - 1);
            var unique = delivered.Concat(replayed).Distinct().ToArray();
            AssertExpected(unique);
            Assert.Equal(5_000 + overlap, delivered.Count + replayed.Count);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    private static IAsyncEnumerable<Aggregate> Operation(
        SpillSession session,
        Func<long, CancellationToken, IAsyncEnumerable<int>> sourceFactory,
        string operationId,
        int outputCheckpointInterval = 1_024) =>
        session.GroupAggregateRecoverableSpill(
            sourceFactory,
            operationId,
            value => value % 5_000,
            () => (Count: 0, Sum: 0L),
            (state, value) => (state.Count + 1, state.Sum + value),
            (key, state) => new Aggregate(key, state.Count, state.Sum),
            outputCheckpointInterval: outputCheckpointInterval);

    private static void AssertExpected(IReadOnlyCollection<Aggregate> result)
    {
        Assert.Equal(5_000, result.Count);
        foreach (var aggregate in result)
        {
            Assert.Equal(4, aggregate.Count);
            Assert.Equal(4L * aggregate.Key + 30_000, aggregate.Sum);
        }
        Assert.Equal(5_000, result.Select(value => value.Key).Distinct().Count());
    }

    private static SpillOptions Options(string root) => new()
    {
        TemporaryDirectory = root,
        CleanupAbandonedSessions = false,
        MemoryLimitBytes = 64 * 1024,
        PartitionCount = 8,
        RecursivePartitionCount = 4
    };

    private static string NewRoot()
    {
        var path = Path.Combine(Path.GetTempPath(), $"spillflow-recoverable-aggregate-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static async IAsyncEnumerable<int> Source(
        long offset,
        int count,
        int? throwAtPosition,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        for (var position = offset; position < count; position++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (position == throwAtPosition)
                throw new SimulatedInterruptionException();
            yield return (int)position;
            await Task.Yield();
        }
    }

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source)
    {
        var result = new List<T>();
        await foreach (var item in source)
            result.Add(item);
        return result;
    }

    private sealed record Aggregate(int Key, int Count, long Sum);
    private sealed class SimulatedInterruptionException : Exception;
}
