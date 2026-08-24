using System.Runtime.CompilerServices;

namespace SpillFlow.Tests;

public sealed class RecoverableGroupJoinStreamTests
{
    [Fact]
    public async Task Resumes_left_source_from_its_last_durable_chunk()
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
                    (offset, token) => Values(offset, 10_000, 3_500, token),
                    (offset, token) => Values(offset, 20_000, null, token),
                    "left-resume");
                await Assert.ThrowsAsync<SimulatedInterruptionException>(() => CollectAsync(operation));
                first.PreserveForRecovery();
            }
            finally
            {
                await first.DisposeAsync();
            }

            long leftOffset = -1;
            await using var recovered = SpillSession.Recover(workingDirectory, Options(root));
            var result = await CollectAsync(Operation(
                recovered,
                (offset, token) =>
                {
                    leftOffset = offset;
                    return Values(offset, 10_000, null, token);
                },
                (offset, token) => Values(offset, 20_000, null, token),
                "left-resume"));

            Assert.InRange(leftOffset, 1, 3_499);
            AssertExpected(result);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Resumes_right_source_without_reopening_left_source()
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
                    (offset, token) => Values(offset, 10_000, null, token),
                    (offset, token) => Values(offset, 20_000, 7_500, token),
                    "right-resume");
                await Assert.ThrowsAsync<SimulatedInterruptionException>(() => CollectAsync(operation));
                first.PreserveForRecovery();
            }
            finally
            {
                await first.DisposeAsync();
            }

            var leftReopened = false;
            long rightOffset = -1;
            await using var recovered = SpillSession.Recover(workingDirectory, Options(root));
            var result = await CollectAsync(recovered.GroupJoinStreamRecoverableSpill<int, int, int, JoinResult>(
                (offset, token) =>
                {
                    leftReopened = true;
                    return Values(offset, 10_000, null, token);
                },
                (offset, token) =>
                {
                    rightOffset = offset;
                    return Values(offset, 20_000, null, token);
                },
                "right-resume", value => value % 5_000, value => value % 5_000, SelectResult));

            Assert.False(leftReopened);
            Assert.InRange(rightOffset, 1, 7_499);
            AssertExpected(result);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Rebuilds_interrupted_partitions_without_reopening_either_source()
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
                var operation = first.GroupJoinStreamRecoverableSpill(
                    (offset, token) => Values(offset, 10_000, null, token),
                    (offset, token) => Values(offset, 20_000, null, token),
                    "partition-resume",
                    value => ++keyCalls == 3_500 ? throw new SimulatedInterruptionException() : value % 5_000,
                    value => value % 5_000,
                    SelectResult);
                await Assert.ThrowsAsync<SimulatedInterruptionException>(() => CollectAsync(operation));
                first.PreserveForRecovery();
            }
            finally
            {
                await first.DisposeAsync();
            }

            var leftReopened = false;
            var rightReopened = false;
            await using var recovered = SpillSession.Recover(workingDirectory, Options(root));
            var result = await CollectAsync(recovered.GroupJoinStreamRecoverableSpill<int, int, int, JoinResult>(
                (offset, token) =>
                {
                    leftReopened = true;
                    return Values(offset, 10_000, null, token);
                },
                (offset, token) =>
                {
                    rightReopened = true;
                    return Values(offset, 20_000, null, token);
                },
                "partition-resume", value => value % 5_000, value => value % 5_000, SelectResult));

            Assert.False(leftReopened);
            Assert.False(rightReopened);
            AssertExpected(result);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Hot_key_output_recovers_with_bounded_replay_without_materializing_matches()
    {
        var root = NewRoot();
        string workingDirectory;
        var delivered = new List<JoinResult>();
        const int replayWindow = 3;
        try
        {
            var first = new SpillSession(Options(root));
            workingDirectory = first.WorkingDirectory;
            try
            {
                var operation = first.GroupJoinStreamRecoverableSpill(
                    (offset, token) => Values(offset, 10, null, token),
                    (offset, token) => Values(offset, 20_000, null, token),
                    "hot-output", _ => 1, _ => 1, SelectResult,
                    outputCheckpointInterval: replayWindow);
                await Assert.ThrowsAsync<SimulatedInterruptionException>(async () =>
                {
                    await foreach (var value in operation)
                    {
                        delivered.Add(value);
                        if (delivered.Count == 5)
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
            var replayed = await CollectAsync(recovered.GroupJoinStreamRecoverableSpill<int, int, int, JoinResult>(
                (offset, token) => throw new InvalidOperationException("Output recovery must not reopen the left source."),
                (offset, token) => throw new InvalidOperationException("Output recovery must not reopen the right source."),
                "hot-output", _ => 1, _ => 1, SelectResult,
                outputCheckpointInterval: replayWindow));

            var overlap = delivered.Intersect(replayed).Count();
            Assert.InRange(overlap, 0, replayWindow - 1);
            var unique = delivered.Concat(replayed).Distinct().OrderBy(value => value.Left).ToArray();
            Assert.Equal(10, unique.Length);
            Assert.All(unique, value =>
            {
                Assert.Equal(20_000, value.MatchCount);
                Assert.Equal(199_990_000, value.MatchSum);
            });
            Assert.InRange(recovered.Statistics.PeakLeasedBytes, 1, Options(root).MemoryLimitBytes * 3 / 4);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    private static IAsyncEnumerable<JoinResult> Operation(
        SpillSession session,
        Func<long, CancellationToken, IAsyncEnumerable<int>> leftFactory,
        Func<long, CancellationToken, IAsyncEnumerable<int>> rightFactory,
        string operationId) =>
        session.GroupJoinStreamRecoverableSpill(
            leftFactory, rightFactory, operationId,
            value => value % 5_000, value => value % 5_000, SelectResult);

    private static async ValueTask<JoinResult> SelectResult(
        int left,
        IAsyncEnumerable<int> matches,
        CancellationToken cancellationToken)
    {
        var count = 0;
        long sum = 0;
        await foreach (var match in matches.WithCancellation(cancellationToken))
        {
            count++;
            sum += match;
        }
        return new JoinResult(left, count, sum);
    }

    private static void AssertExpected(IReadOnlyCollection<JoinResult> result)
    {
        Assert.Equal(10_000, result.Count);
        Assert.Equal(10_000, result.Select(value => value.Left).Distinct().Count());
        foreach (var value in result)
        {
            var key = value.Left % 5_000;
            Assert.Equal(4, value.MatchCount);
            Assert.Equal(4L * key + 30_000, value.MatchSum);
        }
    }

    private static SpillOptions Options(string root) => new()
    {
        TemporaryDirectory = root,
        CleanupAbandonedSessions = false,
        MemoryLimitBytes = 64 * 1024,
        PartitionCount = 8,
        RecursivePartitionCount = 4,
        MaxPartitionDepth = 2
    };

    private static string NewRoot()
    {
        var path = Path.Combine(Path.GetTempPath(), $"spillflow-recoverable-join-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static async IAsyncEnumerable<int> Values(
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

    private sealed record JoinResult(int Left, int MatchCount, long MatchSum);
    private sealed class SimulatedInterruptionException : Exception;
}
