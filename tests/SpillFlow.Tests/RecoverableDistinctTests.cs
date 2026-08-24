using System.Runtime.CompilerServices;

namespace SpillFlow.Tests;

public sealed class RecoverableDistinctTests
{
    [Fact]
    public async Task Resumes_input_from_the_last_durable_chunk()
    {
        var root = NewRoot();
        string workingDirectory;
        try
        {
            var first = new SpillSession(Options(root));
            workingDirectory = first.WorkingDirectory;
            try
            {
                var operation = first.DistinctRecoverableSpill(
                    (offset, token) => Source(offset, 20_000, throwAtPosition: 7_500, token),
                    "input-resume", value => value % 5_000);
                await Assert.ThrowsAsync<SimulatedInterruptionException>(() => CollectAsync(operation));
                first.PreserveForRecovery();
            }
            finally
            {
                await first.DisposeAsync();
            }

            long resumedOffset = -1;
            await using var recovered = SpillSession.Recover(workingDirectory, Options(root));
            var result = await CollectAsync(recovered.DistinctRecoverableSpill(
                (offset, token) =>
                {
                    resumedOffset = offset;
                    return Source(offset, 20_000, throwAtPosition: null, token);
                },
                "input-resume", value => value % 5_000));

            Assert.InRange(resumedOffset, 1, 7_499);
            Assert.Equal(5_000, result.Count);
            Assert.Equal(Enumerable.Range(0, 5_000), result.Order());
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
                var operation = first.DistinctRecoverableSpill(
                    (offset, token) => Source(offset, 20_000, throwAtPosition: null, token),
                    "partition-resume",
                    value => ++keyCalls == 7_500 ? throw new SimulatedInterruptionException() : value % 5_000);
                await Assert.ThrowsAsync<SimulatedInterruptionException>(() => CollectAsync(operation));
                first.PreserveForRecovery();
            }
            finally
            {
                await first.DisposeAsync();
            }

            var sourceReopened = false;
            await using var recovered = SpillSession.Recover(workingDirectory, Options(root));
            var result = await CollectAsync(recovered.DistinctRecoverableSpill<int, int>(
                (offset, token) =>
                {
                    sourceReopened = true;
                    return Source(offset, 20_000, throwAtPosition: null, token);
                },
                "partition-resume", value => value % 5_000));

            Assert.False(sourceReopened);
            Assert.Equal(5_000, result.Count);
            Assert.Equal(Enumerable.Range(0, 5_000), result.Order());
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Output_recovery_is_at_least_once_with_a_bounded_replay_window()
    {
        var root = NewRoot();
        string workingDirectory;
        var delivered = new List<int>();
        const int replayWindow = 200;
        try
        {
            var first = new SpillSession(Options(root));
            workingDirectory = first.WorkingDirectory;
            try
            {
                var operation = first.DistinctRecoverableSpill(
                    (offset, token) => Source(offset, 20_000, throwAtPosition: null, token),
                    "output-resume", value => value % 5_000,
                    outputCheckpointInterval: replayWindow);
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
            var replayed = await CollectAsync(recovered.DistinctRecoverableSpill<int, int>(
                (offset, token) => throw new InvalidOperationException("Output recovery must not reopen the source."),
                "output-resume", value => value % 5_000,
                outputCheckpointInterval: replayWindow));

            var overlap = delivered.Intersect(replayed).Count();
            Assert.InRange(overlap, 0, replayWindow - 1);
            Assert.Equal(5_000, delivered.Concat(replayed).Distinct().Count());
            Assert.Equal(5_000 + overlap, delivered.Count + replayed.Count);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
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
        var path = Path.Combine(Path.GetTempPath(), $"spillflow-recoverable-distinct-{Guid.NewGuid():N}");
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

    private sealed class SimulatedInterruptionException : Exception;
}
