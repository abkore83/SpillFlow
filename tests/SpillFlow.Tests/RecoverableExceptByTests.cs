using System.Runtime.CompilerServices;

namespace SpillFlow.Tests;

public sealed class RecoverableExceptByTests
{
    [Fact]
    public async Task Resumes_primary_source_from_its_last_durable_chunk()
    {
        var root = NewRoot();
        string workingDirectory;
        try
        {
            var first = new SpillSession(Options(root));
            workingDirectory = first.WorkingDirectory;
            try
            {
                var operation = first.ExceptByRecoverableSpill(
                    (offset, token) => Source(offset, 20_000, 7_500, token),
                    (offset, token) => Excluded(offset, 8_000, null, token),
                    "primary-resume", value => value % 5_000);
                await Assert.ThrowsAsync<SimulatedInterruptionException>(() => CollectAsync(operation));
                first.PreserveForRecovery();
            }
            finally
            {
                await first.DisposeAsync();
            }

            long resumedOffset = -1;
            await using var recovered = SpillSession.Recover(workingDirectory, Options(root));
            var result = await CollectAsync(recovered.ExceptByRecoverableSpill(
                (offset, token) =>
                {
                    resumedOffset = offset;
                    return Source(offset, 20_000, null, token);
                },
                (offset, token) => Excluded(offset, 8_000, null, token),
                "primary-resume", value => value % 5_000));

            Assert.InRange(resumedOffset, 1, 7_499);
            AssertExpected(result);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Resumes_exclusion_source_without_reopening_primary_source()
    {
        var root = NewRoot();
        string workingDirectory;
        try
        {
            var first = new SpillSession(Options(root));
            workingDirectory = first.WorkingDirectory;
            try
            {
                var operation = first.ExceptByRecoverableSpill(
                    (offset, token) => Source(offset, 20_000, null, token),
                    (offset, token) => Excluded(offset, 8_000, 3_500, token),
                    "excluded-resume", value => value % 5_000);
                await Assert.ThrowsAsync<SimulatedInterruptionException>(() => CollectAsync(operation));
                first.PreserveForRecovery();
            }
            finally
            {
                await first.DisposeAsync();
            }

            var primaryReopened = false;
            long excludedOffset = -1;
            await using var recovered = SpillSession.Recover(workingDirectory, Options(root));
            var result = await CollectAsync(recovered.ExceptByRecoverableSpill<int, int>(
                (offset, token) =>
                {
                    primaryReopened = true;
                    return Source(offset, 20_000, null, token);
                },
                (offset, token) =>
                {
                    excludedOffset = offset;
                    return Excluded(offset, 8_000, null, token);
                },
                "excluded-resume", value => value % 5_000));

            Assert.False(primaryReopened);
            Assert.InRange(excludedOffset, 1, 3_499);
            AssertExpected(result);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Rebuilds_interrupted_paired_partitions_from_durable_local_inputs()
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
                var operation = first.ExceptByRecoverableSpill(
                    (offset, token) => Source(offset, 20_000, null, token),
                    (offset, token) => Excluded(offset, 8_000, null, token),
                    "partition-resume",
                    value => ++keyCalls == 7_500 ? throw new SimulatedInterruptionException() : value % 5_000);
                await Assert.ThrowsAsync<SimulatedInterruptionException>(() => CollectAsync(operation));
                first.PreserveForRecovery();
            }
            finally
            {
                await first.DisposeAsync();
            }

            var primaryReopened = false;
            var excludedReopened = false;
            await using var recovered = SpillSession.Recover(workingDirectory, Options(root));
            var result = await CollectAsync(recovered.ExceptByRecoverableSpill<int, int>(
                (offset, token) =>
                {
                    primaryReopened = true;
                    return Source(offset, 20_000, null, token);
                },
                (offset, token) =>
                {
                    excludedReopened = true;
                    return Excluded(offset, 8_000, null, token);
                },
                "partition-resume", value => value % 5_000));

            Assert.False(primaryReopened);
            Assert.False(excludedReopened);
            AssertExpected(result);
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
        const int replayWindow = 100;
        try
        {
            var first = new SpillSession(Options(root));
            workingDirectory = first.WorkingDirectory;
            try
            {
                var operation = first.ExceptByRecoverableSpill(
                    (offset, token) => Source(offset, 20_000, null, token),
                    (offset, token) => Excluded(offset, 8_000, null, token),
                    "output-resume", value => value % 5_000,
                    outputCheckpointInterval: replayWindow);
                await Assert.ThrowsAsync<SimulatedInterruptionException>(async () =>
                {
                    await foreach (var value in operation)
                    {
                        delivered.Add(value);
                        if (delivered.Count == 1_550)
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
            var replayed = await CollectAsync(recovered.ExceptByRecoverableSpill<int, int>(
                (offset, token) => throw new InvalidOperationException("Output recovery must not reopen the source."),
                (offset, token) => throw new InvalidOperationException("Output recovery must not reopen exclusions."),
                "output-resume", value => value % 5_000,
                outputCheckpointInterval: replayWindow));

            var overlap = delivered.Intersect(replayed).Count();
            Assert.InRange(overlap, 0, replayWindow - 1);
            Assert.Equal(3_000, delivered.Concat(replayed).Distinct().Count());
            Assert.Equal(3_000 + overlap, delivered.Count + replayed.Count);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    private static void AssertExpected(IReadOnlyCollection<int> result)
    {
        Assert.Equal(3_000, result.Count);
        Assert.Equal(Enumerable.Range(2_000, 3_000), result.Order());
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
        var path = Path.Combine(Path.GetTempPath(), $"spillflow-recoverable-exceptby-{Guid.NewGuid():N}");
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

    private static async IAsyncEnumerable<int> Excluded(
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
            yield return (int)position % 2_000;
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
