using System.Runtime.CompilerServices;

namespace SpillFlow.Tests;

public sealed class RecoverableOrderByTests
{
    [Fact]
    public async Task Resumes_input_from_the_last_durable_run_boundary()
    {
        var root = NewRoot();
        string workingDirectory;
        try
        {
            var first = new SpillSession(Options(root));
            workingDirectory = first.WorkingDirectory;
            try
            {
                var operation = first.OrderByRecoverableSpill(
                    (offset, token) => DescendingSource(offset, 5_000, throwAtPosition: 2_500, token),
                    "input-resume", value => value);
                await Assert.ThrowsAsync<SimulatedInterruptionException>(() => CollectAsync(operation));
                first.PreserveForRecovery();
            }
            finally
            {
                await first.DisposeAsync();
            }

            long resumedOffset = -1;
            await using var recovered = SpillSession.Recover(workingDirectory, Options(root));
            var result = await CollectAsync(recovered.OrderByRecoverableSpill(
                (offset, token) =>
                {
                    resumedOffset = offset;
                    return DescendingSource(offset, 5_000, throwAtPosition: null, token);
                },
                "input-resume", value => value));

            Assert.InRange(resumedOffset, 1, 2_499);
            Assert.Equal(Enumerable.Range(0, 5_000), result);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Resumes_from_a_durable_merge_replacement_without_reopening_the_source()
    {
        var root = NewRoot();
        string workingDirectory;
        try
        {
            using var cancellation = new CancellationTokenSource();
            var mergeCheckpoints = 0;
            var first = new SpillSession(Options(root));
            workingDirectory = first.WorkingDirectory;
            try
            {
                var operation = first.OrderByRecoverableSpill(
                    (offset, token) => DescendingSource(offset, 8_000, throwAtPosition: null, token),
                    "merge-resume", value => value,
                    checkpointed: progress =>
                    {
                        if (progress.Phase == SpillOperationPhase.Merging && ++mergeCheckpoints == 2)
                            cancellation.Cancel();
                    },
                    cancellationToken: cancellation.Token);
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => CollectAsync(operation));
                first.PreserveForRecovery();
            }
            finally
            {
                await first.DisposeAsync();
            }

            var sourceReopened = false;
            await using var recovered = SpillSession.Recover(workingDirectory, Options(root));
            var result = await CollectAsync(recovered.OrderByRecoverableSpill(
                (offset, token) =>
                {
                    sourceReopened = true;
                    return DescendingSource(offset, 8_000, throwAtPosition: null, token);
                },
                "merge-resume", value => value));

            Assert.False(sourceReopened);
            Assert.Equal(Enumerable.Range(0, 8_000), result);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Output_recovery_replays_only_the_uncheckpointed_at_least_once_window()
    {
        var root = NewRoot();
        string workingDirectory;
        try
        {
            var deliveredBeforeCrash = new List<int>();
            var first = new SpillSession(Options(root));
            workingDirectory = first.WorkingDirectory;
            try
            {
                var operation = first.OrderByRecoverableSpill(
                    (offset, token) => DescendingSource(offset, 5_000, throwAtPosition: null, token),
                    "output-resume", value => value,
                    outputCheckpointInterval: 1_000);
                await Assert.ThrowsAsync<SimulatedInterruptionException>(async () =>
                {
                    await foreach (var value in operation)
                    {
                        deliveredBeforeCrash.Add(value);
                        if (deliveredBeforeCrash.Count == 1_500)
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
            var replayed = await CollectAsync(recovered.OrderByRecoverableSpill<int, int>(
                (offset, token) => throw new InvalidOperationException("Output recovery must not reopen the source."),
                "output-resume", value => value,
                outputCheckpointInterval: 1_000));

            Assert.Equal(Enumerable.Range(0, 1_500), deliveredBeforeCrash);
            Assert.Equal(Enumerable.Range(1_000, 4_000), replayed);
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
        MergeFanIn = 2
    };

    private static string NewRoot()
    {
        var path = Path.Combine(Path.GetTempPath(), $"spillflow-recoverable-orderby-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static async IAsyncEnumerable<int> DescendingSource(
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
            yield return count - 1 - (int)position;
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
