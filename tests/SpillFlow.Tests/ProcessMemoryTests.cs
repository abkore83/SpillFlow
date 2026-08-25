namespace SpillFlow.Tests;

public sealed class ProcessMemoryTests
{
    [Fact]
    public void Session_creation_rejects_process_memory_above_threshold()
    {
        var options = Options(threshold: 100);

        var exception = Assert.Throws<SpillResourceLimitException>(
            () => new SpillSession(options, () => 101));

        Assert.Contains("Process private memory exceeded", exception.Message);
        Assert.Contains("Observed 101 bytes", exception.Message);
        Assert.Contains("threshold 100 bytes", exception.Message);
        Assert.Contains("not a hard operating-system memory limit", exception.Message);
    }

    [Fact]
    public async Task Memory_lease_detects_crossed_threshold_and_reports_peak()
    {
        long observed = 50;
        var options = Options(threshold: 100, interval: TimeSpan.Zero);
        await using var session = new SpillSession(options, () => Volatile.Read(ref observed));

        Assert.Equal(50, session.Statistics.PeakObservedProcessPrivateMemoryBytes);
        Volatile.Write(ref observed, 101);

        Assert.Throws<SpillResourceLimitException>(() => session.AcquireMemoryLease(1, "test buffer"));
        Assert.Equal(101, session.Statistics.PeakObservedProcessPrivateMemoryBytes);
    }

    [Fact]
    public async Task Positive_sampling_interval_avoids_checking_every_record()
    {
        var calls = 0;
        var options = Options(threshold: 100, interval: TimeSpan.FromHours(1));
        await using var session = new SpillSession(options, () =>
        {
            Interlocked.Increment(ref calls);
            return 50;
        });

        session.ReadRecord();
        session.WroteRecord(1);
        using var lease = session.AcquireMemoryLease(1, "test buffer");

        Assert.Equal(1, calls);
        Assert.Equal(50, session.Statistics.PeakObservedProcessPrivateMemoryBytes);
    }

    [Fact]
    public async Task Disabled_monitor_does_not_call_provider()
    {
        var calls = 0;
        await using var session = new SpillSession(Options(threshold: null), () =>
        {
            Interlocked.Increment(ref calls);
            return long.MaxValue;
        });

        session.ReadRecord();
        Assert.Equal(0, calls);
        Assert.Equal(0, session.Statistics.PeakObservedProcessPrivateMemoryBytes);
    }

    [Fact]
    public void Options_reject_invalid_process_memory_settings()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new SpillSession(Options(threshold: 0)));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new SpillSession(Options(threshold: 100, interval: TimeSpan.FromMilliseconds(-1))));
    }

    [Fact]
    public void Options_reject_invalid_hot_key_join_batch_sizes()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new SpillSession(new SpillOptions { HotKeyJoinBatchSize = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new SpillSession(new SpillOptions { HotKeyJoinBatchSize = 4_097 }));
    }

    private static SpillOptions Options(long? threshold, TimeSpan? interval = null) => new()
    {
        TemporaryDirectory = Path.GetTempPath(),
        CleanupAbandonedSessions = false,
        MaxProcessPrivateMemoryBytes = threshold,
        ProcessMemoryCheckInterval = interval ?? TimeSpan.Zero
    };
}
