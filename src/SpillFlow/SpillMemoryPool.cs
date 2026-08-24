namespace SpillFlow;

internal sealed class SpillMemoryPool(long capacityBytes)
{
    private long _leasedBytes;
    private long _peakLeasedBytes;

    public long CapacityBytes { get; } = capacityBytes;
    public long PeakLeasedBytes => Interlocked.Read(ref _peakLeasedBytes);

    public SpillMemoryLease Acquire(long bytes, string owner)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(bytes);
        while (true)
        {
            var current = Interlocked.Read(ref _leasedBytes);
            if (bytes > CapacityBytes - current)
            {
                throw new SpillResourceLimitException(
                    $"{owner} requested a {bytes:N0}-byte memory lease, but only " +
                    $"{CapacityBytes - current:N0} of {CapacityBytes:N0} bytes remain available.");
            }

            var next = current + bytes;
            if (Interlocked.CompareExchange(ref _leasedBytes, next, current) != current)
                continue;
            UpdatePeak(next);
            return new SpillMemoryLease(this, bytes);
        }
    }

    private void UpdatePeak(long value)
    {
        while (true)
        {
            var peak = Interlocked.Read(ref _peakLeasedBytes);
            if (value <= peak || Interlocked.CompareExchange(ref _peakLeasedBytes, value, peak) == peak)
                return;
        }
    }

    internal void Release(long bytes) => Interlocked.Add(ref _leasedBytes, -bytes);
}

internal sealed class SpillMemoryLease(SpillMemoryPool owner, long bytes) : IDisposable
{
    private SpillMemoryPool? _owner = owner;

    public long Bytes { get; } = bytes;

    public void Dispose()
    {
        Interlocked.Exchange(ref _owner, null)?.Release(Bytes);
    }
}
