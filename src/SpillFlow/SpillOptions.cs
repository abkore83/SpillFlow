namespace SpillFlow;

/// <summary>Controls memory, storage, partitioning, encryption, and cleanup for a spill session.</summary>
public sealed class SpillOptions
{
    /// <summary>Gets the approximate byte budget for SpillFlow-owned memory.</summary>
    public long MemoryLimitBytes { get; init; } = 256L * 1024 * 1024;
    /// <summary>Gets the parent directory used for session files, or the system temporary directory.</summary>
    public string? TemporaryDirectory { get; init; }
    /// <summary>Gets the initial number of hash partitions.</summary>
    public int PartitionCount { get; init; } = 64;
    /// <summary>Gets the maximum number of sorted runs opened by one merge pass.</summary>
    public int MergeFanIn { get; init; } = 16;
    /// <summary>Gets the multiplier used to estimate retained memory from encoded partition bytes.</summary>
    public double PartitionMemoryExpansionFactor { get; init; } = 2.0;
    /// <summary>Gets the maximum recursive partition-refinement depth.</summary>
    public int MaxPartitionDepth { get; init; } = 4;
    /// <summary>Gets the number of child partitions created during recursive refinement.</summary>
    public int RecursivePartitionCount { get; init; } = 16;
    /// <summary>Gets the preferred pooled I/O block size in bytes.</summary>
    public int IoBlockSizeBytes { get; init; } = 128 * 1024;
    /// <summary>Gets the maximum physical bytes reserved by session spill files.</summary>
    public long MaxDiskUsageBytes { get; init; } = 10L * 1024 * 1024 * 1024;
    /// <summary>Gets whether stale session directories are removed when a new session starts.</summary>
    public bool CleanupAbandonedSessions { get; init; } = true;
    /// <summary>Gets the minimum age at which a session directory is considered abandoned.</summary>
    public TimeSpan AbandonedSessionAge { get; init; } = TimeSpan.FromHours(24);
    /// <summary>Gets an optional 16, 24, or 32-byte AES-GCM key for temporary blocks.</summary>
    public byte[]? EncryptionKey { get; init; }
    /// <summary>Gets whether unregistered record types may use the slower JSON fallback.</summary>
    public bool AllowJsonFallback { get; init; } = true;

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(MemoryLimitBytes, 64 * 1024);
        ArgumentOutOfRangeException.ThrowIfLessThan(PartitionCount, 2);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(PartitionCount, 4096);
        ArgumentOutOfRangeException.ThrowIfLessThan(MergeFanIn, 2);
        ArgumentOutOfRangeException.ThrowIfLessThan(PartitionMemoryExpansionFactor, 1.0);
        ArgumentOutOfRangeException.ThrowIfNegative(MaxPartitionDepth);
        ArgumentOutOfRangeException.ThrowIfLessThan(RecursivePartitionCount, 2);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(RecursivePartitionCount, 256);
        ArgumentOutOfRangeException.ThrowIfLessThan(IoBlockSizeBytes, 4 * 1024);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(IoBlockSizeBytes, 16 * 1024 * 1024);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxDiskUsageBytes);
        ArgumentOutOfRangeException.ThrowIfLessThan(AbandonedSessionAge, TimeSpan.FromMinutes(1));
        if (EncryptionKey is not null && EncryptionKey.Length is not (16 or 24 or 32))
            throw new ArgumentException("EncryptionKey must contain 16, 24, or 32 bytes.", nameof(EncryptionKey));
    }
}

/// <summary>Indicates that a configured SpillFlow memory, partition, or disk limit was reached.</summary>
/// <param name="message">A description of the exceeded limit.</param>
public sealed class SpillResourceLimitException(string message) : InvalidOperationException(message);

/// <summary>Reports cumulative and peak resource counters for a spill session.</summary>
/// <param name="RecordsRead">The number of records decoded from temporary blocks.</param>
/// <param name="RecordsWritten">The number of records encoded into temporary blocks.</param>
/// <param name="BytesWritten">The cumulative logical record bytes written.</param>
/// <param name="SpillFiles">The number of temporary files created.</param>
/// <param name="UsedDisk">Whether the session created any temporary files.</param>
/// <param name="PeakOpenReaders">The greatest number of concurrently open record readers.</param>
/// <param name="PeakLeasedBytes">The greatest number of bytes held through session memory leases.</param>
/// <param name="BlocksWritten">The number of framed blocks written.</param>
/// <param name="CurrentDiskBytes">The currently reserved physical spill bytes.</param>
/// <param name="PeakDiskBytes">The greatest number of simultaneously reserved physical spill bytes.</param>
public sealed record SpillStatistics(
    long RecordsRead,
    long RecordsWritten,
    long BytesWritten,
    int SpillFiles,
    bool UsedDisk,
    int PeakOpenReaders,
    long PeakLeasedBytes,
    long BlocksWritten,
    long CurrentDiskBytes,
    long PeakDiskBytes);
