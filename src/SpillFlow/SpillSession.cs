using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace SpillFlow;

/// <summary>Owns resource limits, codecs, temporary files, statistics, and recovery state for spill operations.</summary>
public sealed class SpillSession : IAsyncDisposable
{
    private readonly ConcurrentBag<string> _paths = [];
    private int _fileCount;
    private long _recordsRead;
    private long _recordsWritten;
    private long _bytesWritten;
    private readonly ConcurrentDictionary<Type, object> _codecs = new();
    private int _openReaders;
    private int _peakOpenReaders;
    private readonly SpillMemoryPool _memoryPool;
    private long _blocksWritten;
    private long _currentDiskBytes;
    private long _peakDiskBytes;
    private bool _preserveForRecovery;
    private readonly DateTime _createdUtc;
    private readonly object _manifestLock = new();

    /// <summary>Gets the validated options used by this session.</summary>
    public SpillOptions Options { get; }
    /// <summary>Gets the session temporary directory.</summary>
    public string WorkingDirectory { get; }
    /// <summary>Gets the stable identifier stored in the recovery manifest.</summary>
    public string SessionId { get; }
    /// <summary>Gets files restored from a recovery manifest.</summary>
    public IReadOnlyList<string> RecoveredFiles { get; }
    internal byte[]? EncryptionKey { get; }

    /// <summary>Gets a current snapshot of session resource counters.</summary>
    public SpillStatistics Statistics => new(
        Interlocked.Read(ref _recordsRead),
        Interlocked.Read(ref _recordsWritten),
        Interlocked.Read(ref _bytesWritten),
        Volatile.Read(ref _fileCount),
        Volatile.Read(ref _fileCount) > 0,
        Volatile.Read(ref _peakOpenReaders),
        _memoryPool.PeakLeasedBytes,
        Interlocked.Read(ref _blocksWritten),
        Interlocked.Read(ref _currentDiskBytes),
        Interlocked.Read(ref _peakDiskBytes));

    /// <summary>Creates a new spill session.</summary>
    public SpillSession(SpillOptions? options = null) : this(options ?? new SpillOptions(), null)
    {
    }

    private SpillSession(SpillOptions options, string? recoveryDirectory)
    {
        Options = options;
        Options.Validate();
        _memoryPool = new SpillMemoryPool((long)(Options.MemoryLimitBytes * 0.75));
        EncryptionKey = Options.EncryptionKey?.ToArray();
        var root = Options.TemporaryDirectory ?? Path.GetTempPath();
        if (recoveryDirectory is null && Options.CleanupAbandonedSessions)
            CleanupAbandoned(root, Options.AbandonedSessionAge);
        if (recoveryDirectory is null)
        {
            SessionId = Guid.NewGuid().ToString("N");
            WorkingDirectory = Path.Combine(root, $"spillflow-{SessionId}");
            _createdUtc = DateTime.UtcNow;
            Directory.CreateDirectory(WorkingDirectory);
            RecoveredFiles = [];
            WriteManifest();
        }
        else
        {
            WorkingDirectory = Path.GetFullPath(recoveryDirectory);
            var manifest = ReadManifest(WorkingDirectory);
            SessionId = manifest.SessionId;
            _createdUtc = manifest.CreatedUtc;
            var files = manifest.Files.Select(name => Path.Combine(WorkingDirectory, name)).Where(File.Exists).ToArray();
            foreach (var path in files) _paths.Add(path);
            _fileCount = manifest.NextFileNumber;
            var diskBytes = files.Sum(path => new FileInfo(path).Length);
            if (diskBytes > Options.MaxDiskUsageBytes)
                throw new SpillResourceLimitException("Recovered spill files exceed the configured disk quota.");
            _currentDiskBytes = diskBytes;
            _peakDiskBytes = diskBytes;
            RecoveredFiles = files;
        }
    }

    /// <summary>Opens a session preserved in an existing working directory.</summary>
    public static SpillSession Recover(string workingDirectory, SpillOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(workingDirectory);
        if (!Directory.Exists(workingDirectory)) throw new DirectoryNotFoundException(workingDirectory);
        return new SpillSession(options ?? new SpillOptions { TemporaryDirectory = Path.GetDirectoryName(workingDirectory) }, workingDirectory);
    }

    internal string NewFile(string extension = ".spill")
    {
        ObjectDisposedException.ThrowIf(!Directory.Exists(WorkingDirectory), this);
        var path = Path.Combine(WorkingDirectory, $"{Interlocked.Increment(ref _fileCount):D8}{extension}");
        _paths.Add(path);
        WriteManifest();
        return path;
    }

    /// <summary>Atomically saves caller-defined source or operation state under a safe checkpoint name.</summary>
    public async ValueTask SaveCheckpointAsync<T>(string name, T state, CancellationToken cancellationToken = default)
    {
        var path = CheckpointPath(name);
        var temporary = path + ".tmp";
        await using (var stream = new FileStream(temporary, FileMode.Create, FileAccess.Write, FileShare.None,
                         16 * 1024, FileOptions.Asynchronous | FileOptions.WriteThrough))
        {
            await JsonSerializer.SerializeAsync(stream, state, cancellationToken: cancellationToken).ConfigureAwait(false);
            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        File.Move(temporary, path, true);
    }

    /// <summary>Loads caller-defined state from a named checkpoint.</summary>
    public async ValueTask<T> LoadCheckpointAsync<T>(string name, CancellationToken cancellationToken = default)
    {
        await using var stream = new FileStream(CheckpointPath(name), FileMode.Open, FileAccess.Read, FileShare.Read,
            16 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
        return await JsonSerializer.DeserializeAsync<T>(stream, cancellationToken: cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidDataException("Recovery checkpoint deserialized to null.");
    }

    /// <summary>Prevents disposal from deleting this session so it can be reopened with <see cref="Recover"/>.</summary>
    public void PreserveForRecovery()
    {
        ObjectDisposedException.ThrowIf(!Directory.Exists(WorkingDirectory), this);
        _preserveForRecovery = true;
        WriteManifest();
    }

    internal void ReadRecord() => Interlocked.Increment(ref _recordsRead);

    internal void ReaderOpened()
    {
        var current = Interlocked.Increment(ref _openReaders);
        while (true)
        {
            var peak = Volatile.Read(ref _peakOpenReaders);
            if (current <= peak || Interlocked.CompareExchange(ref _peakOpenReaders, current, peak) == peak)
                return;
        }
    }

    internal void ReaderClosed() => Interlocked.Decrement(ref _openReaders);

    internal long EstimatePartitionMemory(string path)
    {
        var fileBytes = new FileInfo(path).Length;
        return Math.Max(1, checked((long)Math.Ceiling(fileBytes * Options.PartitionMemoryExpansionFactor)));
    }

    internal bool CanPartitionFit(string path) => EstimatePartitionMemory(path) <= _memoryPool.CapacityBytes;

    internal bool CanPartitionsFit(params string?[] paths) =>
        paths.Where(path => path is not null && File.Exists(path))
            .Sum(path => EstimatePartitionMemory(path!)) <= _memoryPool.CapacityBytes;

    internal SpillMemoryLease AcquirePartitionLease(string path, string operation) =>
        _memoryPool.Acquire(EstimatePartitionMemory(path), $"{operation} partition '{Path.GetFileName(path)}'");

    internal SpillMemoryLease AcquirePartitionsLease(IEnumerable<string?> paths, string operation)
    {
        var existing = paths.Where(path => path is not null && File.Exists(path)).Cast<string>().ToArray();
        var bytes = existing.Sum(EstimatePartitionMemory);
        return _memoryPool.Acquire(Math.Max(1, bytes),
            $"{operation} partitions [{string.Join(", ", existing.Select(Path.GetFileName))}]");
    }

    internal SpillMemoryLease AcquireMemoryLease(long bytes, string owner) => _memoryPool.Acquire(bytes, owner);

    internal int GetIoBlockSize(int potentialWriters = 1)
    {
        var fairShare = Math.Max(1_024, Options.MemoryLimitBytes / Math.Max(4, potentialWriters * 4));
        return (int)Math.Min(Options.IoBlockSizeBytes, fairShare);
    }

    /// <summary>Registers a binary record codec for values of type <typeparamref name="T"/>.</summary>
    public SpillSession RegisterCodec<T>(ISpillCodec<T> codec)
    {
        ArgumentNullException.ThrowIfNull(codec);
        _codecs[typeof(T)] = codec;
        return this;
    }

    internal bool TryGetCodec<T>(out ISpillCodec<T>? codec)
    {
        if (_codecs.TryGetValue(typeof(T), out var value))
        {
            codec = (ISpillCodec<T>)value;
            return true;
        }
        codec = null;
        return false;
    }

    internal byte[] GetSchemaFingerprint<T>()
    {
        string schemaId;
        if (typeof(T) == typeof(int)) schemaId = "spillflow:primitive:int32-le:v1";
        else if (typeof(T) == typeof(long)) schemaId = "spillflow:primitive:int64-le:v1";
        else if (typeof(T) == typeof(Guid)) schemaId = "spillflow:primitive:guid-dotnet:v1";
        else if (TryGetCodec<T>(out var codec))
        {
            schemaId = codec!.SchemaId;
            if (string.IsNullOrWhiteSpace(schemaId))
                throw new InvalidOperationException($"The codec for '{typeof(T)}' returned an empty schema identifier.");
        }
        else
        {
            if (!Options.AllowJsonFallback)
                throw new InvalidOperationException(
                    $"No binary spill codec is registered for '{typeof(T)}' and JSON fallback is disabled.");
            schemaId = $"spillflow:json:{typeof(T).FullName}:v1";
        }

        return SHA256.HashData(Encoding.UTF8.GetBytes(schemaId))[..SpillBlockFormat.SchemaFingerprintSize];
    }

    internal void WroteRecord(long bytes)
    {
        Interlocked.Increment(ref _recordsWritten);
        Interlocked.Add(ref _bytesWritten, bytes);
    }

    internal void WroteBlock() => Interlocked.Increment(ref _blocksWritten);

    internal void ReserveDisk(long bytes)
    {
        while (true)
        {
            var current = Interlocked.Read(ref _currentDiskBytes);
            if (bytes > Options.MaxDiskUsageBytes - current)
                throw new SpillResourceLimitException(
                    $"Spill disk quota exceeded. Requested {bytes:N0} additional bytes with {current:N0} of " +
                    $"{Options.MaxDiskUsageBytes:N0} bytes already reserved.");
            var next = current + bytes;
            if (Interlocked.CompareExchange(ref _currentDiskBytes, next, current) != current)
                continue;
            UpdatePeak(ref _peakDiskBytes, next);
            return;
        }
    }

    internal void DeleteFile(string path)
    {
        if (!File.Exists(path))
            return;
        var bytes = new FileInfo(path).Length;
        File.Delete(path);
        Interlocked.Add(ref _currentDiskBytes, -bytes);
        WriteManifest();
    }

    private string CheckpointPath(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (name.Any(character => !(char.IsAsciiLetterOrDigit(character) || character is '-' or '_')))
            throw new ArgumentException("Checkpoint names may contain only ASCII letters, digits, '-' and '_'.", nameof(name));
        return Path.Combine(WorkingDirectory, $"checkpoint-{name}.json");
    }

    private void WriteManifest()
    {
        lock (_manifestLock)
        {
            var manifest = new RecoveryManifest(1, SpillBlockFormat.Version, SessionId, _createdUtc,
                Volatile.Read(ref _fileCount), _paths.Select(path => Path.GetFileName(path)!)
                    .Distinct().OrderBy(name => name).ToArray());
            var path = Path.Combine(WorkingDirectory, "session.manifest.json");
            var temporary = path + ".tmp";
            File.WriteAllText(temporary, JsonSerializer.Serialize(manifest));
            File.Move(temporary, path, true);
        }
    }

    private static RecoveryManifest ReadManifest(string directory)
    {
        var path = Path.Combine(directory, "session.manifest.json");
        var manifest = JsonSerializer.Deserialize<RecoveryManifest>(File.ReadAllText(path))
            ?? throw new InvalidDataException("Invalid spill recovery manifest.");
        if (manifest.ManifestVersion != 1 || manifest.BlockFormatVersion != SpillBlockFormat.Version)
            throw new InvalidDataException("Unsupported spill recovery format version.");
        if (manifest.Files.Any(name => Path.GetFileName(name) != name))
            throw new InvalidDataException("Recovery manifest contains an unsafe file name.");
        return manifest;
    }

    private static void UpdatePeak(ref long target, long value)
    {
        while (true)
        {
            var peak = Interlocked.Read(ref target);
            if (value <= peak || Interlocked.CompareExchange(ref target, value, peak) == peak)
                return;
        }
    }

    private static void CleanupAbandoned(string root, TimeSpan age)
    {
        if (!Directory.Exists(root))
            return;
        var cutoff = DateTime.UtcNow - age;
        foreach (var path in Directory.EnumerateDirectories(root, "spillflow-*", SearchOption.TopDirectoryOnly))
        {
            try
            {
                if (Directory.GetLastWriteTimeUtc(path) < cutoff)
                    Directory.Delete(path, recursive: true);
            }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }
        }
    }

    /// <summary>Deletes session files unless recovery preservation was requested.</summary>
    public ValueTask DisposeAsync()
    {
        if (Directory.Exists(WorkingDirectory) && !_preserveForRecovery)
            Directory.Delete(WorkingDirectory, recursive: true);
        Interlocked.Exchange(ref _currentDiskBytes, 0);
        return ValueTask.CompletedTask;
    }

    private sealed record RecoveryManifest(int ManifestVersion, int BlockFormatVersion, string SessionId,
        DateTime CreatedUtc, int NextFileNumber, string[] Files);
}
