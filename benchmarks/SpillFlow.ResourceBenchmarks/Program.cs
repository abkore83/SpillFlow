using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using ExternalSort;
using SpillFlow;

var implementation = args.ElementAtOrDefault(0)?.ToLowerInvariant() ?? "spill";
var operation = args.ElementAtOrDefault(1)?.ToLowerInvariant() ?? "distinct";
var inputCount = int.TryParse(args.ElementAtOrDefault(2), out var parsedCount) ? parsedCount : 1_000_000;
var spillMemoryMb = int.TryParse(args.ElementAtOrDefault(3), out var parsedMemory) ? parsedMemory : 64;
var uniqueKeys = Math.Max(1, inputCount / 10);

GC.Collect();
GC.WaitForPendingFinalizers();
GC.Collect();
var process = Process.GetCurrentProcess();
process.Refresh();
long peakWorkingSet = process.WorkingSet64;
long peakPrivateBytes = process.PrivateMemorySize64;
using var samplingCts = new CancellationTokenSource();
var sampler = Task.Run(async () =>
{
    try
    {
        while (!samplingCts.IsCancellationRequested)
        {
            process.Refresh();
            peakWorkingSet = Math.Max(peakWorkingSet, process.WorkingSet64);
            peakPrivateBytes = Math.Max(peakPrivateBytes, process.PrivateMemorySize64);
            await Task.Delay(5, samplingCts.Token);
        }
    }
    catch (OperationCanceledException) { }
});

var allocationStart = GC.GetTotalAllocatedBytes(true);
var gen0Start = GC.CollectionCount(0);
var gen1Start = GC.CollectionCount(1);
var gen2Start = GC.CollectionCount(2);
var cpuStart = process.TotalProcessorTime;
var stopwatch = Stopwatch.StartNew();
SpillStatistics? spillStatistics = null;
(long Count, long Checksum) result;

if (implementation == "spill")
{
    await using var session = new SpillSession(new SpillOptions
    {
        MemoryLimitBytes = (long)spillMemoryMb * 1024 * 1024,
        MaxDiskUsageBytes = 20L * 1024 * 1024 * 1024,
        PartitionCount = 64,
        RecursivePartitionCount = 16,
        MergeFanIn = 16
    });
    session.RegisterCodec(new BankRowCodec());
    result = operation switch
    {
        "distinct" => await Consume(Keys(inputCount, uniqueKeys).DistinctSpill(session, value => value), value => value),
        "orderby" => await Consume(UnsortedKeys(inputCount).OrderBySpill(session, value => value), value => value),
        "exceptby" => await Consume(Rows(inputCount, uniqueKeys).ExceptBySpill(Excluded(uniqueKeys), session, row => row.Id), row => row.Id),
        "groupjoin" => await Consume(Rows(inputCount, uniqueKeys).GroupJoinSpill(
            ReferenceRows(uniqueKeys), session, row => row.Id, row => row.Id,
            (row, matches) => new MetricRow { Key = row.Id, Value = matches.Count }),
            row => StableMetricHash(row.Key, row.Value)),
        "aggregate" => await Consume(Rows(inputCount, uniqueKeys).GroupAggregateSpill(
            session, row => row.Id, () => (Count: 0, Sum: 0L),
            (state, row) => (state.Count + 1, state.Sum + row.Amount),
            (key, state) => new AggregateRow { Key = key, Count = state.Count, Sum = state.Sum }),
            row => StableAggregateMetricHash(row.Key, row.Count, row.Sum)),
        _ => throw new ArgumentException($"Unsupported operation '{operation}'.")
    };
    spillStatistics = session.Statistics;
}
else if (implementation == "external")
{
    result = operation switch
    {
        "distinct" => await Consume(Keys(inputCount, uniqueKeys).DistinctExternal(), value => value),
        "orderby" => await Consume(UnsortedKeys(inputCount).OrderByExternal(value => value), value => value),
        "exceptby" => await Consume(Rows(inputCount, uniqueKeys).ExceptByExternal(Excluded(uniqueKeys), row => row.Id), row => row.Id),
        "groupjoin" => await Consume(Rows(inputCount, uniqueKeys).GroupJoinExternal(
            ReferenceRows(uniqueKeys), row => row.Id, row => row.Id,
            (row, matches) => new MetricRow { Key = row.Id, Value = matches.Count() }),
            row => StableMetricHash(row.Key, row.Value)),
        _ => throw new ArgumentException($"ExternalSort does not provide operation '{operation}'.")
    };
}
else if (implementation == "linq")
{
    result = operation switch
    {
        "distinct" => ConsumeSync(Enumerable.Range(0, inputCount).Select(index => index % uniqueKeys).Distinct(), value => value),
        "orderby" => ConsumeSync(Enumerable.Range(0, inputCount)
            .Select(index => (int)((long)index * 48_271 % inputCount)).OrderBy(value => value), value => value),
        "exceptby" => ConsumeSync(Enumerable.Range(0, inputCount)
            .Select(index => new BankRow { Id = index % uniqueKeys, Amount = index % 997L })
            .ExceptBy(Enumerable.Range(0, uniqueKeys).Where(key => key % 3 == 0), row => row.Id), row => row.Id),
        "groupjoin" => ConsumeSync(Enumerable.Range(0, inputCount)
            .Select(index => new BankRow { Id = index % uniqueKeys, Amount = index % 997L })
            .GroupJoin(Enumerable.Range(0, uniqueKeys).Select(key => new BankRow { Id = key, Amount = key * 2L }),
                row => row.Id, row => row.Id,
                (row, matches) => new MetricRow { Key = row.Id, Value = matches.Count() }),
            row => StableMetricHash(row.Key, row.Value)),
        "aggregate" => ConsumeSync(Enumerable.Range(0, inputCount)
            .Select(index => new BankRow { Id = index % uniqueKeys, Amount = index % 997L })
            .GroupBy(row => row.Id).Select(group => new AggregateRow
            {
                Key = group.Key,
                Count = group.Count(),
                Sum = group.Sum(row => row.Amount)
            }), row => StableAggregateMetricHash(row.Key, row.Count, row.Sum)),
        _ => throw new ArgumentException($"Unsupported operation '{operation}'.")
    };
}
else
{
    throw new ArgumentException($"Unsupported implementation '{implementation}'.");
}

stopwatch.Stop();
process.Refresh();
var cpu = process.TotalProcessorTime - cpuStart;
samplingCts.Cancel();
await sampler;
process.Refresh();
peakWorkingSet = Math.Max(peakWorkingSet, process.WorkingSet64);
peakPrivateBytes = Math.Max(peakPrivateBytes, process.PrivateMemorySize64);

var measurement = new ResourceMeasurement
{
    Implementation = implementation,
    Operation = operation,
    InputRecords = inputCount,
    OutputRecords = result.Count,
    Checksum = result.Checksum,
    WallMilliseconds = stopwatch.Elapsed.TotalMilliseconds,
    CpuMilliseconds = cpu.TotalMilliseconds,
    PeakWorkingSetBytes = peakWorkingSet,
    PeakPrivateBytes = peakPrivateBytes,
    AllocatedBytes = GC.GetTotalAllocatedBytes(true) - allocationStart,
    Gen0Collections = GC.CollectionCount(0) - gen0Start,
    Gen1Collections = GC.CollectionCount(1) - gen1Start,
    Gen2Collections = GC.CollectionCount(2) - gen2Start,
    SpillBytesWritten = spillStatistics?.BytesWritten ?? 0,
    PeakSpillDiskBytes = spillStatistics?.PeakDiskBytes ?? 0,
    PeakLeasedBytes = spillStatistics?.PeakLeasedBytes ?? 0,
    BlocksWritten = spillStatistics?.BlocksWritten ?? 0,
    GcHeapHardLimit = Environment.GetEnvironmentVariable("DOTNET_GCHeapHardLimit"),
    GcTotalAvailableMemoryBytes = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes
};
Console.WriteLine(JsonSerializer.Serialize(measurement));

static async Task<(long Count, long Checksum)> Consume<T>(IAsyncEnumerable<T> source, Func<T, int> checksum)
{
    long count = 0;
    long sum = 0;
    await foreach (var item in source)
    {
        count++;
        sum = unchecked(sum + checksum(item));
    }
    return (count, sum);
}

static (long Count, long Checksum) ConsumeSync<T>(IEnumerable<T> source, Func<T, int> checksum)
{
    long count = 0;
    long sum = 0;
    foreach (var item in source)
    {
        count++;
        sum = unchecked(sum + checksum(item));
    }
    return (count, sum);
}

static int StableMetricHash(int key, int value) => unchecked((key * 397) ^ value);
static int StableAggregateMetricHash(int key, int count, long sum) => unchecked(((key * 397) ^ count) * 397 ^ (int)(sum ^ (sum >> 32)));

static async IAsyncEnumerable<int> Keys(int count, int uniqueKeys,
    [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
    for (var index = 0; index < count; index++)
    {
        cancellationToken.ThrowIfCancellationRequested();
        yield return index % uniqueKeys;
    }
    await Task.CompletedTask;
}

static async IAsyncEnumerable<int> UnsortedKeys(int count,
    [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
    for (var index = 0; index < count; index++)
    {
        cancellationToken.ThrowIfCancellationRequested();
        yield return (int)((long)index * 48_271 % count);
    }
    await Task.CompletedTask;
}

static async IAsyncEnumerable<BankRow> Rows(int count, int uniqueKeys,
    [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
    for (var index = 0; index < count; index++)
    {
        cancellationToken.ThrowIfCancellationRequested();
        yield return new BankRow { Id = index % uniqueKeys, Amount = index % 997L };
    }
    await Task.CompletedTask;
}

static async IAsyncEnumerable<int> Excluded(int uniqueKeys,
    [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
    for (var key = 0; key < uniqueKeys; key += 3)
    {
        cancellationToken.ThrowIfCancellationRequested();
        yield return key;
    }
    await Task.CompletedTask;
}

static async IAsyncEnumerable<BankRow> ReferenceRows(int uniqueKeys,
    [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
    for (var key = 0; key < uniqueKeys; key++)
    {
        cancellationToken.ThrowIfCancellationRequested();
        yield return new BankRow { Id = key, Amount = key * 2L };
    }
    await Task.CompletedTask;
}

public sealed class BankRow
{
    public int Id { get; set; }
    public long Amount { get; set; }
}

public sealed class MetricRow
{
    public int Key { get; set; }
    public int Value { get; set; }
}

public sealed class AggregateRow
{
    public int Key { get; set; }
    public int Count { get; set; }
    public long Sum { get; set; }
}

public sealed class BankRowCodec : ISpillCodec<BankRow>
{
    public string SchemaId => "spillflow-resource-benchmarks:bank-row:v1";
    public byte[] Encode(BankRow value)
    {
        var bytes = new byte[12];
        BitConverter.TryWriteBytes(bytes.AsSpan(0, 4), value.Id);
        BitConverter.TryWriteBytes(bytes.AsSpan(4, 8), value.Amount);
        return bytes;
    }

    public BankRow Decode(ReadOnlySpan<byte> bytes) => new()
    {
        Id = BitConverter.ToInt32(bytes[..4]),
        Amount = BitConverter.ToInt64(bytes[4..12])
    };

    public int GetEncodedSize(BankRow value) => 12;
}

public sealed class ResourceMeasurement
{
    public string Implementation { get; set; } = "";
    public string Operation { get; set; } = "";
    public int InputRecords { get; set; }
    public long OutputRecords { get; set; }
    public long Checksum { get; set; }
    public double WallMilliseconds { get; set; }
    public double CpuMilliseconds { get; set; }
    public long PeakWorkingSetBytes { get; set; }
    public long PeakPrivateBytes { get; set; }
    public long AllocatedBytes { get; set; }
    public int Gen0Collections { get; set; }
    public int Gen1Collections { get; set; }
    public int Gen2Collections { get; set; }
    public long SpillBytesWritten { get; set; }
    public long PeakSpillDiskBytes { get; set; }
    public long PeakLeasedBytes { get; set; }
    public long BlocksWritten { get; set; }
    public string? GcHeapHardLimit { get; set; }
    public long GcTotalAvailableMemoryBytes { get; set; }
}
