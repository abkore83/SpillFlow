using System.Diagnostics;
using System.Runtime.CompilerServices;
using ExternalSort;
using SpillFlow;

var count = args.Length > 0 ? int.Parse(args[0]) : 250_000;
var uniqueCount = Math.Max(1, count / 10);
var random = new Random(42);
var keys = Enumerable.Range(0, count).Select(i => i % uniqueCount).OrderBy(_ => random.Next()).ToArray();
var rows = keys.Select((key, i) => new BankRow { Id = key, Amount = i % 997 }).ToArray();
var excluded = Enumerable.Range(0, uniqueCount).Where(i => i % 3 == 0).ToArray();
var right = Enumerable.Range(0, uniqueCount).Select(i => new BankRow { Id = i, Amount = i * 2L }).ToArray();

Console.WriteLine($"Runtime: {Environment.Version}; records: {count:N0}; unique keys: {uniqueCount:N0}");
Console.WriteLine("Each result is fully enumerated and validated. Lower time and allocation are better.\n");

await Compare("Distinct",
    async () => await RunExternal(ToAsync(keys).DistinctExternal()),
    async () => await RunSpill(session => ToAsync(keys).DistinctSpill(session, x => x), x => x));

await Compare("OrderBy",
    async () => await RunExternal(ToAsync(keys).OrderByExternal(x => x)),
    async () => await RunSpill(session => ToAsync(keys).OrderBySpill(session, x => x), x => x));

await Compare("ExceptBy",
    async () => await RunExternal(ToAsync(rows).ExceptByExternal(ToAsync(excluded), x => x.Id), x => x.Id),
    async () => await RunSpill(session => ToAsync(rows).ExceptBySpill(ToAsync(excluded), session, x => x.Id), x => x.Id));

await Compare("GroupJoin",
    async () => await RunExternal(ToAsync(rows).GroupJoinExternal(ToAsync(right), x => x.Id, x => x.Id,
        (left, matches) => new ResultRow { Key = left.Id, Value = matches.Count() }), x => HashCode.Combine(x.Key, x.Value)),
    async () => await RunSpill(session => ToAsync(rows).GroupJoinSpill(ToAsync(right), session, x => x.Id, x => x.Id,
        (left, matches) => new ResultRow { Key = left.Id, Value = matches.Count }), x => HashCode.Combine(x.Key, x.Value)));

await CompareImplementations("GroupAggregate", "LINQ GroupBy",
    async () => await Measure(ToAsync(rows.GroupBy(x => x.Id)
        .Select(group => new AggregateRow { Key = group.Key, Count = group.Count(), Sum = group.Sum(x => x.Amount) })),
        x => HashCode.Combine(x.Key, x.Count, x.Sum)),
    "SpillFlow",
    async () => await RunSpill(session => ToAsync(rows).GroupAggregateSpill(
        session, x => x.Id, () => (Count: 0, Sum: 0L),
        (state, row) => (state.Count + 1, state.Sum + row.Amount),
        (key, state) => new AggregateRow { Key = key, Count = state.Count, Sum = state.Sum }),
        x => HashCode.Combine(x.Key, x.Count, x.Sum), memoryLimitBytes: 64L * 1024 * 1024));

static async Task Compare(string operation, Func<Task<Measurement>> external, Func<Task<Measurement>> spill)
    => await CompareImplementations(operation, "ExternalSort", external, "SpillFlow", spill);

static async Task CompareImplementations(string operation, string leftName, Func<Task<Measurement>> left,
    string rightName, Func<Task<Measurement>> right)
{
    var leftResult = await left();
    var rightResult = await right();
    var valid = leftResult.Count == rightResult.Count && leftResult.Checksum == rightResult.Checksum;
    Console.WriteLine(operation);
    Print(leftName, leftResult);
    Print(rightName, rightResult);
    Console.WriteLine($"  Result match: {valid}; speed ratio {rightName}/{leftName}: {rightResult.Milliseconds / leftResult.Milliseconds:N2}x\n");
}

static void Print(string name, Measurement value) =>
    Console.WriteLine($"  {name,-12} {value.Milliseconds,9:N0} ms  allocated {value.AllocatedBytes / 1024d / 1024d,9:N1} MiB  " +
                      $"spill {value.SpillBytes / 1024d / 1024d,8:N1} MiB  records {value.Count,10:N0}");

static Task<Measurement> RunExternal<T>(IAsyncEnumerable<T> source, Func<T, int>? checksum = null) => Measure(source, checksum);

static async Task<Measurement> RunSpill<T>(Func<SpillSession, IAsyncEnumerable<T>> operation, Func<T, int>? checksum,
    long memoryLimitBytes = 1L * 1024 * 1024)
{
    await using var session = new SpillSession(new SpillOptions
    {
        MemoryLimitBytes = memoryLimitBytes,
        PartitionCount = 64
    });
    session.RegisterCodec(new BankRowCodec());
    var measurement = await Measure(operation(session), checksum);
    return measurement with { SpillBytes = session.Statistics.BytesWritten };
}

static async Task<Measurement> Measure<T>(IAsyncEnumerable<T> source, Func<T, int>? checksum = null)
{
    GC.Collect();
    GC.WaitForPendingFinalizers();
    GC.Collect();
    var allocatedBefore = GC.GetTotalAllocatedBytes(true);
    var stopwatch = Stopwatch.StartNew();
    long count = 0;
    long sum = 0;
    await foreach (var item in source)
    {
        count++;
        sum = unchecked(sum + (checksum?.Invoke(item) ?? EqualityComparer<T>.Default.GetHashCode(item!)));
    }
    stopwatch.Stop();
    return new Measurement(stopwatch.Elapsed.TotalMilliseconds,
        GC.GetTotalAllocatedBytes(true) - allocatedBefore, count, sum, 0);
}

static async IAsyncEnumerable<T> ToAsync<T>(IEnumerable<T> source,
    [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
    foreach (var item in source)
    {
        cancellationToken.ThrowIfCancellationRequested();
        yield return item;
    }
    await Task.CompletedTask;
}

internal sealed record Measurement(double Milliseconds, long AllocatedBytes, long Count, long Checksum, long SpillBytes);

public sealed class BankRow
{
    public int Id { get; set; }
    public long Amount { get; set; }
}

public sealed class ResultRow
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
    public string SchemaId => "spillflow-benchmarks:bank-row:v1";
    public int GetEncodedSize(BankRow value) => 12;

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
}
