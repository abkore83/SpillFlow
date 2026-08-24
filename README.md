# SpillFlow

SpillFlow is an experimental .NET 8 library for bounded-memory operations over
`IAsyncEnumerable<T>`. It provides `OrderBySpill`, `DistinctSpill`,
`ExceptBySpill`, `GroupJoinSpill`, and `GroupAggregateSpill`.

> **Alpha:** APIs, generated codec layout, and recovery file formats may change
> before 1.0. Review [DRAWBACKS.md](DRAWBACKS.md) before production use.

## Install

```powershell
dotnet add package SpillFlow --version 0.1.0-alpha.1
```

The package includes the codec source generator; no second analyzer package is
required. SpillFlow targets .NET 8.

The implementation uses an in-memory fast path for small distinct workloads,
stable hash partitioning for set and join operations, and sorted runs with a
k-way merge for ordering. A `SpillSession` owns temporary storage and removes it
when disposed.

```csharp
await using var session = new SpillSession(new SpillOptions
{
    MemoryLimitBytes = 256L * 1024 * 1024
});

session.RegisterCodec(new TransactionSpillCodec());

await foreach (var row in transactions.DistinctSpill(session, x => x.Id))
{
    // Process each unique transaction.
}
```

For production record types, annotate the type and register its generated codec:

```csharp
[GenerateSpillCodec]
public sealed class Transaction
{
    public Guid Id { get; set; }
    public long AmountInCents { get; set; }
    public string Currency { get; set; } = "";
}

session.RegisterCodec(new TransactionSpillCodec());
```

The generator supports public get/set properties of type `int`, `long`, `bool`,
`Guid`, and `string`. It emits checked, deterministic little-endian encoding and
rejects unsupported property types at compile time. You can instead implement
`ISpillCodec<T>` manually. `int`, `long`, and `Guid` have built-in fixed-width
encoding. Without a codec, records use the slower JSON fallback.

Grouped aggregation retains one accumulator per key instead of materializing
every member of every group:

```csharp
var totals = transactions.GroupAggregateSpill(
    session,
    row => row.AccountId,
    () => 0m,
    (total, row) => total + row.Amount,
    (accountId, total) => new AccountTotal(accountId, total));
```

`GroupAggregateSpill` begins with an adaptive in-memory path. It creates no
temporary files when the estimated accumulator state fits within 75 percent of
the session budget. If the threshold is crossed, it writes the buffered prefix
to stable hash partitions and continues in bounded-memory mode without reading
the source a second time.

Sorting enforces `MergeFanIn` through bounded multi-pass merging. Hash operators
use the supplied equality comparer for both equality and partition routing.
Before a partition is materialized, SpillFlow applies
`PartitionMemoryExpansionFactor` and fails with `SpillResourceLimitException`
when the estimated retained state cannot fit safely inside the session budget.

## Status

This is a correctness-first prototype, not yet a production banking library.
It now includes recursive partitioning, session memory leases, bounded merge
fan-in, pooled asynchronous block I/O, disk quotas, abandoned-session cleanup,
checksummed blocks, optional AES-GCM encryption, source-generated codecs, and
explicit recovery checkpoints. Remaining production work includes codec/schema
fingerprints in block headers, automatic operator-level restart orchestration,
heavy-key isolation, restricted file permissions, and statistically repeated
benchmarks on multiple storage devices.

## Security

Temporary blocks can be protected with a caller-managed AES-GCM key by setting
`SpillOptions.EncryptionKey`. SpillFlow never stores or recovers the key. Do not
hard-code production keys. See [SECURITY.md](SECURITY.md) for the reporting policy
and [DRAWBACKS.md](DRAWBACKS.md) for current security boundaries.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). This project is licensed under the
[MIT License](LICENSE).

```powershell
dotnet test SpillFlow.sln -c Release
dotnet run --project benchmarks/SpillFlow.Benchmarks -c Release -- 1000000
```

See `BENCHMARK_RESULTS.md` for the isolated 100-million-record SpillFlow versus
ExternalSort matrix under a 1 GiB cap, plus a supplemental billion-row result.

For a simple but complete explanation of the architecture, operator algorithms,
resource strategy, ExternalSort comparison, benchmark evidence, and current
limits, see [ARCHITECTURE.md](ARCHITECTURE.md).
