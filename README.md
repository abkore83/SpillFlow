# SpillFlow

SpillFlow is an experimental .NET 8 library for bounded-memory operations over
`IAsyncEnumerable<T>`. It provides `OrderBySpill`, `DistinctSpill`,
`ExceptBySpill`, `GroupJoinSpill`, `GroupJoinStreamSpill`,
`DistinctRecoverableSpill`, `ExceptByRecoverableSpill`, and
`GroupAggregateSpill`.

> **Alpha:** APIs, generated codec layout, and recovery file formats may change
> before 1.0. Review [DRAWBACKS.md](DRAWBACKS.md) before production use.

## Install

```powershell
dotnet add package SpillFlow --version 0.3.0-alpha.4
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
    MemoryLimitBytes = 256L * 1024 * 1024,
    MaxProcessPrivateMemoryBytes = 1024L * 1024 * 1024
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

Every binary codec declares a stable `SchemaId`. SpillFlow stores a 128-bit
fingerprint of that identifier in each temporary block and rejects mismatched
blocks before decoding. A custom codec must change its `SchemaId` whenever its
binary layout changes.

Production-style callers can require binary codecs and reject the JSON fallback:

```csharp
await using var strictSession = new SpillSession(new SpillOptions
{
    MemoryLimitBytes = 256L * 1024 * 1024,
    AllowJsonFallback = false
});
```

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

`GroupJoinSpill` returns each match group as an `IReadOnlyList`, so a single
group must fit its memory lease. For a key with more matches than RAM, use
`GroupJoinStreamSpill`. Its result selector consumes an asynchronous match
stream before returning:

```csharp
var counts = accounts.GroupJoinStreamSpill(
    transactions,
    session,
    account => account.Id,
    transaction => transaction.AccountId,
    async (account, matches, cancellationToken) =>
    {
        var count = 0L;
        await foreach (var transaction in matches.WithCancellation(cancellationToken))
            count++;
        return new AccountTransactionCount(account.Id, count);
    });
```

When an oversized partition stops shrinking, the streaming operator reads its
matches directly from disk. This keeps memory bounded. Multiple left records
for the same oversized key may rescan that disk partition, so callers should
prefer grouped aggregation when they only need a count, sum, or other summary.

Sorting is stable for equal keys and enforces `MergeFanIn` through bounded
multi-pass merging. Hash operators
use the supplied equality comparer for both equality and partition routing.
Before a partition is materialized, SpillFlow applies
`PartitionMemoryExpansionFactor` and fails with `SpillResourceLimitException`
when the estimated retained state cannot fit safely inside the session budget.

`MaxProcessPrivateMemoryBytes` is an optional process-wide safety tripwire.
SpillFlow samples private memory at session creation and at record or lease
boundaries, no more often than `ProcessMemoryCheckInterval`, and reports the
largest observation in `PeakObservedProcessPrivateMemoryBytes`. Sampling cannot
guarantee a hard ceiling between checks. Use a container, Windows Job Object,
or service-level operating-system limit when strict process isolation is needed.

For long-running sorting jobs whose source can reopen at a record offset,
`OrderByRecoverableSpill` checkpoints input runs, merge replacements, and output
progress:

```csharp
await foreach (var row in session.OrderByRecoverableSpill(
    (offset, cancellationToken) => ReadTransactionsFromOffset(offset, cancellationToken),
    operationId: "daily-transactions",
    keySelector: transaction => transaction.Timestamp,
    outputCheckpointInterval: 1_000,
    cancellationToken: cancellationToken))
{
    await WriteResult(row, cancellationToken);
}
```

After an interruption, reopen the preserved directory with
`SpillSession.Recover` and call the same operation ID. Reading resumes at the
last durable run boundary, and merging resumes from the last durable run graph.
Output is at-least-once: after a crash, up to `outputCheckpointInterval` records
may be replayed. Applications needing exactly-once effects must make their sink
and SpillFlow checkpoint part of an external transactional or idempotent design.

Recoverable distinct processing uses the same offset-aware source contract:

```csharp
await foreach (var row in session.DistinctRecoverableSpill(
    (offset, cancellationToken) => ReadTransactionsFromOffset(offset, cancellationToken),
    operationId: "unique-transactions",
    keySelector: transaction => transaction.Id,
    outputCheckpointInterval: 1_000,
    cancellationToken: cancellationToken))
{
    await WriteUniqueResult(row, cancellationToken);
}
```

It checkpoints durable input chunks before partitioning. Raw chunks are deleted
only after the complete recursively refined partition graph is checkpointed, so
interrupted partition construction can restart without reopening the source.
Output is at-least-once and may replay up to the configured interval within the
current partition. Recovery must use equivalent key-selector and comparer logic.

`ExceptByRecoverableSpill` accepts separate offset-aware factories for the
primary sequence and exclusion keys. Each source advances independently. Both
sets of input chunks remain available until the complete paired-partition graph
is durably saved, so interrupted partitioning restarts locally. Output has the
same at-least-once checkpoint window as recoverable distinct processing.

## Status

This is a correctness-first prototype, not yet a production banking library.
It now includes recursive partitioning, session memory leases, bounded merge
fan-in, pooled asynchronous block I/O, disk quotas, abandoned-session cleanup,
checksummed and schema-fingerprinted blocks, optional AES-GCM encryption,
source-generated codecs, private per-user session directories, explicit caller
checkpoints, operator-managed recovery for external ordering, recoverable
distinct and ExceptBy processing, and optional process private-memory monitoring.
Remaining production work includes extending recovery to join and aggregation operators,
hot-key performance optimization, secure-deletion limitations, and statistically
repeated benchmarks on multiple storage devices.

## Security

Temporary blocks can be protected with a caller-managed AES-GCM key by setting
`SpillOptions.EncryptionKey`. SpillFlow never stores or recovers the key. Do not
hard-code production keys. Session directories use a protected current-user-only
ACL on Windows and owner-only `0700` permissions on Unix by default. Permission
setup fails closed; `ProtectTemporaryDirectory = false` is an explicit compatibility
escape hatch. See [SECURITY.md](SECURITY.md) for the reporting policy and
[DRAWBACKS.md](DRAWBACKS.md) for current security boundaries.

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
