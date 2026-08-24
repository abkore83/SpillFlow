# SpillFlow Architecture and Algorithms

This document explains what SpillFlow does, how it works, and how it differs
from ExternalSort. It uses simple language, but it does not hide important
technical limits.

## 1. The problem

Normal LINQ operations usually keep their working data in RAM. For example,
`Distinct` must remember every key it has already seen. `OrderBy` normally holds
all records before it returns the first result.

This is fast when the data fits in memory. It can slow down or fail when the
input is larger than available RAM.

External-memory software solves this problem by using both RAM and temporary
disk storage. SpillFlow is an experimental .NET 8 library that applies this idea
to familiar asynchronous sequence operations.

## 2. The short comparison

ExternalSort and SpillFlow can both move work to disk. SpillFlow adds a broader
resource-control architecture around that basic idea.

| Area | ExternalSort comparison package | SpillFlow |
| --- | --- | --- |
| Main purpose | Disk-backed sequence work | Disk-backed work with shared resource controls |
| Memory policy | Package-specific behavior | Explicit session memory leases |
| Large hash partition | Package behavior | Recursively split until safe or fail clearly |
| Merge readers | External merge behavior | Configured maximum fan-in per merge pass |
| Disk limit | No matching public telemetry used in our test | Reserve bytes before writing and enforce a quota |
| Temporary block checks | Not compared through its public API | Versioned blocks with CRC32 and schema fingerprints |
| Temporary encryption | Not part of our comparison | Optional caller-keyed AES-GCM |
| Recovery | Not part of our comparison | Session manifest and caller checkpoints |
| Encoding | Package implementation | Built-in, custom, generated binary, or JSON fallback codecs |
| Operations tested | Distinct, OrderBy, ExceptBy, GroupJoin | The same four plus grouped aggregation |

This table describes the versions and public behavior evaluated by this
project. It is not a claim that ExternalSort has no other features.

## 3. Main strategy

SpillFlow follows three rules:

1. Divide large work into small pieces.
2. Reserve a resource before using it.
3. Stream results instead of collecting the full result in RAM.

The goal is predictable failure. If a safe operation is impossible under the
configured limits, SpillFlow throws a clear resource exception instead of
quietly using unlimited memory or disk.

```text
Large input
    |
Choose the operator algorithm
    |
Use bounded RAM and temporary disk blocks
    |
Process safe partitions or merge safe groups of runs
    |
Stream the result through IAsyncEnumerable
```

## 4. Architecture

```text
Application
    |
SpillFlow extension operators
    |
Operator algorithms
    |
SpillSession resource controller
    |
Codecs and versioned block I/O
    |
Temporary files
```

### Application layer

The caller supplies an `IAsyncEnumerable<T>`, key selectors, comparers, codecs,
and resource settings. The caller reads each output record as it becomes
available.

### Operator layer

The operator layer provides:

- `DistinctSpill`
- `ExceptBySpill`
- `GroupJoinSpill`
- `GroupJoinStreamSpill`
- `GroupAggregateSpill`
- `OrderBySpill`

Each operator uses an algorithm suited to its job. They share the same session
and storage system.

### SpillSession layer

`SpillSession` owns the policy for one set of operations. It manages:

- memory leases
- temporary-disk reservations
- temporary files
- registered codecs
- optional encryption material
- checksums and block validation
- cancellation and cleanup
- resource statistics
- recovery metadata and checkpoints

This central session is important. A small-data fast path and a large-data spill
path use the same limits instead of inventing separate rules.

### Codec layer

A codec converts a record to and from bytes. SpillFlow includes fixed-width
codecs for common primitive types. Applications can register a custom codec.

The source generator can create deterministic binary codecs for supported C#
record shapes. Generated binary encoding is normally smaller and more
predictable than JSON. Unsupported generated models produce a compile-time
diagnostic. JSON remains a convenient but slower fallback.

### Block-storage layer

SpillFlow groups many records into one pooled block instead of making one disk
write for every record. A temporary block contains a versioned header, a 128-bit
codec/schema fingerprint, encoded record lengths, record data, a CRC32 checksum,
and optional AES-GCM fields.

The read path checks lengths, decrypts when enabled, verifies integrity, and
then decodes records. The session reserves disk space before a physical write.

## 5. Algorithms by operation

### Distinct

`DistinctSpill` uses comparer-compatible hash partitioning.

```text
Input record
    |
Get the key and comparer hash
    |
Choose a disk partition
    |
Process one partition in RAM
    |
Return each key once
```

Equal keys must go to the same partition. SpillFlow therefore uses the supplied
equality comparer for both equality checks and partition routing. This protects
correctness for case-insensitive and other custom comparers.

For small inputs, the operator can stay in RAM and avoid temporary files.

### ExceptBy

`ExceptBySpill` partitions the source keys and exclusion keys with the same hash
rules. Corresponding partitions are processed together. A source record is
returned only when its key does not exist in the matching exclusion partition.

### GroupJoin

`GroupJoinSpill` also creates matching partitions for both inputs. Equal join
keys reach the same partition, so SpillFlow can build and use a bounded lookup
for one partition at a time.

A very frequent key can still create a huge group. Hashing cannot split one key
into smaller keys. `GroupJoinSpill` fails safely when its required
`IReadOnlyList` cannot fit. `GroupJoinStreamSpill` provides the bounded
alternative: its callback consumes matches through `IAsyncEnumerable`. When a
partition stops shrinking, SpillFlow streams matching records from disk rather
than repeating all remaining hash depths. Several left records sharing the same
oversized key can cause repeated disk scans.

### Grouped aggregation

`GroupAggregateSpill` uses a hybrid algorithm:

```text
Start aggregation in RAM
    |
Estimated state below threshold?
    | yes                 | no
Keep fast RAM path       Spill buffered data into partitions
                         and continue in bounded mode
```

It stores one accumulator per key instead of every member of every group. It
starts spilling when estimated accumulator state crosses 75 percent of the
session memory budget. The input is not read a second time.

### OrderBy

`OrderBySpill` uses external merge sort:

1. Read a memory-sized batch.
2. Sort that batch in RAM.
3. Write the sorted batch as a disk run.
4. Repeat until the input ends.
5. Merge a limited number of runs at a time.
6. Stream the final sorted result.

Sorting is stable. Records receive an in-memory encounter sequence while a run
is built, and that sequence breaks equal-key ties inside the run. Sequence
numbers are not written to disk. During every merge pass, the earlier input run
wins equal-key ties. Because runs represent contiguous source ranges and merge
groups retain their order, equal-key records preserve source encounter order
through the final output.

`MergeFanIn` limits how many run readers may be opened by one merge. When there
are too many runs, SpillFlow performs more than one merge pass. This trades some
extra I/O for bounded file handles and buffers.

## 6. Recursive partitioning

A first hash partition may still be too large for RAM. Before loading it,
SpillFlow estimates how much decoded memory it could need.

```text
Partition
    |
Estimated retained state fits a memory lease?
    | yes                      | no
Process it                    Hash it again at a deeper level
                              into smaller child partitions
```

By default, recursive refinement creates 16 child partitions and allows up to
four refinement levels. These values are configurable. If the data still
cannot fit safely, SpillFlow throws `SpillResourceLimitException`.

## 7. Memory and CPU strategy

### Memory leases

A lease is permission to retain a known number of bytes. An operator asks the
session before loading a partition or keeping a large buffer. The configured
memory limit controls SpillFlow-owned retained state.

It is not a hard limit for the whole process. The CLR, operating system, user
callbacks, codecs, and other libraries can allocate memory outside the leases.

### Pooled asynchronous I/O

SpillFlow rents reusable byte arrays from `ArrayPool`, fills blocks, and writes
them asynchronously. This reduces small allocations, garbage-collection work,
and system calls. Large sequential block I/O is also friendlier to storage than
many tiny writes.

The design does not promise low total CPU for every workload. Encoding,
hashing, checksums, encryption, repartitioning, and extra merge passes all cost
CPU. The goal is controlled resource use with useful performance.

## 8. Disk, integrity, and security strategy

- `MaxDiskUsageBytes` sets a temporary-disk quota.
- Disk bytes are reserved before a block is written.
- Consumed temporary files are removed as early as the algorithm permits.
- CRC32 detects accidental block corruption.
- Optional AES-GCM provides authenticated encryption for temporary blocks.
- The caller owns and protects the encryption key.
- Windows session directories are created atomically with a protected ACL that grants the current user inheritable full control.
- Unix session directories use owner-only `0700` permissions.
- Recovery reapplies private permissions before it reads the session manifest.
- Disposing the session removes its temporary files.
- Old abandoned session directories can be cleaned after a configured age.

SpillFlow does not provide a key vault or guaranteed physical secure deletion.
Permission failures stop session creation unless the caller explicitly sets
`ProtectTemporaryDirectory` to `false` for compatibility.

## 9. Recovery strategy

A recovery manifest records the session identity, format versions, files, disk
accounting, and caller checkpoints. SpillFlow writes checkpoint replacements to
a flushed temporary file and then moves the file into place.

Recovery validates versions and file names before accepting preserved data. It
does not yet restart every internal operator phase automatically. The calling
application must use the saved checkpoint to coordinate its restart.

## 10. Why the measured prototype was faster

The benchmark evidence suggests several combined reasons:

- pooled block I/O reduced tiny writes and temporary allocations
- binary representations reduced encoding overhead for tested types
- comparer-compatible partitioning kept matching data together
- bounded processing avoided large in-memory structures
- streaming avoided building the complete output in RAM
- limited merge groups prevented an uncontrolled number of active readers

The benchmark does not isolate each improvement, so it cannot prove how much
speed came from each design choice.

## 11. Recorded benchmark results

Each comparison ran in a fresh Release process under a 1 GiB managed-heap cap.
SpillFlow used a 64 MiB session memory budget. Output counts and checksums had to
match.

At 100 million records, SpillFlow was faster in all four comparable cases:

| Operation | SpillFlow | ExternalSort | Measured speedup |
| --- | ---: | ---: | ---: |
| Distinct | 56.4 seconds | 262.7 seconds | 4.66x |
| OrderBy | 133.8 seconds | 348.4 seconds | 2.60x |
| ExceptBy | 251.5 seconds | 379.6 seconds | 1.51x |
| GroupJoin | 132.3 seconds | 359.9 seconds | 2.72x |

In the supplemental one-billion-record `Distinct` run:

- SpillFlow completed in 18 minutes 19 seconds.
- ExternalSort completed in 60 minutes 57 seconds.
- The measured wall-time speedup was 3.33x.
- Both returned 100 million unique rows with the same checksum.
- LINQ ran out of memory under the same managed-heap ceiling.
- SpillFlow peak working set was 154.8 MB.
- ExternalSort peak working set was 176.4 MB.
- SpillFlow recorded 0.91 GB of cumulative managed allocations.
- ExternalSort recorded 375.2 GB of cumulative managed allocations.

These were single runs on one incompletely recorded machine. They demonstrate
prototype feasibility, not guaranteed performance on every computer, data set,
or storage device. See [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) for the full
record and reproduction commands.

## 12. When to use each option

Use normal LINQ when the complete working state safely fits in RAM. It is often
the fastest and simplest choice for small inputs.

Consider SpillFlow when:

- input size is unknown or can exceed RAM
- predictable temporary-disk limits matter
- custom equality comparers must remain correct
- results should stream asynchronously
- temporary integrity checks or encryption are useful
- an application needs observable resource statistics

ExternalSort remains a valid disk-backed comparison library. Teams should run
their own representative tests before choosing either implementation.

## 13. Current limitations

SpillFlow is an alpha prototype, not yet a production banking library. Important
remaining work includes:

- reduce repeated disk scans when many left records share one oversized join key
- automatically resume complete operator phases
- run repeated benchmarks with confidence intervals
- publish full hardware and storage details
- test multiple operating systems and storage devices
- reduce cumulative allocations in `ExceptBySpill` and `GroupJoinSpill`

The detailed risk list is maintained in [DRAWBACKS.md](DRAWBACKS.md).

## 14. Summary

External-memory sorting and hash partitioning are established algorithms.
SpillFlow's value is the way it combines them for .NET with one shared control
system for memory, disk, files, block integrity, optional encryption, cleanup,
recovery, generated codecs, and asynchronous streaming.

The core design is simple:

> Divide large work, control every temporary resource, and stream the result.
