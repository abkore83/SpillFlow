# SpillFlow Drawback Audit

## Resolved in the current prototype

- Equality comparers now control both equality and partition hashing.
- Merge fan-in is enforced by bounded multi-pass merging.
- Oversized partitions are recursively split under session memory leases.
- Record writes use pooled asynchronous blocks with backpressure.
- Temporary storage has a hard quota and abandoned-session cleanup.
- Blocks have versioned headers, CRC32 checksums, and optional AES-GCM encryption.
- Compile-time codecs provide checked deterministic binary encoding.
- Version 2 blocks carry codec/schema fingerprints and reject incompatible codecs before decoding.
- Production callers can disable the JSON fallback and require registered binary codecs.
- Session directories use current-user-only Windows ACLs or owner-only Unix permissions by default.
- Recovery reapplies private directory permissions before reading preserved data.
- `GroupJoinStreamSpill` handles unsplittable hot-key groups with a bounded asynchronous match stream.
- `OrderBySpill` preserves encounter order for equal keys across all merge passes.
- `OrderByRecoverableSpill` resumes durable input-run, merge, and output phases.
- `DistinctRecoverableSpill` resumes durable input, partition, and output phases without reopening a fully consumed source.
- `ExceptByRecoverableSpill` independently resumes its primary and exclusion inputs, paired partitioning, and output.
- `GroupAggregateRecoverableSpill` resumes durable input, partitioning, and deterministic aggregate output.
- `GroupJoinStreamRecoverableSpill` resumes both inputs and preserves disk-streamed oversized match groups.
- Sessions support atomic typed checkpoints and explicit file recovery.
- Sessions can optionally sample process private memory, stop after crossing a configured threshold, and report the observed peak.

## Remaining production work

1. Memory leases bound SpillFlow-owned retained state. The optional process
   private-memory monitor also observes CLR, callback, and other library memory,
   but only at configured sampling points. Neither mechanism is a hard process
   limit; production systems needing strict enforcement must use an operating-
   system, container, or service-level memory limit.
2. `GroupJoinSpill` requires an `IReadOnlyList` and therefore still rejects a
   group that cannot fit its memory lease. `GroupJoinStreamSpill` is the bounded
   alternative, but it may rescan an oversized right partition for each left
   record sharing that key.
3. Operator-managed phase recovery covers ordering, distinct, ExceptBy, grouped
   aggregation, and streaming joins. The materialized-list join still relies on
   caller checkpoints; recoverable callers can use the streaming form instead.
   Input factories must reopen at durable record offsets. Output is at-least-once,
   and recovery must use equivalent deterministic callback and comparer behavior.
4. SpillFlow deletes temporary files normally, but cannot guarantee physical
   secure deletion on SSDs, copy-on-write filesystems, snapshots, or backups.
5. Hash-based operators such as `DistinctSpill`, `ExceptBySpill`, and the join
   operators do not preserve source encounter order.
6. JSON fallback remains available by default for compatibility. It is slower
   and less predictable than generated binary codecs; production callers should
   set `AllowJsonFallback` to `false`.
7. The benchmark suite uses isolated processes and deterministic validation,
   but needs repeated trials, percentiles, cold/warm cache separation, skewed
   datasets, and multiple SSD/HDD environments.

## Readiness conclusion

SpillFlow now addresses the original correctness and resource-control blockers,
and its billion-row Distinct test completes under a 1 GiB managed-memory cap.
It is still a pre-production library. Before banking deployment, prioritize
recovery support for hash operators, hot-key performance optimization,
secure-deletion policy, and broader soak testing.
