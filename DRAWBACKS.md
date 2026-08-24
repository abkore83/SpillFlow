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
- Sessions support atomic typed checkpoints and explicit file recovery.

## Remaining production work

1. Memory leases bound SpillFlow-owned retained state, but cannot measure every
   CLR/runtime allocation or memory used by user callbacks. `MemoryLimitBytes`
   is therefore a library budget, not a hard process-memory guarantee.
2. `GroupJoinSpill` requires an `IReadOnlyList` and therefore still rejects a
   group that cannot fit its memory lease. `GroupJoinStreamSpill` is the bounded
   alternative, but it may rescan an oversized right partition for each left
   record sharing that key.
3. Recovery restores checked files, disk accounting, and caller source state.
   Operators do not yet automatically restart their internal phase from a
   checkpoint; the caller must resume from its saved source cursor.
4. SpillFlow deletes temporary files normally, but cannot guarantee physical
   secure deletion on SSDs, copy-on-write filesystems, snapshots, or backups.
5. Hash-based operators do not preserve encounter order, and `OrderBySpill` is
   not stable for equal keys unless callers include a sequence tie-breaker.
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
operator-managed restart checkpoints, hot-key performance optimization,
secure-deletion policy, and broader soak testing.
