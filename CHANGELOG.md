# Changelog

All notable changes to this project are documented here. SpillFlow follows
[Semantic Versioning](https://semver.org/) after the `0.x` development series.

## 0.3.0-alpha.5 - 2026-08-24

- Added `GroupAggregateRecoverableSpill` with durable input, partitioning, output, and completed phases.
- Rebuilds interrupted partition work from local input chunks without reopening the source.
- Recomputes only the active output partition after interruption without serializing arbitrary accumulator types.
- Preserves deterministic first-seen key order for bounded at-least-once output checkpointing.
- Added exact count-and-sum recovery tests for input, partition, and output interruptions.

## 0.3.0-alpha.4 - 2026-08-24

- Added `ExceptByRecoverableSpill` with independent durable offsets for its primary and exclusion sources.
- Keeps both sets of input chunks until the complete paired-partition graph is checkpointed.
- Rebuilds interrupted paired-partition work without reopening either fully consumed source.
- Checkpoints completed partition pairs and bounded at-least-once output progress.
- Added operation-specific file tracking and recursive paired-partition cleanup.
- Added simulated interruptions during primary input, exclusion input, partitioning, and output.

## 0.3.0-alpha.3 - 2026-08-24

- Added `DistinctRecoverableSpill` with durable reading, partitioning, output, and completed phases.
- Resumes input from the last completed chunk through an offset-aware source factory.
- Keeps raw input chunks until the recursively refined partition graph is durably checkpointed.
- Rebuilds interrupted partition work without reopening the source.
- Checkpoints completed partitions and bounded at-least-once output progress.
- Cleans incomplete, superseded, and already-consumed operation files during recovery.
- Added simulated interruptions during input, partitioning, and output.

## 0.3.0-alpha.2 - 2026-08-24

- Added an optional sampled process private-memory safety threshold.
- Checks memory at session creation and record or memory-lease boundaries.
- Added peak observed process private-memory telemetry to session statistics.
- Added a configurable sampling interval to limit monitoring overhead.
- Documented that operating-system or container limits are still required for a hard process-memory ceiling.

## 0.3.0-alpha.1 - 2026-08-24

- Added `OrderByRecoverableSpill` with durable reading, merge, output, and completed phases.
- Resumes input from the last completed run through a caller-supplied offset-aware source factory.
- Uses crash-safe merge replacement: write the new run, save the checkpoint, then delete old runs.
- Tracks current-pass and next-pass runs to preserve efficient bounded multi-pass merging.
- Removes incomplete and superseded operation runs during recovery.
- Added configurable at-least-once output checkpoint intervals and synchronous progress notifications.
- Added simulated interruptions during input, merge, and output with verified recovery results.

## 0.2.0-alpha.4 - 2026-08-24

- Made `OrderBySpill` stable for equal keys.
- In-memory run sorting uses encounter sequence as its tie-breaker without writing sequence metadata to disk.
- Multi-pass merging uses original run order as the equal-key tie-breaker.
- Added a forced multi-run, multi-pass stability test against LINQ `OrderBy` semantics.

## 0.2.0-alpha.3 - 2026-08-24

- Added `GroupJoinStreamSpill`, which gives the result selector an asynchronous match stream instead of a materialized list.
- Oversized right-side partitions that stop shrinking switch to bounded disk streaming instead of repeating every hash depth.
- The original `GroupJoinSpill` still fails safely when its required `IReadOnlyList` cannot fit the memory lease.
- Added hot-key, no-match, and lease-budget tests.

## 0.2.0-alpha.2 - 2026-08-24

- Creates Windows session directories atomically with a protected current-user-only ACL.
- Creates Unix session directories with owner-only `0700` permissions.
- Reapplies private permissions before reading a recovered session.
- Added `ProtectTemporaryDirectory`; protection is enabled by default and permission failures fail closed.
- Added platform-aware creation and recovery permission tests.

## 0.2.0-alpha.1 - 2026-08-24

- Added a stable `SchemaId` contract to binary spill codecs.
- Added deterministic schema identifiers to generated codecs.
- Added a 128-bit codec/schema fingerprint to every version 2 temporary block.
- Rejects a temporary block before decoding when its fingerprint does not match the registered codec.
- Added `AllowJsonFallback`; set it to `false` to require binary codecs for non-primitive record types.
- Added compatibility and strict-mode tests.

This release changes the temporary-block format and the `ISpillCodec<T>`
interface. Version 1 temporary files and custom codecs built for 0.1 are not
compatible with this alpha release.

## 0.1.0-alpha.1 - 2026-08-23

- Added bounded-memory Distinct, OrderBy, ExceptBy, GroupJoin, and grouped aggregation.
- Added comparer-compatible partition hashing and bounded multi-pass merge fan-in.
- Added recursive partition refinement and session memory leases.
- Added pooled asynchronous block I/O, disk quotas, and abandoned-session cleanup.
- Added CRC32 checksums and optional AES-GCM encryption for temporary blocks.
- Added compile-time binary codec generation for common fixed and variable-width fields.
- Added explicit recovery manifests and typed checkpoints.
- Added isolated resource benchmarks against ExternalSort.
