# Changelog

All notable changes to this project are documented here. SpillFlow follows
[Semantic Versioning](https://semver.org/) after the `0.x` development series.

## 0.1.0-alpha.1 - 2026-08-23

- Added bounded-memory Distinct, OrderBy, ExceptBy, GroupJoin, and grouped aggregation.
- Added comparer-compatible partition hashing and bounded multi-pass merge fan-in.
- Added recursive partition refinement and session memory leases.
- Added pooled asynchronous block I/O, disk quotas, and abandoned-session cleanup.
- Added CRC32 checksums and optional AES-GCM encryption for temporary blocks.
- Added compile-time binary codec generation for common fixed and variable-width fields.
- Added explicit recovery manifests and typed checkpoints.
- Added isolated resource benchmarks against ExternalSort.
