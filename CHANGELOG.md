# Changelog

All notable changes to this project are documented here. SpillFlow follows
[Semantic Versioning](https://semver.org/) after the `0.x` development series.

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
