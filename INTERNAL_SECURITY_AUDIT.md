# SpillFlow Internal Security and Code Audit

Audit date: 2026-08-26  
Audit type: Maintainer-assisted internal review  
Scope: `src/SpillFlow`, `src/SpillFlow.Generators`, recovery and failure tests, package dependencies

## Conclusion

No critical vulnerability was found. Three hardening issues were fixed during this review. The full release test suite passes. NuGet reported no known vulnerable direct or transitive packages.

SpillFlow is suitable for continued alpha testing. This audit does not certify it for banking production. Header authentication, strict production defaults, large-record memory enforcement, and an independent review remain required before making that claim.

## Threat Model

Protected assets include records written to temporary storage, encryption keys, recovery state, application memory, disk capacity, and destination-side transactional effects.

Trust boundaries are:

1. Application values entering codecs or JSON serialization.
2. Spill and checkpoint files crossing process restarts.
3. Caller-provided temporary and recovery directories.
4. Caller-provided encryption keys.
5. Transactional sink implementations outside this package.

The review assumes an attacker may alter or replace spill files that operating-system permissions do not protect. It does not assume SpillFlow can defend against an operating-system administrator, a compromised application process, memory inspection, storage snapshots, or a malicious transactional sink.

## Findings

### SF-SEC-001: Recovery metadata had no size ceiling

Severity: Medium  
Status: Fixed

An oversized manifest or checkpoint could consume excessive memory and CPU during JSON loading. `MaxRecoveryMetadataBytes` now defaults to 4 MiB and is validated between 1 KiB and 64 MiB. Manifest and checkpoint sizes are checked before deserialization.

Evidence: `SpillOptions.MaxRecoveryMetadataBytes`; recovery limit test in `RecoveryTests`.

### SF-SEC-002: Recovery accepted reparse-point paths

Severity: Medium  
Status: Fixed

A recovery directory, manifest, checkpoint, or recovered spill file could be a symbolic link or Windows reparse point. Recovery now rejects these paths before reading data. The directory is checked before SpillFlow changes its permissions.

Evidence: `SpillSession.RejectReparsePoint` and recovery constructor ordering.

### SF-SEC-003: Session encryption-key copy remained in managed memory after disposal

Severity: Low  
Status: Fixed

SpillFlow correctly copied the caller key, but its private copy was not cleared. Disposal now clears that copy with `CryptographicOperations.ZeroMemory`, including when directory cleanup throws.

Evidence: `Disposal_clears_the_session_encryption_key_copy`.

### SF-SEC-004: Encrypted block headers are not authenticated as associated data

Severity: Medium  
Status: Open, release-blocking for regulated production

AES-GCM authenticates ciphertext and its tag, but the current version 2 header is not supplied as associated data. Length, checksum, flags, nonce, tag, and schema fields receive structural validation, and most tampering fails closed. However, the complete block metadata is not cryptographically bound to the ciphertext.

Recommended fix: introduce block format version 3, authenticate the immutable header fields as AES-GCM associated data, retain a read-only version 2 compatibility path, and write only version 3 files.

### SF-SEC-005: Memory limit is not a hard process-memory limit

Severity: Medium  
Status: Open, documented design limitation

The session lease pool bounds cooperating buffers. JSON serialization, codec allocations, collection overhead, framework buffers, and unusually large individual records can allocate outside that pool. `MaxProcessPrivateMemoryBytes` is a sampled tripwire, not enforcement.

Recommended fix: add an explicit maximum encoded-record size, preflight generated codecs, account for encryption buffers, and require container or Windows Job Object limits where a hard ceiling is needed.

### SF-SEC-006: Convenience defaults are not fail-closed production defaults

Severity: Medium  
Status: Open, release-blocking for regulated production

`AllowJsonFallback` defaults to true, encryption is optional, and the system temporary directory may be used. These defaults are useful for adoption but are not appropriate as an unattended banking profile.

Recommended fix: add a separate explicit production options factory or package that disables JSON fallback and abandoned-session cleanup, requires a dedicated protected directory, requires encryption, and sets finite disk and process-memory limits. Keep the core package lightweight.

### SF-SEC-007: Abandoned-session cleanup relies on a trusted root directory

Severity: Low  
Status: Open

Cleanup enumerates old `spillflow-*` directories below the configured root and deletes them recursively. It catches access and I/O failures, but it should be used only with a trusted dedicated root. Shared writable roots increase race and replacement risk.

Recommended fix: disable cleanup in regulated deployments and run a separate privileged cleanup tool that validates ownership, manifest identity, and reparse points.

## Controls Reviewed

- AES-GCM uses random 96-bit nonces and 128-bit tags.
- CRC32 detects accidental corruption for plaintext and decrypted records.
- Block magic, version, flags, lengths, schema fingerprints, and record lengths are validated.
- Spill directories use a protected Windows ACL or Unix mode 0700 by default.
- Checkpoint names and manifest file names reject path traversal.
- Disk reservations, merge fan-in, recursive partition depth, memory leases, and optional process-memory sampling limit resource use.
- Transactional output requires the destination to atomically apply a value and store its acknowledgement.
- The runtime package has no third-party runtime dependency.
- No embedded credentials, process execution, dynamic assembly loading, native interop, or legacy unsafe deserialization was found in shipped source.

## Verification

- `dotnet test SpillFlow.sln -c Release --no-restore`: 70 passed, 0 failed.
- `dotnet list SpillFlow.sln package --vulnerable --include-transitive`: no vulnerable packages reported for all projects.
- Targeted source scans covered credentials, process execution, dynamic loading, native interop, filesystem deletion, temporary paths, and deserialization.
- A warnings-as-errors build compiled every project, but NuGet advisory warning `NU1900` was promoted to an error because the build process could not reach the advisory endpoint. The separate vulnerability command reached NuGet successfully and returned clean results.

## Required Next Security Work

1. Ship authenticated version 3 block headers with version 2 read compatibility.
2. Add encoded-record limits and close remaining unleased allocation paths.
3. Add a strict opt-in production configuration profile without adding runtime dependencies.
4. Add adversarial tests for symlink replacement, disk-full partial writes, oversized records, malicious codecs, and concurrent disposal.
5. Commission an independent reviewer after these internal release blockers are closed.

## Independence Statement

This document records an internal review performed with the maintainers and automated tooling. It is not an independent security assessment, penetration test, legal opinion, compliance certification, or guarantee that the software is vulnerability-free.
