# Open-Source Release Readiness

## Decision

SpillFlow is ready for a `0.1.0-alpha.1` source and NuGet release after the
publisher creates the public repository, adds its URL to the package metadata,
and configures the NuGet publishing credential. It is not ready to be described
as production-stable or released as `1.0.0`.

## Verified Locally

- Release solution build completes without compiler warnings.
- The full automated suite passes on Windows.
- Coverlet reports 88.7 percent line coverage and 81.8 percent branch coverage.
- The online NuGet audit reports no known vulnerable direct or transitive packages.
- CI is configured to build and test on Windows and Linux.
- The NuGet package includes the runtime, XML documentation, symbols, README,
  MIT license, and embedded codec analyzer.
- A clean project restores the local package, compiles a generated codec,
  round-trips a record, and executes `DistinctSpill` correctly.
- The exact `SpillFlow` NuGet package endpoint returned HTTP 404 during this
  audit, indicating that the ID was not published at that time. Availability is
  not reserved until the first successful push.
- The 100-million-record benchmark matrix completed under a 1 GiB managed-heap
  cap with matching output counts and checksums against ExternalSort.

## Publisher Actions

1. Create the public GitHub repository and push the reviewed source history.
2. Add `RepositoryUrl` and `PackageProjectUrl` to `SpillFlow.csproj`.
3. Enable GitHub private vulnerability reporting and branch protection.
4. Add a scoped NuGet credential or trusted-publishing configuration to CI.
5. Run CI on the public commit and review the uploaded package artifact.
6. Reserve the package ID by publishing `0.1.0-alpha.1` from that exact commit.
7. Create the matching `v0.1.0-alpha.1` release and tag.

## Stable-Release Gate

Before `1.0.0`, resolve or deliberately stabilize the limitations documented in
`DRAWBACKS.md`, especially heavy-key behavior, codec/schema fingerprints,
operator-managed restart, filesystem hardening, and broader cross-machine tests.
