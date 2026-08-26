# SpillFlow Production Qualification

Qualification completed on August 26, 2026. All release gates listed below passed on the current Windows x64 worktree. The Linux soak ran in the local `mcr.microsoft.com/dotnet/sdk:8.0` Docker environment.

## Gate results

| Gate | Result | Evidence |
| --- | --- | --- |
| Unit tests | Pass | 68 passed, 0 failed, 0 skipped |
| Hard-crash and fault scenarios | Pass | 22 of 22 scenarios passed |
| Process-kill recovery | Pass | All five recoverable operators passed every applicable phase |
| Output and checksum stability | Pass | One output signature per operator in benchmarks and both six-hour soaks |
| Runtime dependencies | Pass | Zero dependencies in the generated NuSpec |
| NuGet package size | Pass | 120,986 bytes; limit 153,600 bytes |
| Performance regression | Pass | Worst median delta was +8.65%; limit +10% |
| Allocation regression | Pass | Worst median delta was +0.02%; limit +10% |
| SpillFlow memory leases | Pass | Peak 16 MB with a configured 64 MB session budget |
| Linux six-hour soak | Pass | 4,351 measurements over an uninterrupted 6-hour window |
| Windows six-hour soak | Pass | 3,359 measurements over 360.08 minutes |
| Older recovery fixtures | Pass | Stored `0.3.0-alpha.8` fixtures recover for all five operators |

## Performance comparison

Five current samples were compared with the pinned 10-million-record Windows baseline. Benchmarks run before fault injection and after build-server shutdown so crash-test disk activity cannot contaminate timing evidence.

| Operator | Time delta | Allocation delta | Peak leased memory |
| --- | ---: | ---: | ---: |
| Distinct | +8.65% | +0.02% | 8 MB |
| OrderBy | -9.89% | -0.05% | 1.25 MB |
| ExceptBy | -6.04% | 0.00% | 16 MB |
| GroupJoin | -7.13% | -0.01% | 16 MB |
| GroupAggregate | -7.23% | -0.01% | 8 MB |

The memory limit is a SpillFlow working-memory lease limit, not a limit on the entire .NET process. Peak process working set was 137.02 MB on Linux and 124.84 MB on Windows; this includes the runtime, JIT code, GC heap, loaded assemblies, and operating-system buffers.

## Soak evidence

Linux run:

- Artifact: `artifacts/soak-linux/20260826-015751/status.json`
- Started: `2026-08-26T01:57:58Z`
- Completed: `2026-08-26T07:57:59Z`
- Docker exit code: 0
- OOM killed: false
- Unique signatures per operator: 1

Windows run:

- Artifact: `artifacts/soak/20260826-035759/status.json`
- Started: `2026-08-26T07:58:08Z`
- Completed: `2026-08-26T13:58:13Z`
- Requested duration: 360 minutes
- Measured duration: 360.08 minutes
- Unique signatures per operator: 1

Windows sleep was temporarily disabled for the guarded qualification. The original AC timeout of 300 seconds and battery timeout of 180 seconds were restored at `2026-08-26T13:58:13Z`.

## Final local artifacts

- Production gate: `artifacts/qualification/20260826-101531/qualification-report.json`
- Raw benchmark samples: `artifacts/qualification/20260826-101531/benchmark-samples.json`
- Benchmark comparison: `artifacts/qualification/20260826-101531/benchmark-comparison.json`
- Failure harness: `artifacts/qualification/20260826-101531/failure-harness/failure-harness-report.json`
- NuGet package: `artifacts/qualification/20260826-101531/packages/SpillFlow.0.3.0-alpha.8.nupkg`

The `artifacts` directory is intentionally local and ignored by Git because it contains generated evidence. Reproduce the fast gates with:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools/Test-ProductionGates.ps1
```
