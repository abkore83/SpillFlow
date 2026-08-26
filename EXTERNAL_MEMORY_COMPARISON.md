# SpillFlow and ExternalSort Memory Comparison

Measured on Windows x64 on August 26, 2026.

## Result

SpillFlow used less peak process memory than ExternalSort 2.0.0-beta02 for every operation the two libraries share in the benchmark matrix.

| Operation | SpillFlow peak working set | ExternalSort peak working set | Reduction | SpillFlow private memory | ExternalSort private memory | Reduction |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Distinct | 74.8 MiB | 126.1 MiB | 40.7% | 49.4 MiB | 105.1 MiB | 52.9% |
| OrderBy | 72.9 MiB | 127.8 MiB | 43.0% | 46.6 MiB | 103.9 MiB | 55.1% |
| ExceptBy | 92.3 MiB | 116.6 MiB | 20.8% | 79.4 MiB | 94.9 MiB | 16.3% |
| GroupJoin | 94.9 MiB | 116.6 MiB | 18.6% | 67.7 MiB | 95.5 MiB | 29.1% |

## Method

- Input: 10,000,000 deterministic records per operation.
- Repetitions: three per implementation and operation.
- Process isolation: each sample ran in a new .NET process.
- SpillFlow session memory budget: 64 MiB.
- Comparison gate: SpillFlow had to use at least 10% less median peak working set and private memory.
- Correctness: output count and checksum had to match between implementations and all repetitions.
- Raw local report: `artifacts/external-memory-comparison/20260826-114701/comparison-report.json`.

Reproduce the complete comparison with:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools/Test-ExternalMemoryComparison.ps1 `
  -InputRecords 10000000 -Repetitions 3 -SpillMemoryMb 64 -RequiredRamReductionPercent 10
```

## Scope of the Claim

These results support a measured claim for the listed workload and machine. They are not proof that SpillFlow uses less memory for every record type, comparer, operating system, data distribution, or configuration. The release gate makes regressions visible and can be repeated on each supported platform.
