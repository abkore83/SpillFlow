# SpillFlow Resource Benchmark Results

Measured on August 23, 2026. Each case ran in a fresh Release process with
`DOTNET_GCHeapHardLimit=0x40000000` (1 GiB managed-memory ceiling). SpillFlow used
a 64 MiB session memory budget. Inputs were generated lazily, and output counts
and deterministic checksums were verified across implementations.

This simulates a constrained process. It is not identical to a physical laptop
with only 1 GB total RAM because the operating system and other processes remain
outside the .NET heap cap.

## One hundred million records

These are the requested head-to-head results. ExternalSort does not provide a
grouped-aggregation operation, so the comparable matrix contains its four
supported operations. Every pair returned the same output count and checksum.

| Operation | Spill wall | External wall | Spill speedup | Spill CPU | External CPU | Spill peak WS | External peak WS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Distinct | 56.4s | 262.7s | 4.66x | 78.6s | 349.5s | 123.0 MB | 143.6 MB |
| OrderBy | 133.8s | 348.4s | 2.60x | 183.1s | 477.7s | 70.1 MB | 142.1 MB |
| ExceptBy | 251.5s | 379.6s | 1.51x | 337.3s | 524.6s | 124.8 MB | 126.9 MB |
| GroupJoin | 132.3s | 359.9s | 2.72x | 184.0s | 484.9s | 153.0 MB | 153.4 MB |

SpillFlow was faster in all four measured operations. Its largest advantage was
Distinct, while ExceptBy was the closest comparison. OrderBy used about half
the peak working set. ExceptBy and GroupJoin had nearly equal peak working sets,
so those wins are primarily CPU, allocation, and elapsed-time improvements.

| Operation | Spill allocations | External allocations | Spill peak temporary disk | Spill peak leases |
| --- | ---: | ---: | ---: | ---: |
| Distinct | 0.14 GB | 23.59 GB | 0.80 GB | 25.1 MB |
| OrderBy | 0.08 GB | 23.86 GB | 0.96 GB | 2.23 MB |
| ExceptBy | 19.43 GB | 50.76 GB | 1.65 GB | 16.8 MB |
| GroupJoin | 15.35 GB | 56.33 GB | 1.76 GB | 16.8 MB |

ExternalSort does not expose temporary-file byte counters, so its disk usage
cannot be reported from the same public API. SpillFlow stayed far below both its
64 MiB lease budget and the 1 GiB process heap ceiling. However, the high
cumulative allocations in ExceptBy and GroupJoin remain optimization targets;
bounded peak memory does not mean allocation-free execution.

## Supplemental one-billion-record Distinct result

Input: 1,000,000,000 integers with 100,000,000 unique keys.

| Implementation | Result | Wall time | CPU time | Peak working set | Peak private | Managed allocations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SpillFlow | Completed | 18m 19s | 1,442.7s | 154.8 MB | 137.3 MB | 0.91 GB |
| ExternalSort | Completed | 60m 57s | 4,968.8s | 176.4 MB | 151.8 MB | 375.2 GB |
| LINQ | Out of memory | < 1m | n/a | about 645 MB before failure | n/a | n/a |

SpillFlow and ExternalSort returned 100,000,000 rows and checksum
`4,999,999,950,000,000`. SpillFlow was 3.33 times faster by wall time and used
about 12 percent less peak working-set memory. SpillFlow recorded 16.0 GB of
cumulative block writes, 8.13 GB peak temporary disk, 15.8 MB peak leased memory,
and 122,496 blocks. ExternalSort does not expose equivalent disk telemetry.

GC collections for SpillFlow were 112 Gen0, 12 Gen1, and 5 Gen2. ExternalSort
reported 51,694 Gen0, 36,831 Gen1, and 14,117 Gen2 collections.

## Ten million records

Wall time and peak working set are shown below. All completed implementations
produced matching counts and checksums.

| Operation | SpillFlow | ExternalSort | LINQ |
| --- | --- | --- | --- |
| Distinct | 1.41s, 118 MB | 9.68s, 139 MB | 0.12s, 65 MB |
| OrderBy | 4.20s, 55 MB | 11.11s, 135 MB | 2.05s, 151 MB |
| ExceptBy | 5.68s, 95 MB | 14.91s, 118 MB | 0.25s, 71 MB |
| GroupJoin | 7.11s, 99 MB | 30.24s, 121 MB | 0.67s, 182 MB |
| GroupAggregate | 8.27s, 109 MB | not provided | 2.17s, 818 MB |

LINQ is the best choice when the full state comfortably fits memory. SpillFlow
is intended for workloads where that cannot be guaranteed. The billion-row run
demonstrates the crossover: LINQ fails, while both disk-backed libraries finish.

## Reproduce

```powershell
$env:DOTNET_GCHeapHardLimit='0x40000000'
dotnet run -c Release --project benchmarks/SpillFlow.ResourceBenchmarks -- spill distinct 100000000 64
dotnet run -c Release --project benchmarks/SpillFlow.ResourceBenchmarks -- external distinct 100000000 64
```

These are single-run measurements, not confidence intervals. Storage hardware,
filesystem cache, JIT state, antivirus, and background load affect results.
