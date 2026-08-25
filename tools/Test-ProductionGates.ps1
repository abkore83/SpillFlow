[CmdletBinding()]
param(
    [ValidateRange(10000, 1000000000)]
    [int]$InputRecords = 10000000,

    [ValidateRange(1, 20)]
    [int]$Repetitions = 3,

    [ValidateRange(1, 65536)]
    [int]$SpillMemoryMb = 64,

    [ValidateRange(1, 100)]
    [double]$RegressionPercent = 10,

    [switch]$UpdateBaseline,
    [switch]$SkipBuildAndTests,
    [switch]$SkipFailureHarness
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$platform = if ($IsLinux) { 'linux-x64' } elseif ($IsMacOS) { 'macos' } else { 'windows-x64' }
$baselinePath = Join-Path $projectRoot "benchmarks/baselines/resource-$platform.json"
$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$output = Join-Path $projectRoot "artifacts/qualification/$timestamp"
$packages = Join-Path $output 'packages'
$benchmarkProject = Join-Path $projectRoot 'benchmarks/SpillFlow.ResourceBenchmarks/SpillFlow.ResourceBenchmarks.csproj'
$results = [System.Collections.Generic.List[object]]::new()
New-Item -ItemType Directory -Force -Path $output,$packages | Out-Null

function Invoke-Checked([string]$Name, [scriptblock]$Command) {
    Write-Host "[$Name]"
    & $Command
    if ($LASTEXITCODE -ne 0) { throw "$Name failed with exit code $LASTEXITCODE." }
}

function Get-Median([double[]]$Values) {
    $ordered = $Values | Sort-Object
    $middle = [int][Math]::Floor($ordered.Count / 2)
    if ($ordered.Count % 2) { return [double]$ordered[$middle] }
    return ([double]$ordered[$middle - 1] + [double]$ordered[$middle]) / 2.0
}

try {
    if (-not $SkipBuildAndTests) {
        Invoke-Checked 'restore' { dotnet restore (Join-Path $projectRoot 'SpillFlow.sln') --ignore-failed-sources }
        Invoke-Checked 'build' { dotnet build (Join-Path $projectRoot 'SpillFlow.sln') -c Release --no-restore -p:NoWarn=NU1900 }
        Invoke-Checked 'unit-tests' { dotnet test (Join-Path $projectRoot 'SpillFlow.sln') -c Release --no-build --no-restore }
    }

    if (-not $SkipFailureHarness) {
        Invoke-Checked 'all-operator-failure-harness' {
            dotnet run -c Release --no-build --project (Join-Path $projectRoot 'tools/SpillFlow.FailureHarness') -- `
                --output (Join-Path $output 'failure-harness')
        }
    }

    [xml]$coreProject = Get-Content (Join-Path $projectRoot 'src/SpillFlow/SpillFlow.csproj')
    $runtimePackages = @($coreProject.Project.ItemGroup.PackageReference |
        Where-Object { $null -ne $_ -and -not [string]::IsNullOrWhiteSpace($_.Include) })
    if ($runtimePackages.Count -ne 0) {
        throw "Core project has runtime PackageReference entries: $($runtimePackages.Include -join ', ')."
    }
    $results.Add([ordered]@{ gate = 'runtime-dependencies'; passed = $true; actual = 0; limit = 0 })

    Invoke-Checked 'pack' {
        dotnet pack (Join-Path $projectRoot 'src/SpillFlow/SpillFlow.csproj') -c Release --no-build --no-restore -o $packages
    }
    $package = Get-ChildItem $packages -Filter '*.nupkg' | Where-Object Name -NotLike '*.snupkg' |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
    $packageLimit = 150KB
    if ($package.Length -gt $packageLimit) {
        throw "Core NuGet package is $($package.Length) bytes; limit is $packageLimit bytes."
    }
    $results.Add([ordered]@{
        gate = 'package-size'; passed = $true; actualBytes = $package.Length; limitBytes = $packageLimit
    })

    Invoke-Checked 'benchmark-build' { dotnet build $benchmarkProject -c Release --no-restore -p:NoWarn=NU1900 }
    $env:DOTNET_GCHeapHardLimit = '0x40000000'
    $operations = @('distinct', 'orderby', 'exceptby', 'groupjoin', 'aggregate')
    $measurements = @{}
    foreach ($operation in $operations) {
        $runs = @()
        for ($run = 1; $run -le $Repetitions; $run++) {
            $lines = @(& dotnet run -c Release --no-build --project $benchmarkProject -- `
                spill $operation $InputRecords $SpillMemoryMb 2>&1)
            if ($LASTEXITCODE -ne 0) { throw "$operation benchmark run $run failed.`n$($lines -join "`n")" }
            $json = $lines | Where-Object { $_ -match '^\s*\{' } | Select-Object -Last 1
            if (-not $json) { throw "$operation benchmark produced no JSON measurement." }
            $runs += $json | ConvertFrom-Json
        }
        $signatures = @($runs | ForEach-Object { "$($_.OutputRecords):$($_.Checksum)" } | Select-Object -Unique)
        if ($signatures.Count -ne 1) { throw "$operation output count or checksum drifted between runs." }
        $peakLease = [long](($runs | Measure-Object PeakLeasedBytes -Maximum).Maximum)
        $leaseLimit = [long]($SpillMemoryMb * 1MB * 0.75)
        if ($peakLease -gt $leaseLimit) { throw "$operation peak lease $peakLease exceeded $leaseLimit bytes." }
        $measurements[$operation] = [ordered]@{
            wallMilliseconds = Get-Median ([double[]]@($runs.WallMilliseconds))
            allocatedBytes = Get-Median ([double[]]@($runs.AllocatedBytes))
            peakWorkingSetBytes = [long](($runs | Measure-Object PeakWorkingSetBytes -Maximum).Maximum)
            peakLeasedBytes = $peakLease
            outputSignature = $signatures[0]
        }
    }

    $current = [ordered]@{
        schemaVersion = 1
        platform = $platform
        inputRecords = $InputRecords
        spillMemoryMb = $SpillMemoryMb
        repetitions = $Repetitions
        measurements = $measurements
    }
    if ($UpdateBaseline) {
        New-Item -ItemType Directory -Force -Path (Split-Path $baselinePath) | Out-Null
        $current | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $baselinePath -Encoding utf8
        Write-Host "Updated baseline: $baselinePath"
    } else {
        if (-not (Test-Path $baselinePath)) {
            throw "No baseline exists for $platform. Run once with -UpdateBaseline after reviewing the machine."
        }
        $baseline = Get-Content $baselinePath | ConvertFrom-Json
        if ($baseline.inputRecords -ne $InputRecords -or $baseline.spillMemoryMb -ne $SpillMemoryMb) {
            throw "Baseline input or memory configuration does not match this run."
        }
        $factor = 1.0 + $RegressionPercent / 100.0
        foreach ($operation in $operations) {
            $expected = $baseline.measurements.$operation
            $actual = $measurements[$operation]
            if ($actual.outputSignature -ne $expected.outputSignature) {
                throw "$operation output signature changed from $($expected.outputSignature) to $($actual.outputSignature)."
            }
            if ($actual.wallMilliseconds -gt $expected.wallMilliseconds * $factor) {
                throw "$operation median wall time regressed by more than $RegressionPercent percent."
            }
            if ($actual.allocatedBytes -gt $expected.allocatedBytes * $factor) {
                throw "$operation allocations regressed by more than $RegressionPercent percent."
            }
            if ($actual.peakWorkingSetBytes -gt $expected.peakWorkingSetBytes * $factor) {
                throw "$operation peak working set regressed by more than $RegressionPercent percent."
            }
        }
    }

    $report = [ordered]@{
        state = 'passed'
        completedUtc = [DateTime]::UtcNow.ToString('o')
        platform = $platform
        baselineUpdated = [bool]$UpdateBaseline
        gates = $results
        benchmarks = $current
    }
    $report | ConvertTo-Json -Depth 8 | Set-Content (Join-Path $output 'qualification-report.json') -Encoding utf8
    Write-Host "Production gates passed. Report: $(Join-Path $output 'qualification-report.json')"
}
catch {
    $_ | Out-String | Set-Content (Join-Path $output 'qualification-failure.txt') -Encoding utf8
    Write-Error $_
    exit 1
}
