[CmdletBinding()]
param(
    [ValidateRange(10000, 1000000000)]
    [int]$InputRecords = 10000000,

    [ValidateRange(1, 10)]
    [int]$Repetitions = 3,

    [ValidateRange(1, 65536)]
    [int]$SpillMemoryMb = 64,

    [ValidateRange(0, 90)]
    [double]$RequiredRamReductionPercent = 10,

    [ValidateSet('distinct', 'orderby', 'exceptby', 'groupjoin')]
    [string[]]$Operations = @('distinct', 'orderby', 'exceptby', 'groupjoin'),

    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$project = Join-Path $projectRoot 'benchmarks/SpillFlow.ResourceBenchmarks/SpillFlow.ResourceBenchmarks.csproj'
$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$output = Join-Path $projectRoot "artifacts/external-memory-comparison/$timestamp"
New-Item -ItemType Directory -Force -Path $output | Out-Null

function Get-Median([double[]]$Values) {
    $ordered = $Values | Sort-Object
    $middle = [int][Math]::Floor($ordered.Count / 2)
    if ($ordered.Count % 2) { return [double]$ordered[$middle] }
    return ([double]$ordered[$middle - 1] + [double]$ordered[$middle]) / 2.0
}

function Invoke-Measurement([string]$Implementation, [string]$Operation) {
    $lines = @(& dotnet run -c Release --no-build --project $project -- `
        $Implementation $Operation $InputRecords $SpillMemoryMb 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "$Implementation $Operation failed.`n$($lines -join "`n")"
    }
    $json = $lines | Where-Object { $_ -match '^\s*\{' } | Select-Object -Last 1
    if (-not $json) { throw "$Implementation $Operation produced no JSON result." }
    return $json | ConvertFrom-Json
}

if (-not $SkipBuild) {
    & dotnet build $project -c Release -p:NoWarn=NU1900
    if ($LASTEXITCODE -ne 0) { throw 'Benchmark build failed.' }
}

& dotnet build-server shutdown | Out-Null
Start-Sleep -Seconds 2
$results = [ordered]@{}
$requiredFactor = 1.0 - ($RequiredRamReductionPercent / 100.0)

foreach ($operation in $Operations) {
    $spillRuns = @()
    $externalRuns = @()
    for ($run = 1; $run -le $Repetitions; $run++) {
        $spillRuns += Invoke-Measurement 'spill' $operation
        $externalRuns += Invoke-Measurement 'external' $operation
    }

    $spillSignatures = @($spillRuns | ForEach-Object { "$($_.OutputRecords):$($_.Checksum)" } | Select-Object -Unique)
    $externalSignatures = @($externalRuns | ForEach-Object { "$($_.OutputRecords):$($_.Checksum)" } | Select-Object -Unique)
    if ($spillSignatures.Count -ne 1 -or $externalSignatures.Count -ne 1 -or
        $spillSignatures[0] -ne $externalSignatures[0]) {
        throw "$operation produced different output signatures."
    }

    $spillWorkingSet = Get-Median ([double[]]@($spillRuns.PeakWorkingSetBytes))
    $externalWorkingSet = Get-Median ([double[]]@($externalRuns.PeakWorkingSetBytes))
    $spillPrivate = Get-Median ([double[]]@($spillRuns.PeakPrivateBytes))
    $externalPrivate = Get-Median ([double[]]@($externalRuns.PeakPrivateBytes))
    $passed = $spillWorkingSet -le $externalWorkingSet * $requiredFactor -and
        $spillPrivate -le $externalPrivate * $requiredFactor

    $results[$operation] = [ordered]@{
        passed = $passed
        outputSignature = $spillSignatures[0]
        spillMedianPeakWorkingSetBytes = $spillWorkingSet
        externalMedianPeakWorkingSetBytes = $externalWorkingSet
        workingSetReductionPercent = (1.0 - $spillWorkingSet / $externalWorkingSet) * 100.0
        spillMedianPeakPrivateBytes = $spillPrivate
        externalMedianPeakPrivateBytes = $externalPrivate
        privateBytesReductionPercent = (1.0 - $spillPrivate / $externalPrivate) * 100.0
        spillMedianAllocatedBytes = Get-Median ([double[]]@($spillRuns.AllocatedBytes))
        externalMedianAllocatedBytes = Get-Median ([double[]]@($externalRuns.AllocatedBytes))
        spillRuns = $spillRuns
        externalRuns = $externalRuns
    }
    if (-not $passed) {
        throw "$operation did not achieve the required $RequiredRamReductionPercent percent RAM reduction."
    }
}

$report = [ordered]@{
    state = 'passed'
    completedUtc = [DateTime]::UtcNow.ToString('o')
    inputRecords = $InputRecords
    repetitions = $Repetitions
    spillMemoryMb = $SpillMemoryMb
    requiredRamReductionPercent = $RequiredRamReductionPercent
    results = $results
}
$reportPath = Join-Path $output 'comparison-report.json'
$report | ConvertTo-Json -Depth 10 | Set-Content $reportPath -Encoding utf8
Write-Host "External memory comparison passed. Report: $reportPath"
