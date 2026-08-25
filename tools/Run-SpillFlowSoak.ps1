[CmdletBinding()]
param(
    [ValidateRange(1, 10080)]
    [int]$DurationMinutes = 360,

    [ValidateRange(1, 1000000000)]
    [int]$InputRecords = 10000000,

    [ValidateRange(1, 65536)]
    [int]$SpillMemoryMb = 64,

    [ValidateRange(0, 1000000)]
    [int]$MaxIterations = 0,

    [ValidateNotNullOrEmpty()]
    [string[]]$Operations = @('distinct', 'orderby', 'exceptby', 'groupjoin', 'aggregate'),

    [string]$OutputRoot = 'artifacts/soak'
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$benchmarkProject = Join-Path $projectRoot 'benchmarks/SpillFlow.ResourceBenchmarks/SpillFlow.ResourceBenchmarks.csproj'
$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$outputDirectory = Join-Path $projectRoot (Join-Path $OutputRoot $timestamp)
$rawPath = Join-Path $outputDirectory 'measurements.ndjson'
$csvPath = Join-Path $outputDirectory 'measurements.csv'
$summaryPath = Join-Path $outputDirectory 'summary.json'
$statusPath = Join-Path $outputDirectory 'status.json'
$failurePath = Join-Path $outputDirectory 'failure.txt'
$stopPath = Join-Path $outputDirectory 'STOP'

$supportedOperations = @('distinct', 'orderby', 'exceptby', 'groupjoin', 'aggregate')
foreach ($operation in $Operations) {
    if ($operation -notin $supportedOperations) {
        throw "Unsupported soak operation '$operation'. Supported values: $($supportedOperations -join ', ')."
    }
}

New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null

function Get-Percentile([double[]]$Values, [double]$Percentile) {
    if ($Values.Count -eq 0) { return 0 }
    $sorted = $Values | Sort-Object
    $index = [Math]::Ceiling(($Percentile / 100.0) * $sorted.Count) - 1
    return [double]$sorted[[Math]::Max(0, [Math]::Min($index, $sorted.Count - 1))]
}

function Write-Summary([System.Collections.Generic.List[object]]$Measurements, [string]$State) {
    $operationSummaries = @()
    foreach ($group in ($Measurements | Group-Object Operation)) {
        $wall = [double[]]@($group.Group | ForEach-Object { $_.WallMilliseconds })
        $throughput = [double[]]@($group.Group | ForEach-Object {
            if ($_.WallMilliseconds -le 0) { 0 } else { $_.InputRecords / ($_.WallMilliseconds / 1000.0) }
        })
        $operationSummaries += [ordered]@{
            operation = $group.Name
            runs = $group.Count
            wallMillisecondsP50 = Get-Percentile $wall 50
            wallMillisecondsP95 = Get-Percentile $wall 95
            throughputRecordsPerSecondP50 = Get-Percentile $throughput 50
            throughputRecordsPerSecondP05 = Get-Percentile $throughput 5
            peakWorkingSetBytes = [long](($group.Group | Measure-Object PeakWorkingSetBytes -Maximum).Maximum)
            peakPrivateBytes = [long](($group.Group | Measure-Object PeakPrivateBytes -Maximum).Maximum)
            peakSpillDiskBytes = [long](($group.Group | Measure-Object PeakSpillDiskBytes -Maximum).Maximum)
            allocatedBytesPerRunAverage = [long](($group.Group | Measure-Object AllocatedBytes -Average).Average)
            gen2Collections = [long](($group.Group | Measure-Object Gen2Collections -Sum).Sum)
        }
    }

    $summary = [ordered]@{
        state = $State
        startedUtc = $script:startedUtc.ToString('o')
        updatedUtc = [DateTime]::UtcNow.ToString('o')
        requestedDurationMinutes = $DurationMinutes
        elapsedMinutes = ([DateTime]::UtcNow - $script:startedUtc).TotalMinutes
        inputRecordsPerRun = $InputRecords
        spillMemoryMb = $SpillMemoryMb
        completedRuns = $Measurements.Count
        machine = [ordered]@{
            os = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
            framework = [System.Runtime.InteropServices.RuntimeInformation]::FrameworkDescription
            processorCount = [Environment]::ProcessorCount
            machineName = [Environment]::MachineName
        }
        operations = $operationSummaries
        rawMeasurements = $rawPath
        csvMeasurements = $csvPath
    }
    $summary | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $summaryPath -Encoding utf8
    $summary | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $statusPath -Encoding utf8
}

try {
    Write-Host "Building resource benchmark in Release mode..."
    & dotnet build $benchmarkProject -c Release --no-restore --nologo
    if ($LASTEXITCODE -ne 0) { throw "Benchmark build failed with exit code $LASTEXITCODE." }

    # Limit the managed heap to 1 GiB. The SpillFlow session itself remains at SpillMemoryMb.
    $env:DOTNET_GCHeapHardLimit = '0x40000000'
    $script:startedUtc = [DateTime]::UtcNow
    $deadline = $script:startedUtc.AddMinutes($DurationMinutes)
    $measurements = [System.Collections.Generic.List[object]]::new()
    $baselines = @{}
    $iteration = 0
    Write-Summary $measurements 'running'

    Write-Host "Soak output: $outputDirectory"
    Write-Host "Stop gracefully by creating: $stopPath"

    while ([DateTime]::UtcNow -lt $deadline -and ($MaxIterations -eq 0 -or $iteration -lt $MaxIterations)) {
        foreach ($operation in $Operations) {
            if ([DateTime]::UtcNow -ge $deadline -or (Test-Path -LiteralPath $stopPath)) { break }

            $runStartedUtc = [DateTime]::UtcNow
            $output = @(& dotnet run -c Release --no-build --project $benchmarkProject -- spill $operation $InputRecords $SpillMemoryMb 2>&1)
            if ($LASTEXITCODE -ne 0) {
                throw "Operation '$operation' failed with exit code $LASTEXITCODE.`n$($output -join [Environment]::NewLine)"
            }
            $jsonLine = $output | Where-Object { $_ -match '^\s*\{' } | Select-Object -Last 1
            if (-not $jsonLine) { throw "Operation '$operation' did not produce a JSON measurement." }
            $measurement = $jsonLine | ConvertFrom-Json
            $measurement | Add-Member -NotePropertyName RunStartedUtc -NotePropertyValue $runStartedUtc.ToString('o')
            $measurement | Add-Member -NotePropertyName Iteration -NotePropertyValue ($iteration + 1)

            $signature = "$($measurement.OutputRecords):$($measurement.Checksum)"
            if ($baselines.ContainsKey($operation) -and $baselines[$operation] -ne $signature) {
                throw "Correctness drift for '$operation'. Expected $($baselines[$operation]); received $signature."
            }
            $baselines[$operation] = $signature
            $measurements.Add($measurement)
            ($measurement | ConvertTo-Json -Compress) | Add-Content -LiteralPath $rawPath -Encoding utf8
            $measurements | Export-Csv -LiteralPath $csvPath -NoTypeInformation -Encoding utf8
            Write-Summary $measurements 'running'

            $rate = if ($measurement.WallMilliseconds -gt 0) {
                [Math]::Round($measurement.InputRecords / ($measurement.WallMilliseconds / 1000.0))
            } else { 0 }
            Write-Host ("{0:u} {1,-10} {2,8:N1}s {3,12:N0} records/s WS {4,7:N1} MiB" -f `
                [DateTime]::UtcNow, $operation, ($measurement.WallMilliseconds / 1000.0), $rate,
                ($measurement.PeakWorkingSetBytes / 1MB))
        }
        $iteration++
        if (Test-Path -LiteralPath $stopPath) { break }
    }

    $finalState = if (Test-Path -LiteralPath $stopPath) { 'stopped' } else { 'completed' }
    Write-Summary $measurements $finalState
    Write-Host "Soak test $finalState. Summary: $summaryPath"
}
catch {
    $_ | Out-String | Set-Content -LiteralPath $failurePath -Encoding utf8
    if ($null -ne $measurements) { Write-Summary $measurements 'failed' }
    Write-Error "Soak test failed. Details: $failurePath`n$_"
    exit 1
}
