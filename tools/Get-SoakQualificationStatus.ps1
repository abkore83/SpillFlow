[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$artifacts = Join-Path $projectRoot 'artifacts'

function Show-LatestRun([string]$Name, [string]$Directory) {
    Write-Output "`n$Name"
    $root = Join-Path $artifacts $Directory
    $run = Get-ChildItem -LiteralPath $root -Directory -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if (-not $run) {
        Write-Output '  No run found.'
        return
    }

    $statusPath = Join-Path $run.FullName 'status.json'
    $status = Get-Content -LiteralPath $statusPath -Raw | ConvertFrom-Json
    Write-Output "  Run: $($run.FullName)"
    Write-Output "  State: $($status.state)"
    Write-Output "  Completed measurements: $($status.completedRuns)"
    if ($null -ne $status.requestedDurationMinutes) {
        Write-Output "  Requested duration: $($status.requestedDurationMinutes) minutes"
        Write-Output ('  Elapsed duration: {0:N2} minutes' -f [double]$status.elapsedMinutes)
        Write-Output "  Six-hour qualification: $(if ([double]$status.requestedDurationMinutes -ge 360 -and $status.state -eq 'completed' -and [double]$status.elapsedMinutes -ge 360) { 'PASS' } else { 'not proven by this run' })"
    }
    elseif ($null -ne $status.startedUtc) {
        $started = [DateTime]::Parse($status.startedUtc).ToUniversalTime()
        $ended = if ($null -ne $status.completedUtc) {
            [DateTime]::Parse($status.completedUtc).ToUniversalTime()
        } else {
            [DateTime]::UtcNow
        }
        Write-Output ('  Elapsed duration: {0:N2} minutes' -f ($ended - $started).TotalMinutes)
        Write-Output "  Six-hour qualification: $(if ($status.state -eq 'completed' -and ($ended - $started).TotalHours -ge 6) { 'PASS' } else { 'not proven by this run' })"
    }

    $measurementPath = Join-Path $run.FullName 'measurements.ndjson'
    if (-not (Test-Path -LiteralPath $measurementPath)) { return }
    $measurements = @(Get-Content -LiteralPath $measurementPath | ForEach-Object { $_ | ConvertFrom-Json })
    $measurements |
        Group-Object Operation |
        ForEach-Object {
            $signatures = @($_.Group |
                ForEach-Object { "$($_.OutputRecords):$($_.Checksum)" } |
                Sort-Object -Unique)
            [pscustomobject]@{
                Operation = $_.Name
                Runs = $_.Count
                Drift = if ($signatures.Count -eq 1) { 'no' } else { 'YES' }
                PeakLeasedMB = [Math]::Round((($_.Group | Measure-Object PeakLeasedBytes -Maximum).Maximum / 1MB), 2)
                PeakWorkingSetMB = [Math]::Round((($_.Group | Measure-Object PeakWorkingSetBytes -Maximum).Maximum / 1MB), 2)
            }
        } |
        Sort-Object Operation |
        Format-Table -AutoSize
}

try {
    $container = docker inspect spillflow-soak-linux-6h --format '{{.State.Status}}' 2>$null
    if ($LASTEXITCODE -eq 0) { Write-Output "Linux container: $container" }
}
catch {
    Write-Output 'Linux container: unavailable'
}

Show-LatestRun 'Linux soak' 'soak-linux'
Show-LatestRun 'Windows soak' 'soak'

$chainLog = Join-Path $artifacts 'soak-qualification-chain.log'
if (Test-Path -LiteralPath $chainLog) {
    Write-Output "`nQualification chain"
    Get-Content -LiteralPath $chainLog -Tail 5 | ForEach-Object { Write-Output "  $_" }
}
