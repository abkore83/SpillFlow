[CmdletBinding()]
param(
    [string]$LinuxContainerName = 'spillflow-soak-linux-6h',
    [int]$WindowsDurationMinutes = 360,
    [int]$InputRecords = 10000000,
    [int]$SpillMemoryMb = 64
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$artifacts = Join-Path $projectRoot 'artifacts'
$log = Join-Path $artifacts 'soak-qualification-chain.log'

function Write-ChainLog([string]$Message) {
    Add-Content -LiteralPath $log -Value ('{0} {1}' -f [DateTime]::UtcNow.ToString('o'), $Message)
}

Write-ChainLog "Waiting for Linux container $LinuxContainerName."
while ($true) {
    $state = docker inspect $LinuxContainerName --format '{{.State.Status}}' 2>$null
    if ($LASTEXITCODE -ne 0) { throw "Linux container '$LinuxContainerName' was not found." }
    if ($state -ne 'running') { break }
    Start-Sleep -Seconds 60
}

$exitCode = docker inspect $LinuxContainerName --format '{{.State.ExitCode}}'
$latestLinuxRun = Get-ChildItem -LiteralPath (Join-Path $artifacts 'soak-linux') -Directory |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1
$linuxStatus = Get-Content -LiteralPath (Join-Path $latestLinuxRun.FullName 'status.json') -Raw | ConvertFrom-Json
if ([int]$exitCode -ne 0 -or $linuxStatus.state -ne 'completed') {
    throw "Linux qualification did not pass. Container exit: $exitCode; status: $($linuxStatus.state)."
}

Write-ChainLog "Linux qualification passed with $($linuxStatus.completedRuns) measurements. Starting Windows qualification."
& (Join-Path $PSScriptRoot 'Run-SpillFlowSoak.ps1') `
    -DurationMinutes $WindowsDurationMinutes `
    -InputRecords $InputRecords `
    -SpillMemoryMb $SpillMemoryMb
if ($LASTEXITCODE -ne 0) { throw "Windows qualification failed with exit code $LASTEXITCODE." }
Write-ChainLog 'Windows qualification completed successfully.'
