[CmdletBinding(SupportsShouldProcess)]
param(
    [switch]$Apply,
    [int]$DurationMinutesPerPlatform = 360,
    [int]$InputRecords = 10000000,
    [int]$SpillMemoryMb = 64
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$artifacts = Join-Path $projectRoot 'artifacts'
$powerStatePath = Join-Path $artifacts 'qualification-power-state.json'
$logPath = Join-Path $artifacts 'protected-qualification.log'
New-Item -ItemType Directory -Force -Path $artifacts | Out-Null

function Get-SleepTimeouts {
    $text = powercfg /query SCHEME_CURRENT SUB_SLEEP STANDBYIDLE | Out-String
    $acMatch = [regex]::Match($text, 'Current AC Power Setting Index:\s+0x([0-9a-fA-F]+)')
    $dcMatch = [regex]::Match($text, 'Current DC Power Setting Index:\s+0x([0-9a-fA-F]+)')
    if (-not $acMatch.Success -or -not $dcMatch.Success) {
        throw 'Could not read the current Windows sleep timeouts.'
    }
    [ordered]@{
        acSeconds = [Convert]::ToInt32($acMatch.Groups[1].Value, 16)
        dcSeconds = [Convert]::ToInt32($dcMatch.Groups[1].Value, 16)
    }
}

function Write-QualificationLog([string]$Message) {
    $line = '{0} {1}' -f [DateTime]::UtcNow.ToString('o'), $Message
    Add-Content -LiteralPath $logPath -Value $line
    Write-Output $line
}

function Set-SleepTimeouts([int]$AcMinutes, [int]$DcMinutes) {
    powercfg /change standby-timeout-ac $AcMinutes
    if ($LASTEXITCODE -ne 0) { throw 'Could not change the AC sleep timeout.' }
    powercfg /change standby-timeout-dc $DcMinutes
    if ($LASTEXITCODE -ne 0) { throw 'Could not change the battery sleep timeout.' }
}

$original = Get-SleepTimeouts
Write-Output "Current AC sleep timeout: $($original.acSeconds) seconds"
Write-Output "Current battery sleep timeout: $($original.dcSeconds) seconds"
if (-not $Apply) {
    Write-Output 'Dry run only. Pass -Apply after explicitly approving the temporary power-setting change.'
    return
}

if (($original.acSeconds % 60) -ne 0 -or ($original.dcSeconds % 60) -ne 0) {
    throw 'The current timeouts are not whole minutes, so automatic restoration would be lossy.'
}

$state = [ordered]@{
    capturedUtc = [DateTime]::UtcNow.ToString('o')
    acSeconds = $original.acSeconds
    dcSeconds = $original.dcSeconds
    restored = $false
}
$state | ConvertTo-Json | Set-Content -LiteralPath $powerStatePath -Encoding utf8

try {
    if (-not $PSCmdlet.ShouldProcess('Current Windows power scheme', 'Temporarily disable AC and battery sleep')) {
        return
    }
    Set-SleepTimeouts 0 0
    Write-QualificationLog 'Windows sleep disabled temporarily.'

    $existing = docker ps -a --filter 'name=^/spillflow-soak-linux-6h$' --format '{{.Names}}'
    if ($existing) {
        $preservedName = 'spillflow-soak-linux-preserved-{0}' -f (Get-Date -Format 'yyyyMMdd-HHmmss')
        docker rename spillflow-soak-linux-6h $preservedName
        if ($LASTEXITCODE -ne 0) { throw 'Could not preserve the existing Linux soak container.' }
        Write-QualificationLog "Existing container preserved as $preservedName."
    }

    & (Join-Path $PSScriptRoot 'Start-LinuxSoak.ps1') `
        -DurationSeconds ($DurationMinutesPerPlatform * 60) `
        -InputRecords $InputRecords `
        -SpillMemoryMb $SpillMemoryMb
    if ($LASTEXITCODE -ne 0) { throw 'Linux qualification failed.' }

    Write-QualificationLog 'Linux qualification passed. Starting Windows qualification.'
    & (Join-Path $PSScriptRoot 'Run-SpillFlowSoak.ps1') `
        -DurationMinutes $DurationMinutesPerPlatform `
        -InputRecords $InputRecords `
        -SpillMemoryMb $SpillMemoryMb
    if ($LASTEXITCODE -ne 0) { throw 'Windows qualification failed.' }
    Write-QualificationLog 'Windows qualification passed.'
}
finally {
    Set-SleepTimeouts ($original.acSeconds / 60) ($original.dcSeconds / 60)
    $state.restored = $true
    $state.restoredUtc = [DateTime]::UtcNow.ToString('o')
    $state | ConvertTo-Json | Set-Content -LiteralPath $powerStatePath -Encoding utf8
    Write-QualificationLog 'Original Windows sleep timeouts restored.'
}
