[CmdletBinding()]
param(
    [ValidateRange(60, 604800)]
    [int]$DurationSeconds = 21600,

    [ValidateRange(1, 1000000000)]
    [int]$InputRecords = 10000000,

    [ValidateRange(1, 65536)]
    [int]$SpillMemoryMb = 64,

    [ValidateRange(1, 86400)]
    [int]$MaxOperationSeconds = 900,

    [string]$Image = 'spillflow-soak:local',

    [string]$ContainerName = 'spillflow-soak-linux-6h'
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$artifacts = Join-Path $projectRoot 'artifacts'
$launcherLog = Join-Path $artifacts 'linux-soak-launcher.log'
New-Item -ItemType Directory -Force -Path $artifacts | Out-Null

Add-Type @'
using System.Runtime.InteropServices;
public static class SpillFlowLinuxSoakPowerRequest
{
    [DllImport("kernel32.dll")]
    public static extern uint SetThreadExecutionState(uint flags);
}
'@

function Write-LauncherLog([string]$Message) {
    $line = '{0} {1}' -f [DateTime]::UtcNow.ToString('o'), $Message
    Add-Content -LiteralPath $launcherLog -Value $line
    Write-Output $line
}

$sleepBlockEnabled = $false
try {
    $sleepBlockEnabled = [SpillFlowLinuxSoakPowerRequest]::SetThreadExecutionState(2147483649) -ne 0
    if (-not $sleepBlockEnabled) { throw 'Windows refused the system-sleep prevention request.' }

    $existing = docker ps -a --filter "name=^/$ContainerName$" --format '{{.Names}}'
    if ($existing) { throw "Docker container '$ContainerName' already exists." }

    Write-LauncherLog "Starting $ContainerName for $DurationSeconds seconds."
    $containerId = docker run --detach --name $ContainerName `
        --env "DURATION_SECONDS=$DurationSeconds" `
        --env "INPUT_RECORDS=$InputRecords" `
        --env "SPILL_MEMORY_MB=$SpillMemoryMb" `
        --env "MAX_OPERATION_SECONDS=$MaxOperationSeconds" `
        --volume "${artifacts}:/src/artifacts" `
        $Image
    if ($LASTEXITCODE -ne 0 -or -not $containerId) { throw 'Docker failed to start the Linux soak container.' }

    Write-LauncherLog "Container started: $containerId"
    $exitCode = docker wait $ContainerName
    Write-LauncherLog "Container exited with code $exitCode."
    docker logs $ContainerName 2>&1 | Add-Content -LiteralPath $launcherLog
    if ([int]$exitCode -ne 0) { throw "Linux soak failed with exit code $exitCode. See $launcherLog" }
}
finally {
    if ($sleepBlockEnabled) {
        [void][SpillFlowLinuxSoakPowerRequest]::SetThreadExecutionState(2147483648)
    }
}
