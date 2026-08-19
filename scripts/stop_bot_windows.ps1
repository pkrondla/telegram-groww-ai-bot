$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$RuntimeDir = Join-Path $RepoRoot "runtime"
$PidFile = Join-Path $RuntimeDir "bot.pid"
$StopFile = Join-Path $RuntimeDir "bot.stop"

function Get-BotPythonProcesses {
    Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object {
            $_.Name -match '^python(w)?\.exe$' -and
            $_.CommandLine -and
            $_.CommandLine -match 'app\.main'
        }
}

if (!(Test-Path $RuntimeDir)) {
    New-Item -ItemType Directory -Path $RuntimeDir | Out-Null
}

New-Item -Path $StopFile -ItemType File -Force | Out-Null

$targetPids = New-Object System.Collections.Generic.HashSet[int]
if (Test-Path $PidFile) {
    $pidValue = (Get-Content $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
    if ($pidValue) {
        $pidValue = $pidValue.Trim()
        if ($pidValue) {
            [void]$targetPids.Add([int]$pidValue)
        }
    }
}
foreach ($procInfo in @(Get-BotPythonProcesses)) {
    [void]$targetPids.Add([int]$procInfo.ProcessId)
}

if ($targetPids.Count -eq 0) {
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
    Write-Output "No bot process found. Stop signal file created."
    exit 0
}

Write-Output ("Signaled stop for PID(s) {0}." -f (($targetPids | Sort-Object) -join ", "))

$timeoutSec = 45
$elapsed = 0
while ($elapsed -lt $timeoutSec) {
    $alive = @()
    foreach ($candidate in $targetPids) {
        if (Get-Process -Id $candidate -ErrorAction SilentlyContinue) {
            $alive += $candidate
        }
    }
    if ($alive.Count -eq 0) {
        Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
        Write-Output "Bot stopped gracefully."
        exit 0
    }
    Start-Sleep -Seconds 1
    $elapsed += 1
}

$stillAlive = @()
foreach ($candidate in $targetPids) {
    if (Get-Process -Id $candidate -ErrorAction SilentlyContinue) {
        $stillAlive += $candidate
    }
}
if ($stillAlive.Count -gt 0) {
    Write-Output ("Graceful stop timed out. Forcing stop for PID(s) {0}." -f ($stillAlive -join ", "))
    foreach ($candidate in $stillAlive) {
        Stop-Process -Id $candidate -Force -ErrorAction SilentlyContinue
    }
}

# Catch a crash-loop child that appeared while we were waiting.
foreach ($procInfo in @(Get-BotPythonProcesses)) {
    Write-Output "Forcing leftover bot process PID $($procInfo.ProcessId)."
    Stop-Process -Id $procInfo.ProcessId -Force -ErrorAction SilentlyContinue
}

Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
Write-Output "Bot stop completed."
