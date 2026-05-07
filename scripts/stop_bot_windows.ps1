$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$RuntimeDir = Join-Path $RepoRoot "runtime"
$PidFile = Join-Path $RuntimeDir "bot.pid"
$StopFile = Join-Path $RuntimeDir "bot.stop"

if (!(Test-Path $RuntimeDir)) {
    New-Item -ItemType Directory -Path $RuntimeDir | Out-Null
}

New-Item -Path $StopFile -ItemType File -Force | Out-Null

if (!(Test-Path $PidFile)) {
    Write-Output "No PID file found. Stop signal file created."
    exit 0
}

$pidValue = (Get-Content $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1).Trim()
if (!$pidValue) {
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
    Write-Output "PID file was empty. Stop signal file created."
    exit 0
}

$proc = Get-Process -Id $pidValue -ErrorAction SilentlyContinue
if (!$proc) {
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
    Write-Output "Process $pidValue is not running."
    exit 0
}

$timeoutSec = 45
$elapsed = 0
while ($elapsed -lt $timeoutSec) {
    Start-Sleep -Seconds 1
    $elapsed += 1
    $proc = Get-Process -Id $pidValue -ErrorAction SilentlyContinue
    if (!$proc) {
        Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
        Write-Output "Bot stopped gracefully."
        exit 0
    }
}

Write-Output "Graceful stop timed out. Forcing stop for PID $pidValue."
Stop-Process -Id $pidValue -Force -ErrorAction SilentlyContinue
Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
