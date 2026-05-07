$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$RuntimeDir = Join-Path $RepoRoot "runtime"
$PidFile = Join-Path $RuntimeDir "bot.pid"
$StopFile = Join-Path $RuntimeDir "bot.stop"

if (!(Test-Path $RuntimeDir)) {
    New-Item -ItemType Directory -Path $RuntimeDir | Out-Null
}

if (Test-Path $StopFile) {
    Remove-Item $StopFile -Force
}

if (Test-Path $PidFile) {
    $existingPid = (Get-Content $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1).Trim()
    if ($existingPid) {
        $existingProc = Get-Process -Id $existingPid -ErrorAction SilentlyContinue
        if ($existingProc) {
            Write-Output "Bot is already running with PID $existingPid."
            exit 0
        }
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
}

$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$PythonCmd = if (Test-Path $VenvPython) { $VenvPython } else { "python" }
$escapedPython = '"' + $PythonCmd + '"'
$cmdLine = "$escapedPython main.py"

$proc = Start-Process `
    -FilePath "cmd.exe" `
    -ArgumentList "/c", $cmdLine `
    -WorkingDirectory $RepoRoot `
    -PassThru

Set-Content -Path $PidFile -Value $proc.Id
Write-Output "Bot started in visible command window. PID=$($proc.Id)"
