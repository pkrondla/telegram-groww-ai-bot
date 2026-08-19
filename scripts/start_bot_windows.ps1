$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$RuntimeDir = Join-Path $RepoRoot "runtime"
$PidFile = Join-Path $RuntimeDir "bot.pid"
$StopFile = Join-Path $RuntimeDir "bot.stop"
$LogFile = Join-Path $RuntimeDir "bot.log"
$ErrLogFile = Join-Path $RuntimeDir "bot.err.log"
$StartLogFile = Join-Path $RuntimeDir "start-task.log"

function Write-StartLog {
    param([string]$Message)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $StartLogFile -Value "[$ts] $Message"
}

function Get-BotPythonProcesses {
    Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object {
            $_.Name -match '^python(w)?\.exe$' -and
            $_.CommandLine -and
            $_.CommandLine -match 'app\.main'
        }
}

try {
    if (!(Test-Path $RuntimeDir)) {
        New-Item -ItemType Directory -Path $RuntimeDir | Out-Null
    }
    Write-StartLog "Start task invoked. user=$env:USERNAME machine=$env:COMPUTERNAME repo=$RepoRoot"

    if (Test-Path $StopFile) {
        Remove-Item $StopFile -Force
        Write-StartLog "Removed stale stop flag: $StopFile"
    }

    $running = @(Get-BotPythonProcesses)
    if ($running.Count -gt 0) {
        $pids = ($running | ForEach-Object { $_.ProcessId }) -join ", "
        Write-StartLog "Bot already running with PID(s) $pids (app.main). Refusing second start."
        Write-Output "Bot is already running with PID(s) $pids. Stop it first with scripts/stop_bot_windows.ps1."
        exit 0
    }

    if (Test-Path $PidFile) {
        $existingPid = (Get-Content $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1).Trim()
        if ($existingPid) {
            $existingProc = Get-Process -Id $existingPid -ErrorAction SilentlyContinue
            if ($existingProc) {
                Write-StartLog "Bot already running with PID $existingPid."
                Write-Output "Bot is already running with PID $existingPid."
                exit 0
            }
        }
        Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
        Write-StartLog "Removed stale PID file."
    }

    $AppMainPy = Join-Path $RepoRoot "app\main.py"
    if (!(Test-Path $AppMainPy)) {
        throw "app/main.py not found at $AppMainPy"
    }

    $VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
    $PythonExe = $null
    if (Test-Path $VenvPython) {
        $PythonExe = $VenvPython
    } else {
        $pyLauncher = Get-Command py.exe -ErrorAction SilentlyContinue
        if ($pyLauncher) {
            $PythonExe = $pyLauncher.Source
        } else {
            $pythonCmd = Get-Command python.exe -ErrorAction SilentlyContinue
            if ($pythonCmd) {
                $PythonExe = $pythonCmd.Source
            }
        }
    }

    if (!$PythonExe) {
        throw "Python executable not found. Install Python or create .venv before starting the bot."
    }
    Write-StartLog "Using Python executable: $PythonExe"

    $argList = if ([System.IO.Path]::GetFileName($PythonExe).ToLower() -eq "py.exe") {
        @("-3", "-m", "app.main")
    } else {
        @("-m", "app.main")
    }

    $proc = Start-Process `
        -FilePath $PythonExe `
        -ArgumentList $argList `
        -WorkingDirectory $RepoRoot `
        -PassThru `
        -WindowStyle Hidden `
        -RedirectStandardOutput $LogFile `
        -RedirectStandardError $ErrLogFile

    Set-Content -Path $PidFile -Value $proc.Id
    Write-StartLog "Bot started with PID=$($proc.Id). out=$LogFile err=$ErrLogFile"
    Write-Output "Bot started. PID=$($proc.Id) | out=$LogFile | err=$ErrLogFile | startlog=$StartLogFile"
} catch {
    $errText = $_ | Out-String
    try {
        Write-StartLog "Start task failed: $errText"
    } catch {}
    Write-Error $errText
    exit 1
}
