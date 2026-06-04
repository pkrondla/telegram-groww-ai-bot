import subprocess
import sys
from pathlib import Path


def repo_root() -> Path:
    """Return repository root from the app package directory."""
    return Path(__file__).resolve().parent.parent


def runtime_dir() -> Path:
    """Return runtime directory path under repository root."""
    return repo_root() / "runtime"


def stop_flag_path() -> Path:
    return runtime_dir() / "bot.stop"


def restart_flag_path() -> Path:
    return runtime_dir() / "bot.restart"


def request_graceful_stop() -> None:
    """Signal the running bot process to shut down gracefully."""
    runtime_dir().mkdir(parents=True, exist_ok=True)
    stop_flag_path().touch()


def request_graceful_restart() -> None:
    """Gracefully stop the bot and spawn a fresh process after shutdown."""
    runtime_dir().mkdir(parents=True, exist_ok=True)
    restart_flag_path().touch()
    stop_flag_path().touch()


def restart_requested() -> bool:
    return restart_flag_path().exists()


def clear_restart_flag() -> None:
    try:
        restart_flag_path().unlink(missing_ok=True)
    except OSError:
        pass


def spawn_bot_process() -> int | None:
    """Start a detached ``python -m app.main`` child and record its PID."""
    runtime_dir().mkdir(parents=True, exist_ok=True)
    repo = repo_root()
    cmd = [sys.executable, "-m", "app.main"]
    kwargs: dict = {
        "cwd": str(repo),
        "stdin": subprocess.DEVNULL,
    }
    log_file = runtime_dir() / "bot.log"
    log_handle = None
    try:
        log_handle = open(log_file, "a", encoding="utf-8")
        kwargs["stdout"] = log_handle
        kwargs["stderr"] = subprocess.STDOUT
    except OSError:
        kwargs["stdout"] = subprocess.DEVNULL
        kwargs["stderr"] = subprocess.DEVNULL

    if sys.platform == "win32":
        flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
        create_no_window = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        if create_no_window:
            flags |= create_no_window
        kwargs["creationflags"] = flags
    else:
        kwargs["start_new_session"] = True

    try:
        proc = subprocess.Popen(cmd, **kwargs)
    except OSError:
        if log_handle is not None:
            log_handle.close()
        return None
    if log_handle is not None:
        log_handle.close()
    (runtime_dir() / "bot.pid").write_text(str(proc.pid), encoding="ascii")
    return proc.pid
