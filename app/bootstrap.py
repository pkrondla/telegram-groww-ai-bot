from pathlib import Path


def repo_root() -> Path:
    """Return repository root from the app package directory."""
    return Path(__file__).resolve().parent.parent


def runtime_dir() -> Path:
    """Return runtime directory path under repository root."""
    return repo_root() / "runtime"
