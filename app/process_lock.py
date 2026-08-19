"""Exclusive single-instance lock for the trading bot process.

Prevents two Python processes from opening the same Telethon ``.session``
SQLite file and the same ``trading_bot.db``. A stale ``runtime/bot.pid`` is
not enough: Windows can leave an old instance running after a deploy while
the supervisor starts a new one.

The lock is held for the lifetime of the process (the open file handle is
kept in ``_HELD_LOCK``). The OS releases it on crash or kill.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from app.bootstrap import runtime_dir
from src.core.logger import logger

_HELD_LOCK: "ProcessLock | None" = None


def lock_file_path() -> Path:
    return runtime_dir() / "bot.lock"


class ProcessLock:
    """Advisory exclusive lock on a small lockfile (Windows + POSIX)."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._fh = None

    def acquire(self, timeout_sec: float = 120.0) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a+b")
        deadline = time.monotonic() + max(0.0, float(timeout_sec))
        warned = False
        while True:
            try:
                self._try_lock()
                self._write_pid()
                return True
            except OSError:
                if time.monotonic() >= deadline:
                    return False
                if not warned:
                    logger.warning(
                        "Another bot instance still holds %s. Waiting up to %.0fs "
                        "for it to exit (do not start a second copy of the bot).",
                        self.path,
                        timeout_sec,
                    )
                    warned = True
                time.sleep(1.0)

    def release(self) -> None:
        fh = self._fh
        self._fh = None
        if fh is None:
            return
        try:
            self._unlock(fh)
        except OSError:
            pass
        try:
            fh.close()
        except OSError:
            pass

    def _write_pid(self) -> None:
        assert self._fh is not None
        try:
            # Keep the locked first byte intact on Windows; write PID after it.
            self._fh.seek(1)
            self._fh.write(f"{os.getpid()}\n".encode("ascii"))
            self._fh.flush()
        except OSError:
            pass

    def _try_lock(self) -> None:
        assert self._fh is not None
        if sys.platform == "win32":
            import msvcrt

            # msvcrt.locking requires a non-empty byte range.
            self._fh.seek(0)
            if self._fh.read(1) == b"":
                self._fh.write(b"\0")
                self._fh.flush()
            self._fh.seek(0)
            msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
            return
        import fcntl

        fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    @staticmethod
    def _unlock(fh) -> None:
        if sys.platform == "win32":
            import msvcrt

            fh.seek(0)
            try:
                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
            return
        import fcntl

        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def acquire_singleton_lock(*, timeout_sec: float | None = None) -> None:
    """Block until this process owns the bot lock, or exit without touching APIs.

    Exit code 0 on failure so a restart-on-failure supervisor does not immediately
    spawn another process that would hammer Groww while the first instance still
    holds the Telegram session.
    """
    global _HELD_LOCK
    if _HELD_LOCK is not None:
        return
    path = lock_file_path()
    if timeout_sec is None:
        timeout_sec = float(os.getenv("BOT_SINGLETON_LOCK_WAIT_SEC", "120"))
    lock = ProcessLock(path)
    logger.info(
        "Acquiring exclusive bot lock at %s (wait up to %.0fs)...",
        path,
        timeout_sec,
    )
    if not lock.acquire(timeout_sec=timeout_sec):
        logger.error(
            "Another bot instance already holds %s. Stop every leftover python "
            "process running app.main (scripts/stop_bot_windows.ps1 or "
            "stop_bot_linux.sh), then start once. Exiting without calling "
            "Groww or Telegram so we do not worsen rate limits or session locks.",
            path,
        )
        sys.exit(0)
    _HELD_LOCK = lock
    logger.info("Acquired exclusive bot lock (PID %s).", os.getpid())
