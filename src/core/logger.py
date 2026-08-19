import logging
import sys
import time
from datetime import datetime
from pathlib import Path
import requests
from .config import Config


class TelegramErrorAlertHandler(logging.Handler):
    """
    Sends ERROR/CRITICAL log records to admin via Telegram Bot API.
    Uses simple dedupe cooldown to avoid alert floods on repeated errors.
    """

    def __init__(self, cooldown_sec: int = 20):
        super().__init__(level=logging.ERROR)
        self._cooldown_sec = max(1, int(cooldown_sec))
        self._last_sent_at = 0.0
        self._last_fingerprint = ""

    def emit(self, record: logging.LogRecord):
        # Requires bot token + admin id configured.
        if not Config.BOT_TOKEN or not Config.ADMIN_ID:
            return
        try:
            msg = self.format(record)
            fp = f"{record.levelname}|{record.name}|{record.msg}"
            now = time.time()
            if fp == self._last_fingerprint and (now - self._last_sent_at) < self._cooldown_sec:
                return
            self._last_fingerprint = fp
            self._last_sent_at = now

            text = (
                "❌ BOT ERROR ALERT\n"
                f"Level: {record.levelname}\n"
                f"{msg[-3500:]}"
            )
            requests.post(
                f"https://api.telegram.org/bot{Config.BOT_TOKEN}/sendMessage",
                data={
                    "chat_id": Config.ADMIN_ID,
                    "text": text,
                },
                timeout=8,
            )
        except Exception:
            # Never raise from logging handler; avoid recursive logging loops.
            pass


class DailyDateFileHandler(logging.Handler):
    """
    Writes to logs/trading_YYYY-MM-DD.log and switches automatically after midnight.
    Keeps only the latest `backup_count` dated files.
    """

    def __init__(self, log_dir: str = "logs", prefix: str = "trading", backup_count: int = 30, encoding: str = "utf-8"):
        super().__init__()
        self.log_dir = Path(log_dir)
        self.prefix = prefix
        self.backup_count = max(1, int(backup_count))
        self.encoding = encoding
        self._current_date = None
        self._file_handler = None
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._ensure_handler()

    def _today_str(self) -> str:
        return datetime.now().strftime("%Y-%m-%d")

    def _log_path_for(self, date_str: str) -> Path:
        return self.log_dir / f"{self.prefix}_{date_str}.log"

    def _ensure_handler(self):
        today = self._today_str()
        if self._file_handler is not None and self._current_date == today:
            return

        if self._file_handler is not None:
            self._file_handler.close()

        self._current_date = today
        self._file_handler = logging.FileHandler(self._log_path_for(today), encoding=self.encoding)
        self._file_handler.setLevel(self.level)
        if self.formatter:
            self._file_handler.setFormatter(self.formatter)
        self._cleanup_old_files()

    def _cleanup_old_files(self):
        pattern = f"{self.prefix}_*.log"
        dated_logs = sorted(self.log_dir.glob(pattern), key=lambda p: p.stat().st_mtime)
        excess = len(dated_logs) - self.backup_count
        if excess > 0:
            for old_file in dated_logs[:excess]:
                try:
                    old_file.unlink()
                except OSError:
                    pass

    def emit(self, record: logging.LogRecord):
        try:
            self._ensure_handler()
            self._file_handler.emit(record)
        except Exception:
            self.handleError(record)

    def setFormatter(self, fmt):
        super().setFormatter(fmt)
        if self._file_handler:
            self._file_handler.setFormatter(fmt)

    def close(self):
        if self._file_handler is not None:
            self._file_handler.close()
            self._file_handler = None
        super().close()


def _console_stream():
    """
    Stream for console logging that is not a colorama AnsiToWin32 wrapper.

    On Windows, colorama may wrap sys.stderr/stdout in a self-referential
    StreamWrapper. logging.lastResort writes to stderr; that wrap loop
    raises RecursionError and aborts the trade thread.
    """
    for candidate in (
        getattr(sys, "__stdout__", None),
        getattr(sys, "__stderr__", None),
        sys.stdout,
        sys.stderr,
    ):
        if candidate is None:
            continue
        if getattr(candidate, "_StreamWrapper__convertor", None) is not None:
            continue
        return candidate
    return sys.__stdout__


def setup_logger():
    """
    Industrial-standard dual-output logger setup.
    Outputs to both the console and a daily-dated persistent log file.
    """
    
    # Create a custom logger
    logger = logging.getLogger("TradingBot")
    logger.setLevel(Config.LOG_LEVEL)
    logger.propagate = False

    # Prevent duplicate handlers when module is re-imported/reloaded
    if logger.handlers:
        return logger

    import os
    
    # Create logs directory if missing
    if not os.path.exists("logs"):
        os.makedirs("logs")

    log_stream = _console_stream()
    if hasattr(log_stream, "reconfigure"):
        try:
            log_stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    
    # Create handlers
    c_handler = logging.StreamHandler(log_stream)
    f_handler = DailyDateFileHandler(log_dir="logs", prefix="trading", backup_count=30, encoding="utf-8")
    
    c_handler.setLevel(Config.LOG_LEVEL)
    f_handler.setLevel(Config.LOG_LEVEL)

    # Create formatters and add it to handlers
    # Example: 2026-04-03 23:25:01 [INFO] :: Bot started...
    log_format = logging.Formatter('%(asctime)s [%(levelname)s] :: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    c_handler.setFormatter(log_format)
    f_handler.setFormatter(log_format)

    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)

    # Error alerts to Telegram admin for all logger.error/logger.critical calls.
    err_handler = TelegramErrorAlertHandler(cooldown_sec=20)
    err_handler.setLevel(logging.ERROR)
    err_handler.setFormatter(log_format)
    logger.addHandler(err_handler)

    # logging.getLogger(__name__) in trader/db/queue_manager must not fall
    # through to lastResort on a colorama-wrapped stderr.
    root = logging.getLogger()
    root.setLevel(Config.LOG_LEVEL)
    for handler in (c_handler, f_handler, err_handler):
        if handler not in root.handlers:
            root.addHandler(handler)
    try:
        last_resort = logging.StreamHandler(log_stream)
        last_resort.setLevel(logging.WARNING)
        last_resort.setFormatter(log_format)
        logging.lastResort = last_resort
    except Exception:
        logging.lastResort = None
    
    return logger

# Single instance of the logger to be used across all modules
logger = setup_logger()
