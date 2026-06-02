import logging
import sys
import time
from logging.handlers import TimedRotatingFileHandler
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


def setup_logger():
    """
    Industrial-standard dual-output logger setup.
    Outputs to both the console and a daily-rotated persistent log file.
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
        
    log_filename = "logs/trading.log"

    # Force stdout to handle UTF-8 if possible (Fix for Windows Unicode Errors)
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    
    # Create handlers
    c_handler = logging.StreamHandler(sys.stdout)
    f_handler = TimedRotatingFileHandler(
        log_filename,
        when='midnight',
        interval=1,
        backupCount=30,
        encoding='utf-8',
    )
    # Keep historical files in trading_YYYY-MM-DD.log format.
    f_handler.suffix = "%Y-%m-%d"
    
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
    
    return logger

# Single instance of the logger to be used across all modules
logger = setup_logger()
