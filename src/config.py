import os
from dotenv import load_dotenv

# Load all environment variables from .env
load_dotenv()

class Config:
    """
    Centralized configuration for the Trading Bot.
    All settings are loaded from environment variables for security.
    """
    
    # Telegram
    API_ID = int(os.getenv("TELEGRAM_API_ID", 31871346))
    API_HASH = os.getenv("TELEGRAM_API_HASH", "20dba7ad500de39e0290f55a1df0b8bb")
    SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "session_name")
    TARGET_CHAT_ID = int(os.getenv("TELEGRAM_TARGET_CHAT_ID", -1002153367077))
    ADMIN_ID = int(os.getenv("TELEGRAM_ADMIN_ID", 1447109810))
    BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    
    # OpenAI
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    
    # Groww
    GROWW_API_KEY = os.getenv("GROWW_API_KEY")
    GROWW_API_SECRET = os.getenv("GROWW_API_SECRET")
    
    # Bot Settings
    # When True: skip entry MARKET/LIMIT BUY/SELL placement; LTP/quotes still use Groww if key is set.
    # EXIT/trail/target go to the broker only for real smart-order ids; simulated book rows use local handling only.
    DRY_RUN = os.getenv("DRY_RUN", "True").lower() == "true"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    # Preferred sizing: quantity = instrument_lot_size * ORDER_LOTS_COUNT.
    # ORDER_QUANTITY is kept as a fallback if lot size cannot be resolved.
    ORDER_LOTS_COUNT = max(1, int(os.getenv("ORDER_LOTS_COUNT", "1")))
    ORDER_QUANTITY = max(1, int(os.getenv("ORDER_QUANTITY", "1")))
    # Fixed points subtracted from signal target for OCO LIMIT target (e.g. 100 → 99.5 when 0.5).
    TARGET_BUFFER_POINTS = max(0.0, float(os.getenv("TARGET_BUFFER_POINTS", "0.5")))

    # When LTP is outside the signal entry_range, queue and retry instead of permanently skipping
    ENTRY_QUEUE_ENABLED = os.getenv("ENTRY_QUEUE_ENABLED", "true").lower() == "true"
    ENTRY_QUEUE_POLL_SEC = int(os.getenv("ENTRY_QUEUE_POLL_SEC", "30"))
    ENTRY_QUEUE_MAX_WAIT_MIN = int(os.getenv("ENTRY_QUEUE_MAX_WAIT_MIN", "240"))

    # Single-leg only: place a broker LIMIT at range edge and poll order status (spreads still use LTP queue)
    # BUY → limit price = range low; SELL → limit price = range low (minimum premium for short)
    ENTRY_SINGLE_LIMIT_ORDER = os.getenv("ENTRY_SINGLE_LIMIT_ORDER", "true").lower() == "true"

    # Active-trade reconciliation watcher (broker positions vs local active_trades.json)
    ACTIVE_RECONCILE_ENABLED = os.getenv("ACTIVE_RECONCILE_ENABLED", "true").lower() == "true"
    ACTIVE_RECONCILE_POLL_SEC = max(10, int(os.getenv("ACTIVE_RECONCILE_POLL_SEC", "60")))
