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
    # Preferred sizing: quantity = instrument_lot_size * lots_count (separate index/stock knobs).
    INDEX_ORDER_LOTS_COUNT = max(1, int(os.getenv("INDEX_ORDER_LOTS_COUNT", "1")))
    STOCK_ORDER_LOTS_COUNT = max(1, int(os.getenv("STOCK_ORDER_LOTS_COUNT", "1")))
    ORDER_QUANTITY = max(1, int(os.getenv("ORDER_QUANTITY", "1")))
    # Percentage cushion below signal target for OCO LIMIT target
    # (e.g. 2% => 100 -> 98, 5 -> 4.9).
    TARGET_BUFFER_PCT = max(0.0, min(0.5, float(os.getenv("TARGET_BUFFER_PCT", "0.02"))))
    # Multi-target OCO split for single-leg BUY entries.
    MULTI_TARGET_ENABLED = os.getenv("MULTI_TARGET_ENABLED", "true").lower() == "true"
    TARGET_SPLIT_2 = os.getenv("TARGET_SPLIT_2", "60:40").strip()
    TARGET_SPLIT_3 = os.getenv("TARGET_SPLIT_3", "60:30:10").strip()
    # Price tick size used to sanitize OCO target/SL prices before broker API calls.
    TARGET_PRICE_TICK_SIZE = max(0.01, float(os.getenv("TARGET_PRICE_TICK_SIZE", "0.05")))
    # Position sizing mode by segment: fixed (configured lots) or risk (SL-distance based).
    INDEX_POSITION_SIZING_MODE = os.getenv("INDEX_POSITION_SIZING_MODE", "fixed").strip().lower()
    STOCK_POSITION_SIZING_MODE = os.getenv("STOCK_POSITION_SIZING_MODE", "fixed").strip().lower()
    ACCOUNT_CAPITAL_INR = max(0.0, float(os.getenv("ACCOUNT_CAPITAL_INR", "0")))
    RISK_PCT_PER_TRADE = max(0.0, min(1.0, float(os.getenv("RISK_PCT_PER_TRADE", "0.01"))))
    MAX_TOTAL_OPEN_RISK_PCT = max(0.0, min(1.0, float(os.getenv("MAX_TOTAL_OPEN_RISK_PCT", "0.03"))))
    INDEX_MIN_LOTS = max(1, int(os.getenv("INDEX_MIN_LOTS", "1")))
    INDEX_MAX_LOTS = max(1, int(os.getenv("INDEX_MAX_LOTS", "999")))
    STOCK_MIN_LOTS = max(1, int(os.getenv("STOCK_MIN_LOTS", "1")))
    STOCK_MAX_LOTS = max(1, int(os.getenv("STOCK_MAX_LOTS", "999")))
    RISK_MODE_ON_ZERO_LOTS = os.getenv("RISK_MODE_ON_ZERO_LOTS", "fixed").strip().lower()

    # When LTP is outside the signal entry_range, queue and retry instead of permanently skipping
    ENTRY_QUEUE_ENABLED = os.getenv("ENTRY_QUEUE_ENABLED", "true").lower() == "true"
    ENTRY_QUEUE_POLL_SEC = int(os.getenv("ENTRY_QUEUE_POLL_SEC", "30"))
    ENTRY_QUEUE_MAX_WAIT_MIN = int(os.getenv("ENTRY_QUEUE_MAX_WAIT_MIN", "240"))

    # Single-leg only: place a broker LIMIT at range edge and poll order status (spreads still use LTP queue)
    # BUY → limit price = range low; SELL → limit price = range low (minimum premium for short)
    ENTRY_SINGLE_LIMIT_ORDER = os.getenv("ENTRY_SINGLE_LIMIT_ORDER", "true").lower() == "true"
    # Simple BUY chase policy: if LTP is above range max, allow market entry only when
    # reward:risk at current LTP is >= CHASE_MIN_RR; otherwise place LIMIT at RR=CHASE_MIN_RR price.
    CHASE_MIN_RR = max(0.0, float(os.getenv("CHASE_MIN_RR", "1.0")))

    # Active-trade reconciliation watcher (broker positions vs local active_trades.json)
    ACTIVE_RECONCILE_ENABLED = os.getenv("ACTIVE_RECONCILE_ENABLED", "true").lower() == "true"
    ACTIVE_RECONCILE_POLL_SEC = max(10, int(os.getenv("ACTIVE_RECONCILE_POLL_SEC", "60")))
    # Auto re-arm OCO for carry-forward positions. Keep FALSE when broker already carries OCO overnight.
    AUTO_REARM_PROTECTION = os.getenv("AUTO_REARM_PROTECTION", "false").lower() == "true"

    # Volatility schedule: widen OCO SL before close, restore next morning (IST). Policy 1: admin TRAIL_SL
    # still updates broker SL immediately; sl_true in active_trades tracks the channel's true SL.
    VOL_SL_SCHEDULE_ENABLED = os.getenv("VOL_SL_SCHEDULE_ENABLED", "false").lower() == "true"
    VOL_SL_WIDEN_TIME_IST = os.getenv("VOL_SL_WIDEN_TIME_IST", "15:15")  # HH:MM
    VOL_SL_RESTORE_TIME_IST = os.getenv("VOL_SL_RESTORE_TIME_IST", "09:25")
    VOL_SL_WIDEN_PCT = max(0.0, min(0.95, float(os.getenv("VOL_SL_WIDEN_PCT", "0.10"))))
    VOL_SL_SCHEDULE_POLL_SEC = max(10, int(os.getenv("VOL_SL_SCHEDULE_POLL_SEC", "30")))
    VOL_SL_SKIP_WEEKENDS = os.getenv("VOL_SL_SKIP_WEEKENDS", "true").lower() == "true"

    # End-of-day completed-trades summary (IST).
    EOD_SUMMARY_ENABLED = os.getenv("EOD_SUMMARY_ENABLED", "true").lower() == "true"
    EOD_SUMMARY_POLL_SEC = max(10, int(os.getenv("EOD_SUMMARY_POLL_SEC", "30")))
    EOD_SUMMARY_SKIP_WEEKENDS = os.getenv("EOD_SUMMARY_SKIP_WEEKENDS", "true").lower() == "true"

    # Management actions queue (EXIT/TRAIL/RESTORE/TARGET updates).
    MGMT_QUEUE_ENABLED = os.getenv("MGMT_QUEUE_ENABLED", "true").lower() == "true"
    MGMT_QUEUE_POLL_SEC = max(5, int(os.getenv("MGMT_QUEUE_POLL_SEC", "10")))
    MGMT_QUEUE_MAX_RETRIES = max(1, int(os.getenv("MGMT_QUEUE_MAX_RETRIES", "20")))
    MGMT_QUEUE_TTL_MIN = max(5, int(os.getenv("MGMT_QUEUE_TTL_MIN", "360")))
    MARKET_OPEN_TIME_IST = os.getenv("MARKET_OPEN_TIME_IST", "09:15")
    MARKET_CLOSE_TIME_IST = os.getenv("MARKET_CLOSE_TIME_IST", "15:30")
    GROWW_AUTH_POLL_SEC = max(15, int(os.getenv("GROWW_AUTH_POLL_SEC", "60")))

