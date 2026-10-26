import os
import sys
from dotenv import load_dotenv
from src.trading.execution.accounts import AccountConfig, parse_accounts_json

# Load all environment variables from .env
load_dotenv()

class Config:
    """
    Centralized configuration for the Trading Bot.
    All settings are loaded from environment variables for security.
    """

    # Telegram — no defaults; all must be set in .env
    API_ID = int(os.getenv("TELEGRAM_API_ID") or 0) or None
    API_HASH = os.getenv("TELEGRAM_API_HASH")
    SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "session_name")
    PHONE = os.getenv("TELEGRAM_PHONE", "").strip()
    DATA_DIR = os.getenv("DATA_DIR", "").strip()
    SESSION_PATH = (
        os.getenv("SESSION_PATH")
        or os.getenv("TELEGRAM_SESSION_PATH")
        or (os.path.join(DATA_DIR, SESSION_NAME) if DATA_DIR else SESSION_NAME)
    )
    # Persistent copy of the session file on durable storage (e.g. Azure Files /data/).
    # When set, main.py copies {SESSION_PERSISTENT_PATH}.session → {SESSION_PATH}.session
    # at startup and back again on clean shutdown.  This lets Telethon work against a
    # local path (no SMB locking) while still surviving container restarts.
    SESSION_PERSISTENT_PATH = os.getenv("SESSION_PERSISTENT_PATH", "").strip()
    TARGET_CHAT_ID = int(os.getenv("TELEGRAM_TARGET_CHAT_ID") or 0) or None
    ADMIN_ID = int(os.getenv("TELEGRAM_ADMIN_ID") or 0) or None
    BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

    # OpenAI
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    # Groww
    GROWW_ACCOUNTS_JSON = os.getenv("GROWW_ACCOUNTS_JSON", "").strip()

    # Persistent state paths. In Azure Container Apps, point these at the
    # Azure Files mount (for example, /data/trading_bot.db and /data/session_name).
    DB_PATH = os.getenv(
        "DB_PATH",
        os.path.join(DATA_DIR, "trading_bot.db") if DATA_DIR else "trading_bot.db",
    )

    # Required at startup — bot will not start if any of these are missing.
    _REQUIRED = {
        "TELEGRAM_API_ID":      "API_ID",
        "TELEGRAM_API_HASH":    "API_HASH",
        "TELEGRAM_TARGET_CHAT_ID": "TARGET_CHAT_ID",
        "TELEGRAM_ADMIN_ID":    "ADMIN_ID",
        "OPENAI_API_KEY":       "OPENAI_API_KEY",
    }

    @classmethod
    def validate(cls) -> None:
        """
        Call once at startup. Raises SystemExit listing every missing required
        env var so the operator knows exactly what to set in .env.
        """
        missing = [
            env_name
            for env_name, attr in cls._REQUIRED.items()
            if not getattr(cls, attr)
        ]
        if missing:
            lines = "\n".join(f"  - {v}" for v in missing)
            print(
                f"\n[CONFIG] Bot cannot start — missing required environment variables:\n"
                f"{lines}\n"
                f"Copy .env.example to .env and fill in the missing values.\n",
                file=sys.stderr,
            )
            sys.exit(1)
    
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
    RISK_PCT_PER_TRADE = max(0.0, min(1.0, float(os.getenv("RISK_PCT_PER_TRADE", "0.01"))))
    MAX_TOTAL_OPEN_RISK_PCT = max(0.0, min(1.0, float(os.getenv("MAX_TOTAL_OPEN_RISK_PCT", "0.03"))))
    INDEX_MIN_LOTS = max(1, int(os.getenv("INDEX_MIN_LOTS", "1")))
    INDEX_MAX_LOTS = max(1, int(os.getenv("INDEX_MAX_LOTS", "999")))
    STOCK_MIN_LOTS = max(1, int(os.getenv("STOCK_MIN_LOTS", "1")))
    STOCK_MAX_LOTS = max(1, int(os.getenv("STOCK_MAX_LOTS", "999")))
    RISK_MODE_ON_ZERO_LOTS = os.getenv("RISK_MODE_ON_ZERO_LOTS", "fixed").strip().lower()
    # New F&O entries only. Management of already-open positions is unchanged.
    INDEX_FNO_ENABLED = os.getenv("INDEX_FNO_ENABLED", "true").lower() == "true"
    STOCK_FNO_ENABLED = os.getenv("STOCK_FNO_ENABLED", "true").lower() == "true"

    @classmethod
    def fno_entry_block_reason(cls, *symbols: str, trade_data: dict | None = None) -> str | None:
        """Return a skip reason when a new entry hits a disabled F&O segment."""
        from src.trading.reference.option_symbols import is_index_fno

        names = [str(s or "") for s in symbols]
        data = trade_data if isinstance(trade_data, dict) else {}
        for key in ("official_symbol", "underlying", "stock", "trading_symbol"):
            names.append(str(data.get(key) or ""))
        for leg in data.get("legs") or []:
            if not isinstance(leg, dict):
                continue
            names.append(str(leg.get("trading_symbol") or ""))
            names.append(str(leg.get("official_symbol") or ""))
            names.append(str(leg.get("stock") or ""))

        saw_index = False
        saw_stock = False
        for raw in names:
            if not raw.strip():
                continue
            if is_index_fno(raw):
                saw_index = True
            else:
                saw_stock = True

        index_blocked = saw_index and not cls.INDEX_FNO_ENABLED
        stock_blocked = saw_stock and not cls.STOCK_FNO_ENABLED
        if index_blocked and stock_blocked:
            return "Index and stock F&O disabled"
        if index_blocked:
            return "Index F&O disabled"
        if stock_blocked:
            return "Stock F&O disabled"
        return None

    # When LTP is outside the signal entry_range, queue and retry instead of permanently skipping
    ENTRY_QUEUE_ENABLED = os.getenv("ENTRY_QUEUE_ENABLED", "true").lower() == "true"
    ENTRY_QUEUE_POLL_SEC = int(os.getenv("ENTRY_QUEUE_POLL_SEC", "30"))
    ENTRY_QUEUE_MAX_WAIT_MIN = int(os.getenv("ENTRY_QUEUE_MAX_WAIT_MIN", "240"))
    SIZE_ADVISORY_TTL_SEC = max(60, int(os.getenv("SIZE_ADVISORY_TTL_SEC", "900")))

    # Single-leg only: place a broker LIMIT at range edge and poll order status (spreads still use LTP queue)
    # BUY → limit price = range low; SELL → limit price = range low (minimum premium for short)
    ENTRY_SINGLE_LIMIT_ORDER = os.getenv("ENTRY_SINGLE_LIMIT_ORDER", "true").lower() == "true"
    # Simple BUY chase policy: if LTP is above range max, allow market entry only when
    # reward:risk at current LTP is >= CHASE_MIN_RR; otherwise place LIMIT at RR=CHASE_MIN_RR price.
    CHASE_MIN_RR = max(0.0, float(os.getenv("CHASE_MIN_RR", "1.0")))
    # Which parsed target to use for chase R:R math (1-based index into target ladder).
    # Example: 1 -> T1, 2 -> T2, 3 -> T3. Falls back to T1 when unavailable.
    CHASE_RR_TARGET_INDEX = min(3, max(1, int(os.getenv("CHASE_RR_TARGET_INDEX", "1"))))

    # Active-trade reconciliation watcher (broker positions vs local active_trades.json)
    ACTIVE_RECONCILE_ENABLED = os.getenv("ACTIVE_RECONCILE_ENABLED", "true").lower() == "true"
    ACTIVE_RECONCILE_POLL_SEC = max(10, int(os.getenv("ACTIVE_RECONCILE_POLL_SEC", "60")))
    # Auto re-arm OCO for carry-forward positions. Keep FALSE when broker already carries OCO overnight.
    AUTO_REARM_PROTECTION = os.getenv("AUTO_REARM_PROTECTION", "false").lower() == "true"
    AUTO_REARM_POLL_SEC = max(10, int(os.getenv("AUTO_REARM_POLL_SEC", "60")))
    # OCO protection plan integrity enforcement rollout:
    # - SHADOW: log mismatches only (no strict failure semantics)
    # - ENFORCE: mark integrity degraded and force repair paths.
    OCO_PLAN_ENFORCEMENT_MODE = os.getenv("OCO_PLAN_ENFORCEMENT_MODE", "ENFORCE").strip().upper()

    # Volatility schedule: drop OCO SL before market open auction, re-arm after spike window (IST).
    # Drop at VOL_SL_DROP_SL_TIME_IST cancels all OCO legs (no SL during opening spike).
    # Re-arm at VOL_SL_REARM_SL_TIME_IST recreates OCO with sl_true once the spike window passes.
    VOL_SL_SCHEDULE_ENABLED = os.getenv("VOL_SL_SCHEDULE_ENABLED", "false").lower() == "true"
    VOL_SL_DROP_SL_TIME_IST = os.getenv("VOL_SL_DROP_SL_TIME_IST", "09:10")   # HH:MM cancel OCO
    VOL_SL_REARM_SL_TIME_IST = os.getenv("VOL_SL_REARM_SL_TIME_IST", "09:20")  # HH:MM re-arm OCO
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
    BROKER_WINDOW_PREOPEN_MIN = max(0, int(os.getenv("BROKER_WINDOW_PREOPEN_MIN", "15")))
    BROKER_WINDOW_POSTCLOSE_MIN = max(0, int(os.getenv("BROKER_WINDOW_POSTCLOSE_MIN", "15")))
    GROWW_AUTH_POLL_SEC = max(15, int(os.getenv("GROWW_AUTH_POLL_SEC", "60")))
    # Pause Groww get_access_token after HTTP 429 so a crash-loop cannot keep
    # exhausting the broker quota. Background auth watcher resumes after this.
    GROWW_TOKEN_COOLDOWN_SEC = max(60, int(os.getenv("GROWW_TOKEN_COOLDOWN_SEC", "180")))
    # Groww allows 150 access-token requests per 24h (30/min). Re-logins after the first are
    # spaced at least GROWW_TOKEN_REFRESH_MIN_SEC apart and capped per rolling 24h.
    GROWW_TOKEN_REFRESH_MIN_SEC = max(30, int(os.getenv("GROWW_TOKEN_REFRESH_MIN_SEC", "300")))
    GROWW_TOKEN_DAILY_MAX = max(1, int(os.getenv("GROWW_TOKEN_DAILY_MAX", "100")))

    # When False (default), EXTEND_RANGE for an unknown symbol is rejected instead of
    # silently becoming a fresh BUY. Set True only if you intentionally want that behaviour.
    EXTEND_RANGE_ALLOWS_ENTRY = os.getenv("EXTEND_RANGE_ALLOWS_ENTRY", "false").lower() == "true"

    # Pending leg_exits backfill: give up when the broker fill never appears
    # (expired contracts, missing order ids). Avoids unbounded reconcile loops.
    LEG_EXIT_BACKFILL_MAX_ATTEMPTS = max(1, int(os.getenv("LEG_EXIT_BACKFILL_MAX_ATTEMPTS", "30")))
    LEG_EXIT_BACKFILL_MAX_AGE_DAYS = max(1, int(os.getenv("LEG_EXIT_BACKFILL_MAX_AGE_DAYS", "3")))

    @classmethod
    def groww_accounts(cls) -> list[AccountConfig]:
        """
        Multi-account resolution (strict):
        - GROWW_ACCOUNTS_JSON must be provided and must contain at least one enabled account.
        - Every enabled account needs api_key (the Groww TOTP token) and totp_secret.
        """
        if not cls.GROWW_ACCOUNTS_JSON:
            raise ValueError("Missing GROWW_ACCOUNTS_JSON (multi-account mode is required).")
        accounts = parse_accounts_json(cls.GROWW_ACCOUNTS_JSON)
        enabled = [a for a in accounts if a.enabled]
        if not enabled:
            raise ValueError("GROWW_ACCOUNTS_JSON has no enabled accounts.")
        for a in enabled:
            if not str(a.api_key or "").strip():
                raise ValueError(
                    f"GROWW_ACCOUNTS_JSON account '{a.account_id}' is missing api_key "
                    "(use the Groww TOTP token)."
                )
            if not str(a.totp_secret or "").strip():
                raise ValueError(
                    f"GROWW_ACCOUNTS_JSON account '{a.account_id}' is missing totp_secret "
                    "(the secret shown when generating the Groww TOTP token)."
                )
        return accounts

