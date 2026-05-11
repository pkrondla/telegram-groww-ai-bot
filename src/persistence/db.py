"""
SQLite persistence layer for the trading bot.

Replaces the flat JSON files used previously:
  active_trades.json          -> active_trades table
  pending_trades.json         -> pending_entries table
  pending_management.json     -> pending_management table

Schema v3 introduces ``leg_exits`` (one row per individual OCO leg exit, keyed
by ``parent_trade_id``) and ``eod_state`` (per-account/date EOD-sent dedup),
replacing the older ``completed_trades`` table.  ``leg_exits`` is the single
source of truth for all profit reporting (day/week/month and per-signal).

All existing business logic in trader.py is untouched — only the I/O methods
(_load_active_trades, _save_active_trade, etc.) delegate here.

Startup migration
-----------------
If trading_bot.db does not yet exist but active_trades.json is present, the
Database.__init__ migrates all JSON state into the DB automatically, then
renames the JSON files to *.migrated so the bot never reads them again.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from pathlib import Path

from src.core.config import Config

logger = logging.getLogger(__name__)

_DB_PATH = Config.DB_PATH
_MIGRATIONS = [
    # v1 — initial schema
    """
    CREATE TABLE IF NOT EXISTS active_trades (
        symbol     TEXT PRIMARY KEY,
        data       TEXT NOT NULL,
        updated_at REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS pending_entries (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        data       TEXT NOT NULL,
        created_at REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS pending_management (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        data       TEXT NOT NULL,
        created_at REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS completed_trades (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_date       TEXT NOT NULL,
        symbol           TEXT,
        side             TEXT,
        lots             REAL,
        pnl_inr          REAL,
        close_reason     TEXT,
        entry_avg_price  REAL,
        exit_avg_price   REAL,
        opened_at_ist    TEXT,
        closed_at_ist    TEXT,
        eod_sent         INTEGER DEFAULT 0,
        data             TEXT NOT NULL,
        created_at       REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_ct_date   ON completed_trades(trade_date);
    CREATE INDEX IF NOT EXISTS idx_ct_symbol ON completed_trades(symbol);
    CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY);
    INSERT OR IGNORE INTO schema_version VALUES (1);
    """,
    # v2 — account-scoped state for multi-account broker execution
    """
    ALTER TABLE active_trades ADD COLUMN account_id TEXT;
    UPDATE active_trades SET account_id='UNASSIGNED' WHERE account_id IS NULL OR account_id='';
    CREATE TABLE IF NOT EXISTS active_trades_v2 (
        account_id TEXT NOT NULL,
        symbol     TEXT NOT NULL,
        data       TEXT NOT NULL,
        updated_at REAL NOT NULL,
        PRIMARY KEY (account_id, symbol)
    );
    INSERT OR REPLACE INTO active_trades_v2(account_id,symbol,data,updated_at)
    SELECT account_id,symbol,data,updated_at FROM active_trades;
    DROP TABLE active_trades;
    ALTER TABLE active_trades_v2 RENAME TO active_trades;
    CREATE INDEX IF NOT EXISTS idx_active_account ON active_trades(account_id);

    ALTER TABLE pending_entries ADD COLUMN account_id TEXT;
    UPDATE pending_entries SET account_id='UNASSIGNED' WHERE account_id IS NULL OR account_id='';
    CREATE INDEX IF NOT EXISTS idx_pending_entries_account ON pending_entries(account_id, id);

    ALTER TABLE pending_management ADD COLUMN account_id TEXT;
    UPDATE pending_management SET account_id='UNASSIGNED' WHERE account_id IS NULL OR account_id='';
    CREATE INDEX IF NOT EXISTS idx_pending_management_account ON pending_management(account_id, id);

    ALTER TABLE completed_trades ADD COLUMN account_id TEXT;
    UPDATE completed_trades SET account_id='UNASSIGNED' WHERE account_id IS NULL OR account_id='';
    CREATE INDEX IF NOT EXISTS idx_ct_date_account ON completed_trades(trade_date, account_id);

    CREATE TABLE IF NOT EXISTS execution_ledger (
        signal_id   TEXT NOT NULL,
        account_id  TEXT NOT NULL,
        action      TEXT NOT NULL,
        created_at  REAL NOT NULL,
        PRIMARY KEY (signal_id, account_id, action)
    );
    CREATE INDEX IF NOT EXISTS idx_execution_ledger_created ON execution_ledger(created_at);
    INSERT OR IGNORE INTO schema_version VALUES (2);
    """,
    # v3 — replace completed_trades with leg_exits (per-leg PnL tracking) and
    # eod_state (per-account EOD dedup).  No data migration: the DB is clean
    # at deployment time.
    """
    DROP TABLE IF EXISTS completed_trades;

    CREATE TABLE IF NOT EXISTS leg_exits (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        account_id       TEXT NOT NULL,
        parent_trade_id  TEXT NOT NULL,
        symbol           TEXT NOT NULL,
        trade_date       TEXT NOT NULL,
        target_index     INTEGER NOT NULL DEFAULT 0,
        side             TEXT,
        entry_price      REAL NOT NULL DEFAULT 0,
        exit_price       REAL NOT NULL DEFAULT 0,
        qty              INTEGER NOT NULL DEFAULT 0,
        lot_size         INTEGER NOT NULL DEFAULT 1,
        pnl_inr          REAL NOT NULL DEFAULT 0,
        pnl_points       REAL NOT NULL DEFAULT 0,
        close_reason     TEXT,
        exit_pending     INTEGER NOT NULL DEFAULT 0,
        pnl_pending      INTEGER NOT NULL DEFAULT 0,
        exit_fill_source TEXT,
        opened_at_ist    TEXT,
        closed_at_ist    TEXT,
        data             TEXT NOT NULL DEFAULT '{}',
        created_at       REAL NOT NULL,
        UNIQUE (parent_trade_id, account_id, target_index)
    );
    CREATE INDEX IF NOT EXISTS idx_le_date_account   ON leg_exits(trade_date, account_id);
    CREATE INDEX IF NOT EXISTS idx_le_parent         ON leg_exits(parent_trade_id, account_id);
    CREATE INDEX IF NOT EXISTS idx_le_symbol_account ON leg_exits(symbol, account_id);
    CREATE INDEX IF NOT EXISTS idx_le_pending        ON leg_exits(pnl_pending, account_id);

    CREATE TABLE IF NOT EXISTS eod_state (
        account_id  TEXT NOT NULL,
        trade_date  TEXT NOT NULL,
        sent_at     TEXT NOT NULL,
        PRIMARY KEY (account_id, trade_date)
    );
    INSERT OR IGNORE INTO schema_version VALUES (3);
    """,
]


class Database:
    """Thread-safe SQLite wrapper. Use the module-level `db` singleton."""

    def __init__(self, path: str = _DB_PATH):
        self._path = str(Path(path).expanduser())
        parent = Path(self._path).parent
        if str(parent) not in ("", "."):
            parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._cleanup_stale_journal_files()
        is_new = not os.path.exists(self._path)
        timeout_sec = max(1, int(os.getenv("SQLITE_BUSY_TIMEOUT_MS", "30000")) // 1000)
        self._lock_retry_sec = max(0, int(os.getenv("SQLITE_LOCK_RETRY_SEC", "180")))
        self._conn = sqlite3.connect(self._path, timeout=timeout_sec, check_same_thread=False)
        self._with_lock_retry(
            lambda: self._conn.execute(f"PRAGMA busy_timeout={timeout_sec * 1000}"),
            "set SQLite busy_timeout",
        )
        self._configure_journal_mode()
        self._with_lock_retry(
            lambda: self._conn.execute("PRAGMA foreign_keys=ON"),
            "enable SQLite foreign_keys",
        )
        self._apply_migrations()
        if is_new:
            self._migrate_from_json()

    def _cleanup_stale_journal_files(self) -> None:
        """Remove SQLite journal/WAL files left by a previous container instance.

        Only runs for Azure Files paths (/data/…).  On SMB the old container's
        lease may outlive the process and keep these files locked, preventing the
        new connection from opening.  On local filesystems WAL recovery is safe
        and correct — deleting the -wal file there would lose committed data.
        """
        if not self._path.replace("\\", "/").startswith("/data/"):
            return
        for suffix in ("-journal", "-wal", "-shm"):
            stale = self._path + suffix
            try:
                os.remove(stale)
                logger.warning("Removed stale SQLite file %s (leftover from previous container)", stale)
            except FileNotFoundError:
                pass
            except OSError as e:
                logger.warning("Could not remove stale SQLite file %s: %s", stale, e)

    @staticmethod
    def _is_locked_error(exc: sqlite3.OperationalError) -> bool:
        return "database is locked" in str(exc).lower() or "database table is locked" in str(exc).lower()

    def _with_lock_retry(self, operation, label: str):
        deadline = time.monotonic() + self._lock_retry_sec
        attempt = 0
        while True:
            try:
                return operation()
            except sqlite3.OperationalError as e:
                if not self._is_locked_error(e) or time.monotonic() >= deadline:
                    raise
                attempt += 1
                try:
                    self._conn.rollback()
                except Exception:
                    pass
                if attempt == 1 or attempt % 10 == 0:
                    logger.warning("%s waiting for SQLite lock to clear: %s", label, e)
                time.sleep(2)

    def _configure_journal_mode(self) -> None:
        journal_mode = os.getenv("SQLITE_JOURNAL_MODE", "").strip().upper()
        if not journal_mode:
            # Azure Files (SMB) has unreliable byte-range locking.
            # MEMORY keeps the journal in RAM so no journal file is written to the
            # share, reducing SMB lock round-trips.  WAL is unsafe on SMB.
            journal_mode = "MEMORY" if self._path.replace("\\", "/").startswith("/data/") else "WAL"

        # Switching FROM WAL mode (e.g. a DB created locally) requires a WAL
        # checkpoint which needs an exclusive lock.  Use a short independent
        # timeout (30 s) so we fall through to DELETE quickly rather than
        # blocking for the full SQLITE_LOCK_RETRY_SEC window.
        _JOURNAL_MODE_TIMEOUT_SEC = 30
        deadline = time.monotonic() + _JOURNAL_MODE_TIMEOUT_SEC
        attempt = 0
        while True:
            try:
                self._conn.execute(f"PRAGMA journal_mode={journal_mode}")
                return
            except sqlite3.OperationalError as e:
                if not self._is_locked_error(e) or time.monotonic() >= deadline:
                    break
                attempt += 1
                try:
                    self._conn.rollback()
                except Exception:
                    pass
                if attempt == 1 or attempt % 5 == 0:
                    logger.warning(
                        "set SQLite journal_mode=%s waiting for lock to clear: %s", journal_mode, e
                    )
                time.sleep(2)

        if journal_mode != "DELETE":
            logger.warning(
                "SQLite journal_mode=%s could not be set; falling back to DELETE.", journal_mode
            )
            try:
                self._conn.execute("PRAGMA journal_mode=DELETE")
            except sqlite3.OperationalError:
                logger.warning("SQLite journal_mode=DELETE fallback also failed; continuing.")

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _apply_migrations(self) -> None:
        try:
            cur_version = self._with_lock_retry(
                lambda: self._conn.execute(
                    "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
                ).fetchone(),
                "read SQLite schema_version",
            )
            cur = cur_version[0] if cur_version else 0
        except Exception:
            # schema_version table doesn't exist yet — start from scratch.
            cur = 0
        for i, ddl in enumerate(_MIGRATIONS, start=1):
            if i > cur:
                with self._lock:
                    try:
                        self._with_lock_retry(
                            lambda ddl=ddl: self._execute_migration_script(ddl),
                            f"apply SQLite migration step {i}",
                        )
                    except sqlite3.OperationalError as e:
                        # Make migrations idempotent for ALTER TABLE/CREATE INDEX when re-run.
                        msg = str(e).lower()
                        if "duplicate column name" in msg or "already exists" in msg:
                            logger.warning("DB migration step %d already applied: %s", i, e)
                            self._conn.rollback()
                            self._conn.execute(
                                "INSERT OR IGNORE INTO schema_version(version) VALUES (?)", (i,)
                            )
                            self._conn.commit()
                            continue
                        raise

    def _execute_migration_script(self, ddl: str) -> None:
        # Avoid executescript(): it issues an implicit COMMIT before running the DDL,
        # which requires acquiring an exclusive file lock.  On Azure Files SMB that
        # lock acquisition fails ("database is locked") because the previous container's
        # SMB lease may still be alive.  Executing statements individually inside an
        # explicit transaction sidesteps the implicit COMMIT entirely.
        statements = [s.strip() for s in ddl.split(";") if s.strip()]
        for stmt in statements:
            self._conn.execute(stmt)
        self._conn.commit()

    # ------------------------------------------------------------------
    # JSON → SQLite one-time migration
    # ------------------------------------------------------------------

    def _migrate_from_json(self) -> None:
        migrated = 0
        for fname, handler in [
            ("active_trades.json",     self._migrate_active_trades),
            ("pending_trades.json",    self._migrate_pending_entries),
            ("pending_management.json",self._migrate_pending_management),
        ]:
            if os.path.exists(fname):
                try:
                    with open(fname, encoding="utf-8") as f:
                        data = json.load(f)
                    handler(data)
                    os.rename(fname, fname + ".migrated")
                    migrated += 1
                    logger.info("DB migration: imported %s → DB, renamed to %s.migrated", fname, fname)
                except Exception as e:
                    logger.error("DB migration failed for %s: %s", fname, e, exc_info=True)

        if migrated:
            logger.info("DB migration complete — %d file(s) imported.", migrated)

    def _migrate_active_trades(self, data: dict) -> None:
        now = time.time()
        with self._lock:
            for symbol, rec in data.items():
                self._conn.execute(
                    "INSERT OR REPLACE INTO active_trades(symbol, data, updated_at) VALUES (?,?,?)",
                    (str(symbol), json.dumps(rec), now),
                )
            self._conn.commit()

    def _migrate_pending_entries(self, data: list) -> None:
        now = time.time()
        with self._lock:
            for item in (data or []):
                self._conn.execute(
                    "INSERT INTO pending_entries(data, created_at) VALUES (?,?)",
                    (json.dumps(item), now),
                )
            self._conn.commit()

    def _migrate_pending_management(self, data: list) -> None:
        now = time.time()
        with self._lock:
            for item in (data or []):
                self._conn.execute(
                    "INSERT INTO pending_management(data, created_at) VALUES (?,?)",
                    (json.dumps(item), now),
                )
            self._conn.commit()

    # ------------------------------------------------------------------
    # Active trades
    # ------------------------------------------------------------------

    def get_active_trades(self, account_id: str) -> dict:
        rows = self._conn.execute(
            "SELECT symbol, data FROM active_trades WHERE account_id=?",
            (str(account_id),),
        ).fetchall()
        return {r[0]: json.loads(r[1]) for r in rows}

    def upsert_active_trade(self, symbol: str, data: dict, account_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO active_trades(account_id, symbol, data, updated_at) VALUES (?,?,?,?)",
                (str(account_id), symbol, json.dumps(data), time.time()),
            )
            self._conn.commit()

    def delete_active_trade(self, symbol: str, account_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM active_trades WHERE account_id=? AND symbol=?",
                (str(account_id), symbol),
            )
            self._conn.commit()

    def replace_all_active_trades(self, trades: dict, account_id: str) -> None:
        """Atomically replace the entire active-trades table."""
        with self._lock:
            self._conn.execute("DELETE FROM active_trades WHERE account_id=?", (str(account_id),))
            now = time.time()
            for symbol, data in trades.items():
                self._conn.execute(
                    "INSERT INTO active_trades(account_id, symbol, data, updated_at) VALUES (?,?,?,?)",
                    (str(account_id), str(symbol), json.dumps(data), now),
                )
            self._conn.commit()

    # ------------------------------------------------------------------
    # Pending entries
    # ------------------------------------------------------------------

    def get_pending_entries(self, account_id: str) -> list:
        rows = self._conn.execute(
            "SELECT data FROM pending_entries WHERE account_id=? ORDER BY id",
            (str(account_id),),
        ).fetchall()
        return [json.loads(r[0]) for r in rows]

    def replace_pending_entries(self, items: list, account_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM pending_entries WHERE account_id=?", (str(account_id),))
            now = time.time()
            for item in items:
                self._conn.execute(
                    "INSERT INTO pending_entries(account_id, data, created_at) VALUES (?,?,?)",
                    (str(account_id), json.dumps(item), now),
                )
            self._conn.commit()

    # ------------------------------------------------------------------
    # Pending management
    # ------------------------------------------------------------------

    def get_pending_management(self, account_id: str) -> list:
        rows = self._conn.execute(
            "SELECT data FROM pending_management WHERE account_id=? ORDER BY id",
            (str(account_id),),
        ).fetchall()
        return [json.loads(r[0]) for r in rows]

    def replace_pending_management(self, items: list, account_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM pending_management WHERE account_id=?", (str(account_id),))
            now = time.time()
            for item in items:
                self._conn.execute(
                    "INSERT INTO pending_management(account_id, data, created_at) VALUES (?,?,?)",
                    (str(account_id), json.dumps(item), now),
                )
            self._conn.commit()

    # ------------------------------------------------------------------
    # Leg exits — one row per individual OCO leg exit event.
    # This is the single source of truth for all profit reporting.
    # ------------------------------------------------------------------

    @staticmethod
    def _leg_exit_row_to_dict(row: tuple) -> dict:
        """Materialize a leg_exits row tuple into a flat dict.  Column order
        below MUST match the SELECT list used by readers."""
        (
            le_id, account_id, parent_trade_id, symbol, trade_date, target_index,
            side, entry_price, exit_price, qty, lot_size, pnl_inr, pnl_points,
            close_reason, exit_pending, pnl_pending, exit_fill_source,
            opened_at_ist, closed_at_ist, data_json, created_at,
        ) = row
        try:
            data = json.loads(data_json) if data_json else {}
        except Exception:
            data = {}
        return {
            "id": int(le_id),
            "account_id": str(account_id or ""),
            "parent_trade_id": str(parent_trade_id or ""),
            "symbol": str(symbol or ""),
            "trade_date": str(trade_date or ""),
            "target_index": int(target_index or 0),
            "side": str(side or ""),
            "entry_price": float(entry_price or 0.0),
            "exit_price": float(exit_price or 0.0),
            "qty": int(qty or 0),
            "lot_size": int(lot_size or 1),
            "pnl_inr": float(pnl_inr or 0.0),
            "pnl_points": float(pnl_points or 0.0),
            "close_reason": str(close_reason or ""),
            "exit_pending": bool(exit_pending),
            "pnl_pending": bool(pnl_pending),
            "exit_fill_source": str(exit_fill_source or ""),
            "opened_at_ist": str(opened_at_ist or ""),
            "closed_at_ist": str(closed_at_ist or ""),
            "data": data,
            "created_at": float(created_at or 0.0),
        }

    _LEG_EXIT_SELECT_COLUMNS = (
        "id, account_id, parent_trade_id, symbol, trade_date, target_index, "
        "side, entry_price, exit_price, qty, lot_size, pnl_inr, pnl_points, "
        "close_reason, exit_pending, pnl_pending, exit_fill_source, "
        "opened_at_ist, closed_at_ist, data, created_at"
    )

    def insert_leg_exit(self, account_id: str, date_iso: str, row: dict) -> None:
        """Idempotent insert.  Duplicate writes (same parent_trade_id +
        account_id + target_index) are silently dropped — the first write
        wins.  This protects against bot restarts re-triggering stage
        advances before the new oco_legs status has been persisted.
        """
        with self._lock:
            self._conn.execute(
                """INSERT OR IGNORE INTO leg_exits
                   (account_id, parent_trade_id, symbol, trade_date, target_index,
                    side, entry_price, exit_price, qty, lot_size,
                    pnl_inr, pnl_points, close_reason,
                    exit_pending, pnl_pending, exit_fill_source,
                    opened_at_ist, closed_at_ist, data, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    str(account_id),
                    str(row.get("parent_trade_id") or "UNKNOWN"),
                    str(row.get("symbol") or ""),
                    str(date_iso),
                    int(row.get("target_index") or 0),
                    str(row.get("side") or ""),
                    float(row.get("entry_price") or 0.0),
                    float(row.get("exit_price") or 0.0),
                    int(row.get("qty") or 0),
                    int(row.get("lot_size") or 1),
                    float(row.get("pnl_inr") or 0.0),
                    float(row.get("pnl_points") or 0.0),
                    str(row.get("close_reason") or ""),
                    1 if row.get("exit_pending") else 0,
                    1 if row.get("pnl_pending") else 0,
                    str(row.get("exit_fill_source") or ""),
                    str(row.get("opened_at_ist") or ""),
                    str(row.get("closed_at_ist") or ""),
                    json.dumps(row.get("data") or {}),
                    time.time(),
                ),
            )
            self._conn.commit()

    def update_leg_exit_fill(self, leg_exit_id: int, row: dict, account_id: str) -> None:
        """Patch a pending leg_exit row with a real broker fill price."""
        with self._lock:
            self._conn.execute(
                """UPDATE leg_exits SET
                       exit_price=?, qty=?, pnl_inr=?, pnl_points=?,
                       exit_pending=?, pnl_pending=?, exit_fill_source=?,
                       close_reason=COALESCE(NULLIF(?, ''), close_reason),
                       closed_at_ist=COALESCE(NULLIF(?, ''), closed_at_ist),
                       data=?
                   WHERE id=? AND account_id=?""",
                (
                    float(row.get("exit_price") or 0.0),
                    int(row.get("qty") or 0),
                    float(row.get("pnl_inr") or 0.0),
                    float(row.get("pnl_points") or 0.0),
                    1 if row.get("exit_pending") else 0,
                    1 if row.get("pnl_pending") else 0,
                    str(row.get("exit_fill_source") or ""),
                    str(row.get("close_reason") or ""),
                    str(row.get("closed_at_ist") or ""),
                    json.dumps(row.get("data") or {}),
                    int(leg_exit_id),
                    str(account_id),
                ),
            )
            self._conn.commit()

    def get_leg_exits_for_date(self, account_id: str, date_iso: str) -> list[dict]:
        rows = self._conn.execute(
            f"SELECT {self._LEG_EXIT_SELECT_COLUMNS} FROM leg_exits "
            "WHERE account_id=? AND trade_date=? ORDER BY id",
            (str(account_id), str(date_iso)),
        ).fetchall()
        return [self._leg_exit_row_to_dict(r) for r in rows]

    def get_leg_exits_for_range(
        self, account_id: str, start_date: str, end_date: str
    ) -> list[dict]:
        """Inclusive range query over trade_date (ISO YYYY-MM-DD strings)."""
        rows = self._conn.execute(
            f"SELECT {self._LEG_EXIT_SELECT_COLUMNS} FROM leg_exits "
            "WHERE account_id=? AND trade_date>=? AND trade_date<=? "
            "ORDER BY trade_date, id",
            (str(account_id), str(start_date), str(end_date)),
        ).fetchall()
        return [self._leg_exit_row_to_dict(r) for r in rows]

    def get_leg_exits_for_symbol(self, account_id: str, symbol: str) -> list[dict]:
        rows = self._conn.execute(
            f"SELECT {self._LEG_EXIT_SELECT_COLUMNS} FROM leg_exits "
            "WHERE account_id=? AND symbol=? ORDER BY trade_date, id",
            (str(account_id), str(symbol)),
        ).fetchall()
        return [self._leg_exit_row_to_dict(r) for r in rows]

    def get_pending_leg_exits(self, account_id: str) -> list[dict]:
        """All pending leg_exit rows across **all dates**.  Pending exits can
        persist over weekends, so this intentionally does not filter by
        trade_date."""
        rows = self._conn.execute(
            f"SELECT {self._LEG_EXIT_SELECT_COLUMNS} FROM leg_exits "
            "WHERE account_id=? AND pnl_pending=1 ORDER BY id",
            (str(account_id),),
        ).fetchall()
        return [self._leg_exit_row_to_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # EOD state — replaces the old `eod_sent` column on completed_trades.
    # One row per (account, date) marks that the auto-EOD summary was sent.
    # ------------------------------------------------------------------

    def mark_eod_sent(self, account_id: str, date_iso: str, sent_at: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO eod_state(account_id, trade_date, sent_at) VALUES (?,?,?)",
                (str(account_id), str(date_iso), str(sent_at)),
            )
            self._conn.commit()

    def get_eod_sent(self, account_id: str, date_iso: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM eod_state WHERE account_id=? AND trade_date=? LIMIT 1",
            (str(account_id), str(date_iso)),
        ).fetchone()
        return bool(row)

    # ------------------------------------------------------------------
    # Idempotency ledger
    # ------------------------------------------------------------------

    def has_execution(self, signal_id: str, account_id: str, action: str) -> bool:
        row = self._conn.execute(
            """SELECT 1 FROM execution_ledger
               WHERE signal_id=? AND account_id=? AND action=? LIMIT 1""",
            (str(signal_id), str(account_id), str(action).upper()),
        ).fetchone()
        return bool(row)

    def record_execution(self, signal_id: str, account_id: str, action: str) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT OR IGNORE INTO execution_ledger(signal_id, account_id, action, created_at)
                   VALUES (?,?,?,?)""",
                (str(signal_id), str(account_id), str(action).upper(), time.time()),
            )
            self._conn.commit()

    def close(self) -> None:
        self._conn.close()


# Module-level singleton — imported by trader.py and anywhere else that needs it.
db = Database()
