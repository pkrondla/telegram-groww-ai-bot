"""
SQLite persistence layer for the trading bot.

Replaces the flat JSON files used previously:
  active_trades.json          -> active_trades table
  pending_trades.json         -> pending_entries table
  pending_management.json     -> pending_management table
  completed_trades/*.json     -> completed_trades table

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

logger = logging.getLogger(__name__)

_DB_PATH = "trading_bot.db"
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
]


class Database:
    """Thread-safe SQLite wrapper. Use the module-level `db` singleton."""

    def __init__(self, path: str = _DB_PATH):
        self._path = path
        self._lock = threading.Lock()
        is_new = not os.path.exists(path)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._apply_migrations()
        if is_new:
            self._migrate_from_json()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _apply_migrations(self) -> None:
        try:
            cur_version = self._conn.execute(
                "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
            ).fetchone()
            cur = cur_version[0] if cur_version else 0
        except Exception:
            # schema_version table doesn't exist yet — start from scratch.
            cur = 0
        for i, ddl in enumerate(_MIGRATIONS, start=1):
            if i > cur:
                with self._lock:
                    self._conn.executescript(ddl)
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

        ct_dir = "completed_trades"
        if os.path.isdir(ct_dir):
            for fn in sorted(os.listdir(ct_dir)):
                if not fn.endswith(".json"):
                    continue
                path = os.path.join(ct_dir, fn)
                try:
                    with open(path, encoding="utf-8") as f:
                        book = json.load(f)
                    self._migrate_completed_book(book)
                    os.rename(path, path + ".migrated")
                    migrated += 1
                except Exception as e:
                    logger.error("DB migration failed for %s: %s", path, e, exc_info=True)

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

    def _migrate_completed_book(self, book: dict) -> None:
        date = str(book.get("date") or "")
        eod_sent = 1 if book.get("eod", {}).get("summary_sent_at") else 0
        now = time.time()
        with self._lock:
            for row in (book.get("trades") or []):
                self._conn.execute(
                    """INSERT INTO completed_trades
                       (trade_date,symbol,side,lots,pnl_inr,close_reason,
                        entry_avg_price,exit_avg_price,opened_at_ist,closed_at_ist,
                        eod_sent,data,created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        date,
                        str(row.get("symbol") or ""),
                        str(row.get("side") or ""),
                        float(row.get("lots") or 0),
                        float(row.get("pnl_inr") or 0),
                        str(row.get("close_reason") or ""),
                        float(row.get("entry_avg_price") or 0),
                        float(row.get("exit_avg_price") or 0),
                        str(row.get("opened_at_ist") or ""),
                        str(row.get("closed_at_ist") or ""),
                        eod_sent,
                        json.dumps(row),
                        now,
                    ),
                )
            self._conn.commit()

    # ------------------------------------------------------------------
    # Active trades
    # ------------------------------------------------------------------

    def get_active_trades(self) -> dict:
        rows = self._conn.execute(
            "SELECT symbol, data FROM active_trades"
        ).fetchall()
        return {r[0]: json.loads(r[1]) for r in rows}

    def upsert_active_trade(self, symbol: str, data: dict) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO active_trades(symbol, data, updated_at) VALUES (?,?,?)",
                (symbol, json.dumps(data), time.time()),
            )
            self._conn.commit()

    def delete_active_trade(self, symbol: str) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM active_trades WHERE symbol=?", (symbol,)
            )
            self._conn.commit()

    def replace_all_active_trades(self, trades: dict) -> None:
        """Atomically replace the entire active-trades table."""
        with self._lock:
            self._conn.execute("DELETE FROM active_trades")
            now = time.time()
            for symbol, data in trades.items():
                self._conn.execute(
                    "INSERT INTO active_trades(symbol, data, updated_at) VALUES (?,?,?)",
                    (str(symbol), json.dumps(data), now),
                )
            self._conn.commit()

    # ------------------------------------------------------------------
    # Pending entries
    # ------------------------------------------------------------------

    def get_pending_entries(self) -> list:
        rows = self._conn.execute(
            "SELECT data FROM pending_entries ORDER BY id"
        ).fetchall()
        return [json.loads(r[0]) for r in rows]

    def replace_pending_entries(self, items: list) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM pending_entries")
            now = time.time()
            for item in items:
                self._conn.execute(
                    "INSERT INTO pending_entries(data, created_at) VALUES (?,?)",
                    (json.dumps(item), now),
                )
            self._conn.commit()

    # ------------------------------------------------------------------
    # Pending management
    # ------------------------------------------------------------------

    def get_pending_management(self) -> list:
        rows = self._conn.execute(
            "SELECT data FROM pending_management ORDER BY id"
        ).fetchall()
        return [json.loads(r[0]) for r in rows]

    def replace_pending_management(self, items: list) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM pending_management")
            now = time.time()
            for item in items:
                self._conn.execute(
                    "INSERT INTO pending_management(data, created_at) VALUES (?,?)",
                    (json.dumps(item), now),
                )
            self._conn.commit()

    # ------------------------------------------------------------------
    # Completed trades
    # ------------------------------------------------------------------

    def get_completed_trades_for_date(self, date_iso: str) -> dict:
        """Return a book dict matching the old JSON structure."""
        rows = self._conn.execute(
            "SELECT data, eod_sent FROM completed_trades WHERE trade_date=? ORDER BY id",
            (date_iso,),
        ).fetchall()
        trades = [json.loads(r[0]) for r in rows]
        eod_sent_at = "" if not rows or not rows[-1][1] else date_iso
        return {
            "date": date_iso,
            "timezone": "Asia/Kolkata",
            "trades": trades,
            "eod": {"summary_sent_at": eod_sent_at},
        }

    def insert_completed_trade(self, date_iso: str, row: dict, eod_sent: bool = False) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT INTO completed_trades
                   (trade_date,symbol,side,lots,pnl_inr,close_reason,
                    entry_avg_price,exit_avg_price,opened_at_ist,closed_at_ist,
                    eod_sent,data,created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    date_iso,
                    str(row.get("symbol") or ""),
                    str(row.get("side") or ""),
                    float(row.get("lots") or 0),
                    float(row.get("pnl_inr") or 0),
                    str(row.get("close_reason") or ""),
                    float(row.get("entry_avg_price") or 0),
                    float(row.get("exit_avg_price") or 0),
                    str(row.get("opened_at_ist") or ""),
                    str(row.get("closed_at_ist") or ""),
                    1 if eod_sent else 0,
                    json.dumps(row),
                    time.time(),
                ),
            )
            self._conn.commit()

    def update_completed_trade_row(self, trade_id: str, row: dict) -> None:
        """Update an existing completed trade row by trade_id field inside data."""
        with self._lock:
            self._conn.execute(
                """UPDATE completed_trades SET data=?, pnl_inr=?, exit_avg_price=?, eod_sent=?
                   WHERE json_extract(data,'$.trade_id')=?""",
                (
                    json.dumps(row),
                    float(row.get("pnl_inr") or 0),
                    float(row.get("exit_avg_price") or 0),
                    1 if row.get("eod") else 0,
                    str(trade_id),
                ),
            )
            self._conn.commit()

    def mark_eod_sent(self, date_iso: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE completed_trades SET eod_sent=1 WHERE trade_date=?",
                (date_iso,),
            )
            self._conn.commit()

    def close(self) -> None:
        self._conn.close()


# Module-level singleton — imported by trader.py and anywhere else that needs it.
db = Database()
