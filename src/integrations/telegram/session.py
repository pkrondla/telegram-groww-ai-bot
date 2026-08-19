"""Telethon SQLite session with busy_timeout.

Stock Telethon ``SQLiteSession`` connects with the default sqlite timeout
(5s) and no ``PRAGMA busy_timeout``. A brief overlap with a shutting-down
instance then crashes ``client.start()`` with ``database is locked``.
"""
from __future__ import annotations

import sqlite3

from telethon.sessions import SQLiteSession


class BusyTimeoutSQLiteSession(SQLiteSession):
    def __init__(self, session_id=None, *, timeout_sec: float = 30.0):
        self._sqlite_timeout_sec = max(1.0, float(timeout_sec))
        super().__init__(session_id)

    def _cursor(self):
        conn = getattr(self, "_conn", None)
        if conn is None:
            timeout = getattr(self, "_sqlite_timeout_sec", 30.0)
            self._conn = sqlite3.connect(
                self.filename,
                timeout=timeout,
                check_same_thread=False,
            )
            self._conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
        return self._conn.cursor()
