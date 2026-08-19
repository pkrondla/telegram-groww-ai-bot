"""Startup resilience: singleton lock, Telethon busy_timeout, Groww 429 cooldown."""
from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("TELEGRAM_API_ID", "1")
os.environ.setdefault("TELEGRAM_API_HASH", "x")
os.environ.setdefault("TELEGRAM_TARGET_CHAT_ID", "1")
os.environ.setdefault("TELEGRAM_ADMIN_ID", "1")
os.environ.setdefault("OPENAI_API_KEY", "x")
os.environ.setdefault("GROWW_TOKEN_COOLDOWN_SEC", "180")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

_groww = types.ModuleType("growwapi")


class _FakeGrowwAPI:
    SMART_ORDER_TYPE_OCO = "OCO"
    token_calls = 0
    fail_token = True

    @classmethod
    def get_access_token(cls, **_kwargs):
        cls.token_calls += 1
        if cls.fail_token:
            raise RuntimeError(
                "The rate limit for the Groww API has been exceeded. Please try again later."
            )
        return "ok-token"

    def __init__(self, token):
        self.token = token

    def get_user_profile(self):
        raise RuntimeError("Access forbidden for this request.")


_groww.GrowwAPI = _FakeGrowwAPI
sys.modules["growwapi"] = _groww

from app.process_lock import ProcessLock, _is_bot_command_line  # noqa: E402
from src.integrations.telegram.listener import BotListener  # noqa: E402
from src.integrations.telegram.session import BusyTimeoutSQLiteSession  # noqa: E402
from src.trading.execution.trader import Trader  # noqa: E402


class TestProcessLock(unittest.TestCase):
    def test_second_acquire_fails_while_held(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bot.lock"
            first = ProcessLock(path)
            second = ProcessLock(path)
            self.assertTrue(first.acquire(timeout_sec=1))
            try:
                self.assertFalse(second.acquire(timeout_sec=0.3))
            finally:
                first.release()
            self.assertTrue(second.acquire(timeout_sec=1))
            second.release()

    def test_bot_command_line_detection(self):
        self.assertTrue(_is_bot_command_line(r"python.exe -m app.main"))
        self.assertTrue(_is_bot_command_line(r"C:\Python\python.exe C:\groww-bot\app\main.py"))
        self.assertFalse(_is_bot_command_line(r"python.exe -m pytest tests/unit/test_startup_resilience.py"))
        self.assertFalse(_is_bot_command_line(r"python.exe scripts\other.py"))


class TestBusyTimeoutSession(unittest.TestCase):
    def test_sets_busy_timeout_pragma(self):
        with tempfile.TemporaryDirectory() as tmp:
            session_path = str(Path(tmp) / "sess")
            session = BusyTimeoutSQLiteSession(session_path, timeout_sec=12)
            try:
                row = session._conn.execute("PRAGMA busy_timeout").fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(int(row[0]), 12000)
            finally:
                session.close()


class TestGrowwTokenCooldown(unittest.TestCase):
    def setUp(self):
        Trader._token_cooldown_until = 0.0
        _FakeGrowwAPI.token_calls = 0
        _FakeGrowwAPI.fail_token = True

    def tearDown(self):
        Trader._token_cooldown_until = 0.0
        _FakeGrowwAPI.token_calls = 0
        _FakeGrowwAPI.fail_token = True

    def test_rate_limit_message_detected(self):
        err = RuntimeError(
            "The rate limit for the Groww API has been exceeded. Please try again later."
        )
        self.assertTrue(Trader._is_groww_rate_limit_error(err))

    def test_rate_limit_skips_api_key_fallback_and_second_account(self):
        first = Trader(
            dry_run=False,
            account_id="a1",
            api_key="k",
            api_secret="s",
        )
        self.assertIsNone(first.client)
        self.assertEqual(_FakeGrowwAPI.token_calls, 1)

        second = Trader(
            dry_run=False,
            account_id="a2",
            api_key="k2",
            api_secret="s2",
        )
        self.assertIsNone(second.client)
        self.assertEqual(_FakeGrowwAPI.token_calls, 1)

        self.assertFalse(first.check_connection(log_fail=False))
        self.assertEqual(_FakeGrowwAPI.token_calls, 1)

    def test_check_connection_does_not_refetch_after_forbidden_during_cooldown(self):
        Trader._token_cooldown_until = 0.0
        _FakeGrowwAPI.fail_token = False
        trader = Trader(
            dry_run=False,
            account_id="a1",
            api_key="k",
            api_secret="s",
        )
        self.assertIsNotNone(trader.client)
        self.assertEqual(_FakeGrowwAPI.token_calls, 1)
        Trader._arm_groww_token_cooldown("test")
        self.assertFalse(trader.check_connection(log_fail=False))
        self.assertEqual(_FakeGrowwAPI.token_calls, 1)


class TestTelegramStartRetry(unittest.TestCase):
    def test_retries_then_succeeds_on_session_lock(self):
        listener = BotListener.__new__(BotListener)
        listener.session_name = "session_test"
        clients: list[MagicMock] = []

        def make():
            client = MagicMock()
            if not clients:
                client.start = AsyncMock(
                    side_effect=sqlite3.OperationalError("database is locked")
                )
            else:
                client.start = AsyncMock(return_value=None)
            client.disconnect = AsyncMock()
            client.session = MagicMock()
            clients.append(client)
            return client

        listener._make_telegram_client = make
        listener.client = None

        async def _run():
            with patch(
                "src.integrations.telegram.listener.asyncio.sleep",
                new_callable=AsyncMock,
            ), patch("app.process_lock.terminate_other_bot_processes", return_value=[]):
                await listener._start_telegram_client()

        asyncio.run(_run())
        self.assertEqual(len(clients), 2)
        self.assertEqual(clients[0].start.await_count, 1)
        self.assertEqual(clients[1].start.await_count, 1)
        clients[0].session.close.assert_called()


if __name__ == "__main__":
    unittest.main()
