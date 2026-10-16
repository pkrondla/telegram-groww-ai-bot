"""Groww TOTP login: config parsing, token exchange, 06:00 IST expiry, re-login on auth errors."""
from __future__ import annotations

import datetime
import json
import os
import sys
import time
import types
import unittest
from unittest.mock import patch

os.environ.setdefault("TELEGRAM_API_ID", "1")
os.environ.setdefault("TELEGRAM_API_HASH", "x")
os.environ.setdefault("TELEGRAM_TARGET_CHAT_ID", "1")
os.environ.setdefault("TELEGRAM_ADMIN_ID", "1")
os.environ.setdefault("OPENAI_API_KEY", "x")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytz  # noqa: E402

from src.core.config import Config  # noqa: E402
from src.trading.execution.accounts import is_valid_totp_secret, parse_accounts_json  # noqa: E402
from src.trading.execution.trader import Trader  # noqa: E402

SECRET = "JBSWY3DPEHPK3PXP"
IST = pytz.timezone("Asia/Kolkata")


class _FakeClient:
    def __init__(self, token):
        self.token = token

    def get_user_profile(self):
        if self.token == "old":
            raise RuntimeError("Access forbidden for this request.")
        return {"ok": True}

    def place(self):
        if self.token == "old":
            raise RuntimeError("403 Access forbidden for this request.")
        return f"placed-with-{self.token}"


def _fake_growwapi(tokens: list, calls: list, fail: Exception | None = None):
    mod = types.ModuleType("growwapi")

    class GrowwAPI(_FakeClient):
        @staticmethod
        def get_access_token(**kwargs):
            calls.append(kwargs)
            if fail is not None:
                raise fail
            return tokens.pop(0)

    mod.GrowwAPI = GrowwAPI
    return mod


class TestAccountsConfig(unittest.TestCase):
    def test_totp_secret_is_normalized_and_parsed(self):
        raw = json.dumps([{"account_id": "a", "api_key": "tok", "totp_secret": "jbsw y3dp-ehpk3pxp"}])
        acc = parse_accounts_json(raw)[0]
        self.assertEqual(acc.totp_secret, SECRET)
        self.assertEqual(acc.api_key, "tok")

    def test_legacy_api_secret_is_rejected(self):
        raw = json.dumps([{"account_id": "a", "api_key": "k", "api_secret": "s"}])
        with self.assertRaises(ValueError) as ctx:
            parse_accounts_json(raw)
        self.assertIn("TOTP", str(ctx.exception))

    def test_legacy_api_secret_ignored_for_disabled_account(self):
        raw = json.dumps([{"account_id": "a", "enabled": False, "api_key": "k", "api_secret": "s"}])
        self.assertEqual(len(parse_accounts_json(raw)), 1)

    def test_invalid_base32_is_rejected(self):
        self.assertFalse(is_valid_totp_secret("not base32!!"))
        self.assertTrue(is_valid_totp_secret(SECRET))
        raw = json.dumps([{"account_id": "a", "api_key": "k", "totp_secret": "not base32!!"}])
        with self.assertRaises(ValueError):
            parse_accounts_json(raw)

    def test_config_requires_totp_secret(self):
        raw = json.dumps([{"account_id": "a", "api_key": "k"}])
        with patch.object(Config, "GROWW_ACCOUNTS_JSON", raw):
            with self.assertRaises(ValueError) as ctx:
                Config.groww_accounts()
        self.assertIn("totp_secret", str(ctx.exception))

    def test_config_accepts_valid_account(self):
        raw = json.dumps([{"account_id": "a", "api_key": "k", "totp_secret": SECRET}])
        with patch.object(Config, "GROWW_ACCOUNTS_JSON", raw):
            self.assertEqual(Config.groww_accounts()[0].totp_secret, SECRET)


class TestTotpLogin(unittest.TestCase):
    def setUp(self):
        Trader._token_cooldown_until = 0.0
        self._sleep = patch("src.trading.execution.trader.time.sleep", lambda *_a, **_k: None)
        self._sleep.start()

    def tearDown(self):
        self._sleep.stop()
        Trader._token_cooldown_until = 0.0

    def _trader(self, tokens, calls, fail=None):
        with patch.dict(sys.modules, {"growwapi": _fake_growwapi(tokens, calls, fail)}):
            t = Trader(dry_run=True, account_id="a1", api_key="totp-token", totp_secret=SECRET)
        return t

    def test_login_uses_totp_code_not_secret(self):
        calls: list = []
        t = self._trader(["tok1"], calls)
        self.assertIsNotNone(t.client)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["api_key"], "totp-token")
        self.assertNotIn("secret", calls[0])
        self.assertRegex(calls[0]["totp"], r"^\d{6}$")
        self.assertEqual(t._access_token, "tok1")

    def test_missing_totp_secret_means_no_client_and_no_token_call(self):
        calls: list = []
        with patch.dict(sys.modules, {"growwapi": _fake_growwapi(["x"], calls)}):
            t = Trader(dry_run=True, account_id="a1", api_key="only-key")
        self.assertIsNone(t.client)
        self.assertEqual(calls, [])

    def test_failed_login_does_not_fall_back_to_api_key(self):
        calls: list = []
        t = self._trader([], calls, fail=RuntimeError("network down"))
        self.assertIsNone(t.client)
        self.assertIsNone(t._access_token)

    def test_invalid_totp_retries_once_with_fresh_code(self):
        calls: list = []
        err = RuntimeError("Groww API Error 400: Invalid TOTP")
        mod = _fake_growwapi([], calls)

        attempts = {"n": 0}

        def flaky(**kwargs):
            calls.append(kwargs)
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise err
            return "tok-after-retry"

        mod.GrowwAPI.get_access_token = staticmethod(flaky)
        with patch.dict(sys.modules, {"growwapi": mod}):
            t = Trader(dry_run=True, account_id="a1", api_key="k", totp_secret=SECRET)
        self.assertEqual(len(calls), 2)
        self.assertEqual(t._access_token, "tok-after-retry")

    def test_refresh_is_throttled_right_after_login(self):
        calls: list = []
        t = self._trader(["tok1", "tok2"], calls)
        with patch.dict(sys.modules, {"growwapi": _fake_growwapi(["tok2"], calls)}):
            self.assertFalse(t._refresh_access_token("test"))
        self.assertEqual(len(calls), 1)

    def test_daily_cap_blocks_extra_token_fetches(self):
        calls: list = []
        t = self._trader(["tok1", "tok2"], calls)
        t._last_token_attempt_mono = time.monotonic() - 10_000
        with patch.object(Config, "GROWW_TOKEN_DAILY_MAX", 1, create=True):
            with patch.dict(sys.modules, {"growwapi": _fake_growwapi(["tok2"], calls)}):
                self.assertFalse(t._refresh_access_token("test"))
        self.assertEqual(len(calls), 1)


class TestTokenExpiry(unittest.TestCase):
    def setUp(self):
        Trader._token_cooldown_until = 0.0
        self._sleep = patch("src.trading.execution.trader.time.sleep", lambda *_a, **_k: None)
        self._sleep.start()

    def tearDown(self):
        self._sleep.stop()

    def _trader(self, tokens, calls):
        with patch.dict(sys.modules, {"growwapi": _fake_growwapi(tokens, calls)}):
            return Trader(dry_run=True, account_id="a1", api_key="k", totp_secret=SECRET)

    def test_stale_detection_around_0600_ist(self):
        t = self._trader(["tok1"], [])
        t._token_issued_at = IST.localize(datetime.datetime(2026, 10, 15, 22, 0))
        # Issued the evening before; at 05:59 next day it is still valid, at 06:01 it is expired.
        self.assertFalse(t._token_is_stale(IST.localize(datetime.datetime(2026, 10, 16, 5, 59))))
        self.assertTrue(t._token_is_stale(IST.localize(datetime.datetime(2026, 10, 16, 6, 1))))
        # Issued after 06:00 today: valid for the rest of the day.
        t._token_issued_at = IST.localize(datetime.datetime(2026, 10, 16, 6, 5))
        self.assertFalse(t._token_is_stale(IST.localize(datetime.datetime(2026, 10, 16, 15, 0))))
        self.assertTrue(t._token_is_stale(IST.localize(datetime.datetime(2026, 10, 17, 9, 0))))

    def test_check_connection_refreshes_stale_token(self):
        calls: list = []
        t = self._trader(["old"], calls)
        t._token_issued_at = datetime.datetime.now(IST) - datetime.timedelta(days=2)
        t._last_token_attempt_mono = time.monotonic() - 10_000
        with patch.dict(sys.modules, {"growwapi": _fake_growwapi(["new"], calls)}):
            self.assertTrue(t.check_connection(log_fail=False))
        self.assertEqual(t._access_token, "new")
        self.assertEqual(len(calls), 2)


class TestReloginOnAuthError(unittest.TestCase):
    def setUp(self):
        Trader._token_cooldown_until = 0.0
        self._sleep = patch("src.trading.execution.trader.time.sleep", lambda *_a, **_k: None)
        self._sleep.start()

    def tearDown(self):
        self._sleep.stop()

    def test_groww_call_relogs_in_and_retries_on_403(self):
        calls: list = []
        with patch.dict(sys.modules, {"growwapi": _fake_growwapi(["old"], calls)}):
            t = Trader(dry_run=True, account_id="a1", api_key="k", totp_secret=SECRET)
        t._last_token_attempt_mono = time.monotonic() - 10_000
        with patch.dict(sys.modules, {"growwapi": _fake_growwapi(["new"], calls)}):
            result = t._groww_call(t.client.place)
        self.assertEqual(result, "placed-with-new")
        self.assertEqual(len(calls), 2)

    def test_groww_call_does_not_relogin_without_totp_secret(self):
        t = Trader.__new__(Trader)
        t._totp_secret_override = ""
        t.client = _FakeClient("old")
        t._access_token = "old"
        with self.assertRaises(RuntimeError):
            t._groww_call(t.client.place)


if __name__ == "__main__":
    unittest.main()
