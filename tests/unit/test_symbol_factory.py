"""Symbol factory: index month-only vs weekly Groww tokens."""
from __future__ import annotations

import datetime
import unittest
from unittest.mock import patch

from src.integrations.ai.assistant import Assistant


class TestIndexMonthOnlySymbolFactory(unittest.TestCase):
    def setUp(self):
        self.assistant = Assistant()

    @patch.object(Assistant, "_today_ist", return_value=datetime.date(2026, 5, 22))
    @patch.object(Assistant, "_now_ist")
    def test_may_without_day_builds_monthly_nifty(self, mock_now, _mock_today):
        mock_now.return_value = datetime.datetime(
            2026, 5, 22, 10, 6, tzinfo=Assistant._IST_TZ
        )
        data = {
            "official_symbol": "NIFTY",
            "stock": "NIFTY",
            "strike": "23850",
            "option_type": "CE",
            "expiry_day": "01",
            "expiry_month": "MAY",
            "expiry_year": "2026",
            "entry_range": [115, 125],
        }
        out = self.assistant._apply_symbol_factory(data)
        self.assertEqual(out["trading_symbol"], "NIFTY26MAY23850CE")
        self.assertEqual(out["stock"], "NIFTY26MAY23850CE")
        self.assertEqual(out.get("expiry_format"), "monthly")

    @patch.object(Assistant, "_today_ist", return_value=datetime.date(2026, 5, 22))
    @patch.object(Assistant, "_now_ist")
    def test_explicit_weekly_day_not_monthly(self, mock_now, _mock_today):
        mock_now.return_value = datetime.datetime(
            2026, 5, 22, 10, 6, tzinfo=Assistant._IST_TZ
        )
        data = {
            "official_symbol": "NIFTY",
            "strike": "23850",
            "option_type": "CE",
            "expiry_day": "26",
            "expiry_month": "MAY",
            "expiry_year": "2026",
            "entry_range": [115, 125],
        }
        out = self.assistant._apply_symbol_factory(data)
        self.assertEqual(out["trading_symbol"], "NIFTY2652623850CE")
        self.assertEqual(out.get("expiry_format"), "weekly")


class TestTraderMonthlyVariant(unittest.TestCase):
    def test_weekly_to_monthly_alpha(self):
        from src.trading.execution.trader import Trader

        monthly = Trader._index_monthly_alpha_variant("NIFTY2652623850CE")
        self.assertEqual(monthly, "NIFTY26MAY23850CE")


class TestDigitTicker360One(unittest.TestCase):
    def test_safe_underlying_keeps_leading_digits(self):
        self.assertEqual(Assistant._safe_underlying("360ONE", "1180", "CE"), "360ONE")
        self.assertEqual(Assistant._safe_underlying("360 ONE", "1180", "CE"), "360ONE")
        self.assertEqual(
            Assistant._safe_underlying("360ONE26AUG1180CE", "1180", "CE"),
            "360ONE",
        )

    def test_safe_underlying_still_strips_plain_tickers(self):
        self.assertEqual(Assistant._safe_underlying("WIPRO", "200", "PE"), "WIPRO")
        self.assertEqual(Assistant._safe_underlying("NIFTYNXT50", "70000", "CE"), "NIFTYNXT50")
        self.assertEqual(
            Assistant._safe_underlying("WIPRO26APR200PE", "200", "PE"),
            "WIPRO",
        )

    def test_factory_builds_360one_monthly(self):
        data = {
            "official_symbol": "360ONE",
            "stock": "360ONE",
            "strike": "1180",
            "option_type": "CE",
            "expiry_day": "01",
            "expiry_month": "AUG",
            "expiry_year": "2026",
            "entry_range": [19.5, 20.5],
        }
        with patch.object(Assistant, "_expiry_is_stale", return_value=False):
            out = Assistant()._apply_symbol_factory(data)
        self.assertEqual(out["trading_symbol"], "360ONE26AUG1180CE")
        self.assertEqual(out["stock"], "360ONE26AUG1180CE")

    def test_parse_360one_contract(self):
        from src.trading.execution.trader import Trader

        parsed = Trader._parse_option_symbol_parts("360ONE26AUG1180CE")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["underlying"], "360ONE")
        self.assertEqual(parsed["yy"], "26")
        self.assertEqual(parsed["month_abbr"], "AUG")
        self.assertEqual(parsed["strike"], "1180")
        self.assertEqual(parsed["option_type"], "CE")

    def test_old_one_symbol_is_not_rewritten_by_parse(self):
        from src.trading.execution.trader import Trader

        parsed = Trader._parse_option_symbol_parts("ONE26AUG1180CE")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["underlying"], "ONE")


if __name__ == "__main__":
    unittest.main()
