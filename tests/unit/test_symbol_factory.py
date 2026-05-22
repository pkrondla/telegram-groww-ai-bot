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


if __name__ == "__main__":
    unittest.main()
