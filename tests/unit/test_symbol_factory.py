"""Symbol factory: index month-only vs weekly Groww tokens (incl. O/N/D)."""
from __future__ import annotations

import datetime
import unittest
from unittest.mock import patch

from src.integrations.ai.assistant import Assistant
from src.trading.reference.option_symbols import (
    build_index_monthly_symbol,
    build_index_weekly_symbol,
    is_index_monthly_expiry_day,
    weekly_month_token,
)


class TestWeeklyMonthToken(unittest.TestCase):
    def test_jan_sep_digits(self):
        for m in range(1, 10):
            self.assertEqual(weekly_month_token(m), str(m))

    def test_oct_nov_dec_letters(self):
        self.assertEqual(weekly_month_token(10), "O")
        self.assertEqual(weekly_month_token(11), "N")
        self.assertEqual(weekly_month_token(12), "D")


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

    @patch.object(Assistant, "_today_ist", return_value=datetime.date(2026, 5, 1))
    @patch.object(Assistant, "_now_ist")
    def test_explicit_weekly_day_not_monthly(self, mock_now, _mock_today):
        mock_now.return_value = datetime.datetime(
            2026, 5, 1, 10, 6, tzinfo=Assistant._IST_TZ
        )
        # 5 May 2026 is a mid-month Tuesday weekly (month-end is 26 May).
        data = {
            "official_symbol": "NIFTY",
            "strike": "23850",
            "option_type": "CE",
            "expiry_day": "05",
            "expiry_month": "MAY",
            "expiry_year": "2026",
            "entry_range": [115, 125],
        }
        out = self.assistant._apply_symbol_factory(data)
        self.assertEqual(out["trading_symbol"], "NIFTY2650523850CE")
        self.assertEqual(out.get("expiry_format"), "weekly")

    @patch.object(Assistant, "_today_ist", return_value=datetime.date(2026, 9, 30))
    @patch.object(Assistant, "_now_ist")
    def test_oct_weekly_uses_letter_o(self, mock_now, _mock_today):
        mock_now.return_value = datetime.datetime(
            2026, 9, 30, 11, 20, tzinfo=Assistant._IST_TZ
        )
        data = {
            "official_symbol": "NIFTY",
            "strike": "22800",
            "option_type": "CE",
            "expiry_day": "06",
            "expiry_month": "OCT",
            "expiry_year": "2026",
            "entry_range": [100, 110],
        }
        out = self.assistant._apply_symbol_factory(data)
        self.assertEqual(out["trading_symbol"], "NIFTY26O0622800CE")
        self.assertEqual(out.get("expiry_format"), "weekly")
        self.assertNotEqual(out["trading_symbol"], "NIFTY26100622800CE")

    @patch.object(Assistant, "_today_ist", return_value=datetime.date(2026, 9, 30))
    @patch.object(Assistant, "_now_ist")
    def test_nov_weekly_uses_letter_n(self, mock_now, _mock_today):
        mock_now.return_value = datetime.datetime(
            2026, 9, 30, 11, 20, tzinfo=Assistant._IST_TZ
        )
        data = {
            "official_symbol": "NIFTY",
            "strike": "22800",
            "option_type": "CE",
            "expiry_day": "03",
            "expiry_month": "NOV",
            "expiry_year": "2026",
            "entry_range": [100, 110],
        }
        out = self.assistant._apply_symbol_factory(data)
        self.assertEqual(out["trading_symbol"], "NIFTY26N0322800CE")
        self.assertEqual(out.get("expiry_format"), "weekly")

    @patch.object(Assistant, "_today_ist", return_value=datetime.date(2026, 9, 30))
    @patch.object(Assistant, "_now_ist")
    def test_dec_weekly_uses_letter_d(self, mock_now, _mock_today):
        mock_now.return_value = datetime.datetime(
            2026, 9, 30, 11, 20, tzinfo=Assistant._IST_TZ
        )
        data = {
            "official_symbol": "NIFTY",
            "strike": "22800",
            "option_type": "CE",
            "expiry_day": "01",  # placeholder → monthly path; use day 08 for weekly
            "expiry_month": "DEC",
            "expiry_year": "2026",
            "entry_range": [100, 110],
        }
        # day 01 is placeholder → monthly; force a mid-month weekly day
        data["expiry_day"] = "08"
        out = self.assistant._apply_symbol_factory(data)
        self.assertEqual(out["trading_symbol"], "NIFTY26D0822800CE")
        self.assertEqual(out.get("expiry_format"), "weekly")

    @patch.object(Assistant, "_today_ist", return_value=datetime.date(2026, 9, 30))
    @patch.object(Assistant, "_now_ist")
    def test_nifty_month_end_day_builds_monthly(self, mock_now, _mock_today):
        mock_now.return_value = datetime.datetime(
            2026, 9, 30, 11, 20, tzinfo=Assistant._IST_TZ
        )
        # Last Tuesday of Oct 2026 = 27 Oct → monthly alpha, not NIFTY26O27…
        self.assertTrue(
            is_index_monthly_expiry_day("NIFTY", day=27, month=10, year=2026)
        )
        data = {
            "official_symbol": "NIFTY",
            "strike": "22800",
            "option_type": "CE",
            "expiry_day": "27",
            "expiry_month": "OCT",
            "expiry_year": "2026",
            "entry_range": [100, 110],
        }
        out = self.assistant._apply_symbol_factory(data)
        self.assertEqual(out["trading_symbol"], "NIFTY26OCT22800CE")
        self.assertEqual(out.get("expiry_format"), "monthly")

    @patch.object(Assistant, "_today_ist", return_value=datetime.date(2026, 9, 30))
    @patch.object(Assistant, "_now_ist")
    def test_sensex_month_end_day_builds_monthly(self, mock_now, _mock_today):
        mock_now.return_value = datetime.datetime(
            2026, 9, 30, 11, 20, tzinfo=Assistant._IST_TZ
        )
        data = {
            "official_symbol": "SENSEX",
            "strike": "66000",
            "option_type": "CE",
            "expiry_day": "29",
            "expiry_month": "OCT",
            "expiry_year": "2026",
            "entry_range": [100, 110],
        }
        out = self.assistant._apply_symbol_factory(data)
        self.assertEqual(out["trading_symbol"], "SENSEX26OCT66000CE")
        self.assertEqual(out.get("expiry_format"), "monthly")


class TestTraderWeeklyParseAndVariants(unittest.TestCase):
    def _trader(self):
        from src.trading.execution.trader import Trader

        return Trader(dry_run=True, account_id="test", api_key="x")

    def test_parse_oct_letter_weekly(self):
        from src.trading.execution.trader import Trader

        parsed = Trader._parse_option_symbol_parts("NIFTY26O0622800CE")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["underlying"], "NIFTY")
        self.assertEqual(parsed["month_num"], 10)
        self.assertEqual(parsed["day"], 6)
        self.assertEqual(parsed["strike"], "22800")
        self.assertEqual(parsed["expiry"], datetime.date(2026, 10, 6))

    def test_parse_legacy_numeric_oct_weekly(self):
        from src.trading.execution.trader import Trader

        parsed = Trader._parse_option_symbol_parts("NIFTY26100622800CE")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["expiry"], datetime.date(2026, 10, 6))
        self.assertEqual(parsed["month_num"], 10)

    def test_weekly_encoding_variants_include_letter_o(self):
        t = self._trader()
        variants = t._index_weekly_symbol_encoding_variants("NIFTY26100622800CE")
        self.assertIn("NIFTY26O0622800CE", variants)

    def test_true_weekly_variants_exclude_blind_monthly(self):
        t = self._trader()
        # No Groww row → same-expiry monthly must not be appended.
        with patch.object(t, "_get_instrument", return_value=None):
            variants = t._variant_list("NIFTY26O0622800CE")
        self.assertIn("NIFTY26O0622800CE", variants)
        self.assertNotIn("NIFTY26OCT22800CE", variants)

    def test_same_expiry_monthly_promoted_when_instrument_matches(self):
        t = self._trader()
        # Mid-month weekly must not promote to monthly even if monthly instrument exists.
        with patch.object(
            t,
            "_get_instrument",
            return_value={"expiry_date": "2026-11-23", "segment": "FNO", "exchange": "NSE"},
        ):
            variants = t._variant_list("NIFTY26N0322800CE")
            self.assertNotIn("NIFTY26NOV22800CE", variants)

        with patch.object(
            t,
            "_get_instrument",
            return_value={"expiry_date": "2026-11-23", "segment": "FNO", "exchange": "NSE"},
        ):
            promoted = t._same_expiry_monthly_variant("NIFTY26N2322800CE")
        self.assertEqual(promoted, "NIFTY26NOV22800CE")


class TestTraderMonthlyVariant(unittest.TestCase):
    def test_weekly_to_monthly_alpha(self):
        from src.trading.execution.trader import Trader

        monthly = Trader._index_monthly_alpha_variant("NIFTY2652623850CE")
        self.assertEqual(monthly, "NIFTY26MAY23850CE")

    def test_builders(self):
        self.assertEqual(
            build_index_weekly_symbol("NIFTY", "26", 10, 6, "22800", "CE"),
            "NIFTY26O0622800CE",
        )
        self.assertEqual(
            build_index_monthly_symbol("NIFTY", "26", 10, "22800", "CE"),
            "NIFTY26OCT22800CE",
        )


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
