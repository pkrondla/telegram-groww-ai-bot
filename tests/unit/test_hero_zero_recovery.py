"""Regression tests for zero-SL fill management (HEROZERO / nested replies)."""
from __future__ import annotations

import datetime
import os
import sys
import types
from unittest.mock import MagicMock, patch

os.environ.setdefault("TELEGRAM_API_ID", "1")
os.environ.setdefault("TELEGRAM_API_HASH", "x")
os.environ.setdefault("TELEGRAM_TARGET_CHAT_ID", "1")
os.environ.setdefault("TELEGRAM_ADMIN_ID", "1")
os.environ.setdefault("OPENAI_API_KEY", "x")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

_groww = types.ModuleType("growwapi")


class _FakeGrowwAPI:
    SMART_ORDER_TYPE_OCO = "OCO"
    SEGMENT_FNO = "FNO"

    @classmethod
    def get_access_token(cls, **_kwargs):
        return "ok-token"

    def __init__(self, token):
        self.token = token


_groww.GrowwAPI = _FakeGrowwAPI
sys.modules.setdefault("growwapi", _groww)

from src.integrations.ai.assistant import Assistant  # noqa: E402
from src.integrations.telegram.listener import collapse_reply_ancestor_texts  # noqa: E402
from src.trading.execution.trader import Trader  # noqa: E402
from src.trading.state.state_engine import StateEngine  # noqa: E402
from src.trading.state.trade_fsm import TradeState  # noqa: E402


ZERO_SL_FILL = {
    "smart_order_id": "",
    "oco_legs": [],
    "entry_order_id": "GROWW-1",
    "entry_avg_price": 34.3,
    "entry_qty": 65,
    "opened_at_ist": "2026-09-15T11:11:51+05:30",
    "stop_loss": 0.0,
    "sl_true": 0.0,
    "signal_targets": [54.0, 72.0, 95.0],
    "fsm_state": "ACTIVE",
}


class TestReplyChainCollapse:
    def test_single_parent_unchanged(self):
        assert collapse_reply_ancestor_texts(["BUY NIFTY 23300 PE"]) == "BUY NIFTY 23300 PE"

    def test_nested_parents_immediate_first(self):
        text = collapse_reply_ancestor_texts(
            [
                "TARGET 2 : 72 HITS\nTSL TO 54",
                "#HEROZERO\nBUY NIFTY 23300 PE TODAY'S EXPIRY AT CMP 30-40\nSL : 0",
            ]
        )
        assert text.startswith("TARGET 2")
        assert "BUY NIFTY 23300 PE" in text

    def test_skips_blank_and_duplicates(self):
        assert collapse_reply_ancestor_texts(["  ", "A", "A", ""]) == "A"

    def test_empty(self):
        assert collapse_reply_ancestor_texts([]) == ""
        assert collapse_reply_ancestor_texts(None) == ""


class TestUniqueActiveBind:
    def test_binds_single_filled_trade(self):
        active = {"NIFTY2691523300PE": ZERO_SL_FILL}
        assert StateEngine.unique_filled_active_symbol(active) == "NIFTY2691523300PE"

    def test_none_when_multiple(self):
        active = {
            "NIFTY2691523300PE": ZERO_SL_FILL,
            "BANKNIFTY2691552000CE": dict(ZERO_SL_FILL),
        }
        assert StateEngine.unique_filled_active_symbol(active) is None

    def test_none_when_empty(self):
        assert StateEngine.unique_filled_active_symbol({}) is None

    def test_ignores_true_pending_records(self):
        active = {
            "NIFTY2691523300PE": {"smart_order_id": "", "oco_legs": [], "fsm_state": "PENDING"},
        }
        assert StateEngine.unique_filled_active_symbol(active) is None


class TestBookBindPayload:
    def test_exit_all_placeholder(self):
        payload = Assistant._management_book_bind_payload("EXIT_ALL")
        assert payload["action"] == "EXIT_ALL"
        assert payload["is_trade"] is True
        assert payload["bind_unique_active"] is True
        assert payload["stock"] == ""
        assert payload["trading_symbol"] == ""

    def test_trail_placeholder_keeps_sl(self):
        payload = Assistant._management_book_bind_payload("TRAIL_SL", stop_loss=30, target=95)
        assert payload["stop_loss"] == 30
        assert payload["target"] == 95


class TestPendingLegAbandon:
    def test_abandon_on_max_attempts(self):
        reason = Trader._pending_leg_exit_abandon_reason(
            {"trade_date": "2026-09-15"},
            {"reconcile_attempts": 30},
            now_ist=datetime.date(2026, 9, 15),
            max_attempts=30,
            max_age_days=3,
        )
        assert reason.startswith("max_attempts=")

    def test_abandon_on_age(self):
        reason = Trader._pending_leg_exit_abandon_reason(
            {"trade_date": "2026-05-21"},
            {"reconcile_attempts": 2},
            now_ist=datetime.date(2026, 9, 15),
            max_attempts=30,
            max_age_days=3,
        )
        assert reason.startswith("max_age_days=")

    def test_keep_recent_low_attempts(self):
        reason = Trader._pending_leg_exit_abandon_reason(
            {"trade_date": "2026-09-15"},
            {"reconcile_attempts": 5},
            now_ist=datetime.date(2026, 9, 15),
            max_attempts=30,
            max_age_days=3,
        )
        assert reason == ""


class TestUnprotectedFillRearm:
    def _trader(self) -> Trader:
        with patch.object(Trader, "_initialize_groww_client"):
            t = Trader(dry_run=True, account_id="test")
        t.client = None
        return t

    def test_zero_sl_fill_is_unprotected(self):
        t = self._trader()
        assert t._is_unprotected_zero_sl_fill(ZERO_SL_FILL) is True

    def test_protected_fill_is_not_unprotected(self):
        t = self._trader()
        rec = dict(ZERO_SL_FILL)
        rec["smart_order_id"] = "OCO-1"
        rec["sl_true"] = 30.0
        rec["stop_loss"] = 30.0
        assert t._is_unprotected_zero_sl_fill(rec) is False

    def test_skip_rearm_alerts_once(self):
        t = self._trader()
        t._persist_active_trade_record = MagicMock()
        first = t._skip_rearm_unprotected_fill(
            source="schedule", symbol="NIFTY2691523300PE", rec=ZERO_SL_FILL
        )
        assert first["success"] is True
        assert first["attempted"] is True
        assert first["line"]
        rec2 = dict(ZERO_SL_FILL)
        rec2["unprotected_fill_alerted"] = True
        second = t._skip_rearm_unprotected_fill(
            source="schedule", symbol="NIFTY2691523300PE", rec=rec2
        )
        assert second["attempted"] is False
        assert second["line"] is None

    def test_trail_create_protection_dry_run(self):
        t = self._trader()
        t.place_entry_orders = False
        t._update_active_trade_levels = MagicMock()
        t._ltp_suffix_for_symbol = lambda _s: ""
        msg = t._trail_sl_create_protection("NIFTY2691523300PE", 30.0, ZERO_SL_FILL)
        assert msg.startswith("🧪 [DRY RUN] TRAIL_SL")
        t._update_active_trade_levels.assert_called_once()


class TestParserBookBind:
    def test_exit_all_without_instrument_emits_bind_signal(self):
        a = Assistant()
        a._llm_parse_forced = MagicMock(
            return_value={
                "official_symbol": "",
                "stock": "",
                "strike": "",
                "option_type": "",
                "expiry_day": "01",
                "expiry_month": "SEP",
                "expiry_year": "2026",
                "entry_range": [0, 0],
                "stop_loss": 54,
                "target": 0,
                "is_trade": False,
                "action": "EXIT_ALL",
            }
        )
        raw = a.parse_signal(
            {
                "is_reply": True,
                "parent_text": "TARGET 2 : 72 HITS\nTSL TO 54\nHOLD FOR 95+",
                "child_text": "TARGET 3 : 95 HITS\n\nClose the trade.",
                "full_text": (
                    "[CONTEXT: TARGET 2 : 72 HITS\nTSL TO 54\nHOLD FOR 95+] "
                    "TARGET 3 : 95 HITS\n\nClose the trade."
                ),
            }
        )
        assert raw is not None
        assert raw["action"] == "EXIT_ALL"
        assert raw.get("bind_unique_active") is True
        assert not raw.get("stock")


class TestStateEngineUniqueTrail:
    def test_empty_symbol_trails_unique_active_fill(self):
        from src.core.models import TradeSignal

        class _FakeTrader:
            account_id = "t"

            def __init__(self):
                self.active = {"NIFTY2691523300PE": dict(ZERO_SL_FILL)}
                self.executed = []

            def _load_active_trades(self):
                return self.active

            def resolve_active_symbol(self, symbol):
                s = str(symbol or "").upper().strip()
                return s if s in self.active else None

            def reconcile_trade_symbol(self, _symbol):
                return True

            def execute_trade(self, payload):
                self.executed.append(payload)
                return "📈 TRAIL SUCCESS: Created protection"

        trader = _FakeTrader()
        engine = StateEngine(trader)
        signal = TradeSignal(
            action="TRAIL_SL",
            trading_symbol="",
            stop_loss=30.0,
            target=0.0,
        )
        msg = engine.process(signal)
        assert msg and "TRAIL SUCCESS" in msg
        assert trader.executed
        assert trader.executed[0]["stock"] == "NIFTY2691523300PE"
        assert trader.executed[0]["action"] == "TRAIL_SL"
        assert trader.executed[0]["stop_loss"] == 30.0

    def test_unmatched_concrete_symbol_does_not_bind(self):
        from src.core.models import TradeSignal

        class _FakeTrader:
            account_id = "t"

            def _load_active_trades(self):
                return {"NIFTY2691523300PE": dict(ZERO_SL_FILL)}

            def resolve_active_symbol(self, symbol):
                return None

            def reconcile_trade_symbol(self, _symbol):
                return True

            def execute_trade(self, payload):
                raise AssertionError("must not execute on unmatched concrete symbol")

        engine = StateEngine(_FakeTrader())
        msg = engine.process(
            TradeSignal(action="EXIT_ALL", trading_symbol="BANKNIFTY2691552000CE")
        )
        assert msg and "NO ACTIVE TRADE" in msg

    def test_trail_not_rejected_as_pending_for_zero_sl_fill(self):
        from src.core.models import TradeSignal

        rec = dict(ZERO_SL_FILL)
        rec.pop("fsm_state", None)
        rec["smart_order_id"] = ""
        rec["oco_legs"] = []

        class _FakeTrader:
            account_id = "t"

            def __init__(self):
                self.active = {"NIFTY2691523300PE": rec}

            def _load_active_trades(self):
                return self.active

            def resolve_active_symbol(self, symbol):
                return "NIFTY2691523300PE" if str(symbol or "") == "NIFTY2691523300PE" else None

            def reconcile_trade_symbol(self, _symbol):
                return True

            def execute_trade(self, payload):
                return "📈 TRAIL SUCCESS: Created protection"

        engine = StateEngine(_FakeTrader())
        signal = TradeSignal(
            action="TRAIL_SL",
            trading_symbol="NIFTY2691523300PE",
            stop_loss=30.0,
        )
        msg = engine.process(signal)
        assert msg and "TRAIL SUCCESS" in msg
        assert "PENDING" not in str(msg)
