"""
Unit tests for target progression logic:
  Trader._derive_progression_sl_true
  Trader._recompute_leg_progress
"""
import sys
import os
import types
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("TELEGRAM_API_ID",       "1")
os.environ.setdefault("TELEGRAM_API_HASH",     "x")
os.environ.setdefault("TELEGRAM_TARGET_CHAT_ID","1")
os.environ.setdefault("TELEGRAM_ADMIN_ID",     "1")
os.environ.setdefault("OPENAI_API_KEY",        "x")

growwapi_stub = types.ModuleType("growwapi")
class _FakeAPI:
    SMART_ORDER_TYPE_OCO = "OCO"
growwapi_stub.GrowwAPI = _FakeAPI
sys.modules.setdefault("growwapi", growwapi_stub)

from src.trading.execution.trader import Trader  # noqa: E402


@pytest.fixture()
def trader():
    return Trader(dry_run=True, account_id="test-account", api_key="x")


# ------------------------------------------------------------------
# _derive_progression_sl_true
# ------------------------------------------------------------------
class TestDeriveProgressionSlTrue:
    def _rec(self, sl_true=0, stop_loss=50, entry=100, signal_targets=None):
        return {
            "sl_true": sl_true,
            "stop_loss": stop_loss,
            "entry_avg_price": entry,
            "signal_targets": signal_targets or [120.0, 150.0],
        }

    def test_none_stage_returns_existing_sl(self):
        rec = self._rec(sl_true=50, stop_loss=50)
        result = Trader._derive_progression_sl_true(rec, "NONE")
        assert result == pytest.approx(50.0)

    def test_t1_hit_raises_sl_to_entry(self):
        rec = self._rec(sl_true=50, stop_loss=50, entry=100)
        result = Trader._derive_progression_sl_true(rec, "T1_HIT")
        # SL must move up to at least entry price
        assert result >= 100.0

    def test_t2_hit_raises_sl_to_t1_signal(self):
        rec = self._rec(sl_true=50, stop_loss=50, entry=100, signal_targets=[120.0, 150.0])
        result = Trader._derive_progression_sl_true(rec, "T2_HIT")
        # SL should be at least T1 signal price (120)
        assert result >= 120.0

    def test_zero_entry_t1_hit_keeps_sl(self):
        rec = self._rec(sl_true=50, stop_loss=50, entry=0)
        result = Trader._derive_progression_sl_true(rec, "T1_HIT")
        # entry=0 means cannot raise to entry; stays at sl_true
        assert result == pytest.approx(50.0)


# ------------------------------------------------------------------
# _recompute_leg_progress
# ------------------------------------------------------------------
class TestRecomputeLegProgress:
    def _make_rec(self, entry_qty, legs):
        return {
            "entry_qty": entry_qty,
            "entry_avg_price": 100.0,
            "stop_loss": 50.0,
            "sl_true": 50.0,
            "signal_targets": [120.0, 150.0],
            "oco_legs": legs,
        }

    def test_all_open_no_target_hit(self, trader):
        rec = self._make_rec(
            entry_qty=100,
            legs=[
                {"smart_order_id": "A", "qty": 60, "target_index": 1, "status": "ACTIVE"},
                {"smart_order_id": "B", "qty": 40, "target_index": 2, "status": "ACTIVE"},
            ],
        )
        result_rec, stage = trader._recompute_leg_progress(rec=rec, open_qty=100)
        assert stage == "NONE"
        assert all(l["status"] == "ACTIVE" for l in result_rec["oco_legs"])

    def test_t1_closed_gives_t1_hit(self, trader):
        rec = self._make_rec(
            entry_qty=100,
            legs=[
                {"smart_order_id": "A", "qty": 60, "target_index": 1, "status": "ACTIVE"},
                {"smart_order_id": "B", "qty": 40, "target_index": 2, "status": "ACTIVE"},
            ],
        )
        # open_qty=40 means 60 qty was closed → T1 leg filled
        result_rec, stage = trader._recompute_leg_progress(rec=rec, open_qty=40)
        assert stage == "T1_HIT"
        statuses = {l["target_index"]: l["status"] for l in result_rec["oco_legs"]}
        assert statuses[1] == "FILLED"
        assert statuses[2] == "ACTIVE"

    def test_both_closed_gives_t2_hit(self, trader):
        rec = self._make_rec(
            entry_qty=100,
            legs=[
                {"smart_order_id": "A", "qty": 60, "target_index": 1, "status": "ACTIVE"},
                {"smart_order_id": "B", "qty": 40, "target_index": 2, "status": "ACTIVE"},
            ],
        )
        result_rec, stage = trader._recompute_leg_progress(rec=rec, open_qty=0)
        assert stage == "T2_HIT"

    def test_no_legs_returns_none_stage(self, trader):
        rec = self._make_rec(entry_qty=100, legs=[])
        result_rec, stage = trader._recompute_leg_progress(rec=rec, open_qty=100)
        assert stage == "NONE"
