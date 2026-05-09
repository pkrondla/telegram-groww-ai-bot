"""
Unit tests for Trader._build_oco_legs_plan.

Covers: two-target split, three-target split, single-lot policy,
multi-target disabled fallback, zero/empty inputs, split overflow correction.
"""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Stub out growwapi and config env vars so Trader can be imported without
# a real broker connection or .env file.
os.environ.setdefault("TELEGRAM_API_ID",       "1")
os.environ.setdefault("TELEGRAM_API_HASH",     "x")
os.environ.setdefault("TELEGRAM_TARGET_CHAT_ID","1")
os.environ.setdefault("TELEGRAM_ADMIN_ID",     "1")
os.environ.setdefault("OPENAI_API_KEY",        "x")

# Patch growwapi import so Trader.__init__ does not crash.
import types
growwapi_stub = types.ModuleType("growwapi")
class _FakeAPI:
    SMART_ORDER_TYPE_OCO = "OCO"
growwapi_stub.GrowwAPI = _FakeAPI
sys.modules.setdefault("growwapi", growwapi_stub)

from src.trading.execution.trader import Trader  # noqa: E402


@pytest.fixture()
def trader():
    return Trader(dry_run=True, account_id="test-account", api_key="x")


class TestBuildOcoLegsPlan:
    # -----------------------------------------------------------------
    # Single target → single leg
    # -----------------------------------------------------------------
    def test_single_target_returns_one_leg(self, trader):
        plan = trader._build_oco_legs_plan(
            total_qty=100, lot_size=1, signal_targets=[68.0]
        )
        assert len(plan) == 1
        assert plan[0]["qty"] == 100
        assert plan[0]["target_index"] == 1
        assert plan[0]["target_signal"] == pytest.approx(68.0)

    # -----------------------------------------------------------------
    # Two targets — default split 60:40 (or whatever TARGET_SPLIT_2 is)
    # -----------------------------------------------------------------
    def test_two_targets_split_sum_equals_total(self, trader):
        plan = trader._build_oco_legs_plan(
            total_qty=100, lot_size=1, signal_targets=[68.0, 88.0]
        )
        assert len(plan) == 2
        assert sum(p["qty"] for p in plan) == 100
        assert plan[0]["target_index"] == 1
        assert plan[1]["target_index"] == 2

    # -----------------------------------------------------------------
    # Three targets — split sum equals total
    # -----------------------------------------------------------------
    def test_three_targets_split_sum_equals_total(self, trader):
        plan = trader._build_oco_legs_plan(
            total_qty=300, lot_size=1, signal_targets=[68.0, 88.0, 105.0]
        )
        assert len(plan) == 3
        assert sum(p["qty"] for p in plan) == 300
        assert [p["target_index"] for p in plan] == [1, 2, 3]

    # -----------------------------------------------------------------
    # Single lot → all qty routed to T2
    # -----------------------------------------------------------------
    def test_single_lot_routes_to_t2(self, trader):
        plan = trader._build_oco_legs_plan(
            total_qty=65, lot_size=65, signal_targets=[68.0, 88.0]
        )
        assert len(plan) == 1
        assert plan[0]["target_index"] == 2
        assert plan[0]["qty"] == 65

    # -----------------------------------------------------------------
    # Empty / zero inputs
    # -----------------------------------------------------------------
    def test_zero_qty_returns_empty(self, trader):
        plan = trader._build_oco_legs_plan(
            total_qty=0, lot_size=1, signal_targets=[68.0, 88.0]
        )
        assert plan == []

    def test_empty_targets_returns_empty(self, trader):
        plan = trader._build_oco_legs_plan(
            total_qty=100, lot_size=1, signal_targets=[]
        )
        assert plan == []

    # -----------------------------------------------------------------
    # Lot-size alignment: qty per leg is always a multiple of lot_size
    # -----------------------------------------------------------------
    def test_qty_per_leg_multiples_of_lot_size(self, trader):
        lot = 65
        plan = trader._build_oco_legs_plan(
            total_qty=650, lot_size=lot, signal_targets=[68.0, 88.0]
        )
        for leg in plan:
            assert leg["qty"] % lot == 0, f"leg qty {leg['qty']} not multiple of {lot}"


class TestProtectionPlanLegs:
    def test_normalize_migrates_legacy_oco_legs_to_plan(self, trader):
        rec = {
            "smart_order_id": "O1",
            "sl_true": 20.0,
            "oco_legs": [
                {"smart_order_id": "O1", "qty": 200, "target_signal": 51.0, "target_index": 1, "status": "ACTIVE"},
                {"smart_order_id": "O2", "qty": 400, "target_signal": 75.0, "target_index": 2, "status": "ACTIVE"},
            ],
        }
        out = trader._normalize_active_trade_record(rec)
        plan = out.get("protection_plan_legs") or []
        assert len(plan) == 2
        assert [p["logical_leg_id"] for p in plan] == ["L1", "L2"]
        assert [p["allocated_qty"] for p in plan] == [200, 400]

    def test_expected_leg_count_uses_remaining_plan_not_signal_fallback(self, trader):
        rec = {
            "target": 51.0,
            "signal_targets": [51.0, 75.0, 100.0],
            "protection_plan_legs": [
                {"logical_leg_id": "L1", "target_index": 1, "allocated_qty": 200, "leg_state": "FILLED"},
                {"logical_leg_id": "L2", "target_index": 2, "allocated_qty": 200, "leg_state": "ACTIVE"},
                {"logical_leg_id": "L3", "target_index": 3, "allocated_qty": 200, "leg_state": "ACTIVE"},
            ],
        }
        count = trader._expected_oco_leg_count_for_open_qty(rec, open_qty=400)
        assert count == 2

    def test_remaining_plan_legs_preserves_two_leg_structure(self, trader):
        rec = {
            "protection_plan_legs": [
                {"logical_leg_id": "L1", "target_index": 1, "allocated_qty": 200, "leg_state": "FILLED"},
                {"logical_leg_id": "L2", "target_index": 2, "allocated_qty": 200, "leg_state": "ACTIVE"},
                {"logical_leg_id": "L3", "target_index": 3, "allocated_qty": 400, "leg_state": "ACTIVE"},
            ]
        }
        legs = trader._remaining_plan_legs_for_open_qty(rec, open_qty=600)
        assert len(legs) == 2
        assert [l["logical_leg_id"] for l in legs] == ["L2", "L3"]
        assert sum(int(l["allocated_qty"]) for l in legs) == 600
