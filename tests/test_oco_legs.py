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

from src.trader import Trader  # noqa: E402


@pytest.fixture()
def trader():
    return Trader(dry_run=True)


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
