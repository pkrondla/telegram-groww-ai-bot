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
    # Single lot → all qty routed to T1
    # -----------------------------------------------------------------
    def test_single_lot_routes_to_t1(self, trader):
        plan = trader._build_oco_legs_plan(
            total_qty=65, lot_size=65, signal_targets=[68.0, 88.0]
        )
        assert len(plan) == 1
        assert plan[0]["target_index"] == 1
        assert plan[0]["target_signal"] == pytest.approx(68.0)
        assert plan[0]["qty"] == 65

    def test_single_lot_three_targets_still_routes_to_t1(self, trader):
        plan = trader._build_oco_legs_plan(
            total_qty=75, lot_size=75, signal_targets=[41.0, 63.0, 90.0]
        )
        assert len(plan) == 1
        assert plan[0]["target_index"] == 1
        assert plan[0]["target_signal"] == pytest.approx(41.0)
        assert plan[0]["qty"] == 75

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

    def test_remaining_plan_excludes_t1_when_oco_leg_filled_but_plan_still_active(self, trader):
        """Post-T1: open 750 should map to T2=500 and T3=250, not 375/188 split across 3 legs."""
        rec = {
            "entry_qty": 1000,
            "lot_size": 250,
            "signal_targets": [41.0, 63.0, 90.0],
            "entry_avg_price": 36.3,
            "sl_true": 36.3,
            "sl_progress_stage": "T1_HIT",
            "oco_legs": [
                {
                    "smart_order_id": "T1_DONE",
                    "qty": 250,
                    "target_signal": 41.0,
                    "target_index": 1,
                    "status": "FILLED",
                },
                {
                    "smart_order_id": "T2_LIVE",
                    "qty": 500,
                    "target_signal": 63.0,
                    "target_index": 2,
                    "status": "ACTIVE",
                },
                {
                    "smart_order_id": "T3_LIVE",
                    "qty": 250,
                    "target_signal": 90.0,
                    "target_index": 3,
                    "status": "ACTIVE",
                },
            ],
            "protection_plan_legs": [
                {"logical_leg_id": "L1", "target_index": 1, "allocated_qty": 250, "leg_state": "ACTIVE"},
                {"logical_leg_id": "L2", "target_index": 2, "allocated_qty": 500, "leg_state": "ACTIVE"},
                {"logical_leg_id": "L3", "target_index": 3, "allocated_qty": 250, "leg_state": "ACTIVE"},
            ],
        }
        synced = trader._sync_protection_plan_from_progress(rec, open_qty=750)
        assert synced["protection_plan_legs"][0]["leg_state"] == "FILLED"
        remaining = trader._remaining_plan_legs_for_open_qty(synced, open_qty=750)
        assert len(remaining) == 2
        assert [int(l["target_index"]) for l in remaining] == [2, 3]
        assert [int(l["allocated_qty"]) for l in remaining] == [500, 250]
        assert trader._expected_oco_leg_count_for_open_qty(synced, open_qty=750) == 2

    def test_remaining_plan_uses_entry_minus_open_when_plan_still_active(self, trader):
        """After T1 exit: entry 1000, open 750 — exclude T1 even if plan rows are all ACTIVE."""
        rec = {
            "entry_qty": 1000,
            "protection_plan_legs": [
                {"logical_leg_id": "L1", "target_index": 1, "allocated_qty": 250, "leg_state": "ACTIVE"},
                {"logical_leg_id": "L2", "target_index": 2, "allocated_qty": 500, "leg_state": "ACTIVE"},
                {"logical_leg_id": "L3", "target_index": 3, "allocated_qty": 250, "leg_state": "ACTIVE"},
            ],
            "oco_legs": [
                {"smart_order_id": "ONLY_LIVE", "qty": 375, "target_index": 2, "status": "ACTIVE"},
            ],
        }
        assert trader._filled_target_indices(rec, open_qty=750) == {1}
        remaining = trader._remaining_plan_legs_for_open_qty(rec, open_qty=750)
        assert [int(l["allocated_qty"]) for l in remaining] == [500, 250]
        assert trader._expected_oco_leg_count_for_open_qty(rec, open_qty=750) == 2

    def test_filled_target_indices_from_plan_sum_minus_open_qty(self, trader):
        """Without entry_qty: plan 1000 vs open 750 implies T1 (250) exited."""
        rec = {
            "protection_plan_legs": [
                {"logical_leg_id": "L1", "target_index": 1, "allocated_qty": 250, "leg_state": "ACTIVE"},
                {"logical_leg_id": "L2", "target_index": 2, "allocated_qty": 500, "leg_state": "ACTIVE"},
                {"logical_leg_id": "L3", "target_index": 3, "allocated_qty": 250, "leg_state": "ACTIVE"},
            ],
        }
        assert trader._filled_target_indices(rec, open_qty=750) == {1}

    def test_finalize_rearm_drops_non_lot_multiple_qty(self, trader):
        legs = [
            {"target_index": 1, "qty": 187},
            {"target_index": 2, "qty": 375},
            {"target_index": 3, "qty": 188},
        ]
        out = trader._finalize_rearm_leg_qtys(legs, total_qty=750, lot_size=250)
        assert out == []

    def test_build_rearm_plan_post_t1_two_legs_lot_aligned(self, trader):
        rec = {
            "lot_size": 250,
            "entry_qty": 1000,
            "entry_avg_price": 36.3,
            "signal_targets": [41.0, 63.0, 90.0],
            "protection_plan_legs": [
                {"logical_leg_id": "L1", "target_index": 1, "allocated_qty": 250, "leg_state": "ACTIVE"},
                {"logical_leg_id": "L2", "target_index": 2, "allocated_qty": 500, "leg_state": "ACTIVE"},
                {"logical_leg_id": "L3", "target_index": 3, "allocated_qty": 250, "leg_state": "ACTIVE"},
            ],
        }
        plan = trader._build_rearm_plan_for_open_position(rec, open_qty=750)
        assert [int(l["qty"]) for l in plan] == [500, 250]
        assert all(int(l["qty"]) % 250 == 0 for l in plan)

    def test_broker_only_375_oco_does_not_force_three_leg_split(self, trader):
        """Corrupted book: single live 375 OCO must not imply 3-leg 375/188 re-arm plan."""
        rec = {
            "entry_qty": 1000,
            "entry_avg_price": 36.3,
            "signal_targets": [41.0, 63.0, 90.0],
            "protection_plan_legs": [
                {"logical_leg_id": "L1", "target_index": 1, "allocated_qty": 250, "leg_state": "ACTIVE"},
                {"logical_leg_id": "L2", "target_index": 2, "allocated_qty": 500, "leg_state": "ACTIVE"},
                {"logical_leg_id": "L3", "target_index": 3, "allocated_qty": 250, "leg_state": "ACTIVE"},
            ],
            "oco_legs": [
                {"smart_order_id": "BAD375", "qty": 375, "target_index": 2, "status": "ACTIVE"},
            ],
        }
        synced = trader._sync_protection_plan_from_progress(rec, open_qty=750)
        remaining = trader._remaining_plan_legs_for_open_qty(synced, open_qty=750)
        assert [int(l["allocated_qty"]) for l in remaining] == [500, 250]


class TestPositionQuantity:
  def test_prefers_net_quantity_when_day_quantity_stale(self, trader):
      """Flat broker row may still carry day buy qty in ``quantity``."""
      pos = {"trading_symbol": "FOO", "quantity": 2000, "net_quantity": 0}
      assert trader._position_quantity(pos) == 0

  def test_uses_quantity_when_net_missing(self, trader):
      pos = {"trading_symbol": "FOO", "quantity": 500}
      assert trader._position_quantity(pos) == 500


class TestBookSyncStatus:
    def test_stale_local(self, trader):
        assert trader._book_sync_status(
            local_open=True, broker_open=False, local_qty=100, broker_net_qty=0
        ) == "STALE_LOCAL"

    def test_broker_only(self, trader):
        assert trader._book_sync_status(
            local_open=False, broker_open=True, local_qty=0, broker_net_qty=50
        ) == "BROKER_ONLY"

    def test_qty_mismatch(self, trader):
        assert trader._book_sync_status(
            local_open=True, broker_open=True, local_qty=100, broker_net_qty=50
        ) == "QTY_MISMATCH"


class TestBrokerLocalDiff:
    def test_diff_flags_stale_local(self, trader, monkeypatch):
        trader.client = object()
        monkeypatch.setattr(
            "src.trading.execution.trader.db.get_leg_exits_for_range",
            lambda account_id, start, end: [],
        )
        trader._load_active_trades = lambda: {
            "FOO26MAY100CE": {
                "entry_side": "BUY",
                "entry_qty": 100,
                "entry_avg_price": 10.0,
                "parent_trade_id": "abc12345",
            }
        }
        trader._list_fno_positions = lambda: []
        text = trader.build_broker_local_diff_text("day")
        assert "STALE_LOCAL" in text
        assert "FOO26MAY100CE" in text
        assert "Stale local" in text and "1" in text


class TestReconcileFillFallback:
    def test_infers_exit_from_realized_pnl_when_avg_missing(self, trader):
        class _Client:
            def get_positions_for_user(self, segment="FNO"):
                return {
                    "positions": [
                        {
                            "trading_symbol": "FOO26MAY100CE",
                            "day_sell_qty": 200,
                            "realized_pnl": -1200.0,
                        }
                    ]
                }

        trader.client = _Client()
        rec = {
            "entry_side": "BUY",
            "entry_avg_price": 10.0,
            "entry_qty": 200,
        }
        out = trader._reconcile_exit_fill_from_positions(
            symbol="FOO26MAY100CE",
            active_rec=rec,
        )
        assert out is not None
        assert out["source"] == "broker_positions_realized"
        assert out["exit_qty"] == 200
        assert out["pnl_inr"] == pytest.approx(-1200.0)
        assert out["exit_price"] == pytest.approx(4.0)

    def test_uses_total_pnl_field_for_fallback(self, trader):
        class _Client:
            def get_positions_for_user(self, segment="FNO"):
                return {
                    "positions": [
                        {
                            "trading_symbol": "BAR26MAY100PE",
                            "day_buy_qty": 100,
                            "total_pnl": 500.0,
                        }
                    ]
                }

        trader.client = _Client()
        rec = {
            "entry_side": "SELL",
            "entry_avg_price": 20.0,
            "entry_qty": 100,
        }
        out = trader._reconcile_exit_fill_from_positions(
            symbol="BAR26MAY100PE",
            active_rec=rec,
        )
        assert out is not None
        assert out["source"] == "broker_positions_realized"
        assert out["exit_qty"] == 100
        assert out["pnl_inr"] == pytest.approx(500.0)
        assert out["exit_price"] == pytest.approx(15.0)

    def test_backfill_uses_prefetched_broker_realized_metrics(self, trader, monkeypatch):
        trader.client = object()
        pending_row = {
            "id": 99,
            "symbol": "BAZ26MAY100CE",
            "side": "BUY",
            "entry_price": 10.0,
            "qty": 100,
            "data": {},
            "close_reason": "RECONCILE_FLAT",
        }
        updates = []

        monkeypatch.setattr(
            "src.trading.execution.trader.db.get_pending_leg_exits",
            lambda account_id: [pending_row],
        )

        def _capture_update(leg_exit_id, row, account_id):
            updates.append((leg_exit_id, row, account_id))

        monkeypatch.setattr(
            "src.trading.execution.trader.db.update_leg_exit_fill",
            _capture_update,
        )
        # Force the primary inference path to fail so the prefetch fallback is exercised.
        trader._reconcile_exit_fill_from_positions = lambda symbol, active_rec: {}
        trader._list_fno_positions = lambda: [
            {
                "trading_symbol": "BAZ26MAY100CE",
                "net_quantity": 0,
                "realized_pnl": 200.0,
            }
        ]

        updated = trader._backfill_pending_leg_exits()
        assert updated == 1
        assert len(updates) == 1
        _, row, _ = updates[0]
        assert row["pnl_pending"] is False
        assert row["exit_fill_source"] == "broker_positions_realized_metrics"
        assert row["pnl_inr"] == pytest.approx(200.0)
        assert row["exit_price"] == pytest.approx(12.0)
