"""
Own-qty closes: remaining_qty / product helpers, NRML position filter,
EXIT_ALL sizing, progress without broker-net inference, re-arm size, reconcile.
"""
from __future__ import annotations

import os
import sys
import types
import unittest
from unittest.mock import MagicMock

os.environ.setdefault("TELEGRAM_API_ID", "1")
os.environ.setdefault("TELEGRAM_API_HASH", "x")
os.environ.setdefault("TELEGRAM_TARGET_CHAT_ID", "1")
os.environ.setdefault("TELEGRAM_ADMIN_ID", "1")
os.environ.setdefault("OPENAI_API_KEY", "x")

growwapi_stub = types.ModuleType("growwapi")


class _FakeAPI:
    SMART_ORDER_TYPE_OCO = "OCO"


growwapi_stub.GrowwAPI = _FakeAPI
sys.modules.setdefault("growwapi", growwapi_stub)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.trading.execution.trader import Trader  # noqa: E402


def _trader() -> Trader:
    return Trader(dry_run=True, account_id="test-account", api_key="x")


class TestOwnedRemainingQty(unittest.TestCase):
    def test_persisted_remaining_qty(self):
        t = _trader()
        rec = {"entry_qty": 75, "remaining_qty": 45, "product": "NRML"}
        self.assertEqual(t._owned_remaining_qty(rec), 45)

    def test_reconstruct_from_filled_legs_not_broker_net(self):
        t = _trader()
        rec = {
            "entry_qty": 75,
            "oco_legs": [
                {"target_index": 1, "qty": 45, "status": "FILLED"},
                {"target_index": 2, "qty": 30, "status": "ACTIVE"},
            ],
        }
        # Must NOT use a fake broker net of 150 → that would invent closed_qty.
        self.assertEqual(t._owned_remaining_qty(rec), 30)

    def test_no_remaining_no_fills_is_full_entry(self):
        t = _trader()
        rec = {"entry_qty": 75, "oco_legs": [{"target_index": 1, "qty": 75, "status": "ACTIVE"}]}
        self.assertEqual(t._owned_remaining_qty(rec), 75)

    def test_cap_owned_by_broker(self):
        t = _trader()
        rec = {"entry_qty": 75, "remaining_qty": 75}
        self.assertEqual(t._owned_open_qty_for_protection(rec, 150), 75)
        self.assertEqual(t._owned_open_qty_for_protection(rec, 40), 40)
        self.assertEqual(t._owned_open_qty_for_protection(rec, 0), 0)


class TestPositionProductFilter(unittest.TestCase):
    def test_active_positions_map_filters_mis(self):
        t = _trader()
        t.client = MagicMock()
        t.client.get_positions_for_user.return_value = {
            "positions": [
                {
                    "trading_symbol": "NIFTY25O0724500CE",
                    "net_quantity": 75,
                    "product": "MIS",
                },
                {
                    "trading_symbol": "NIFTY25O0724500CE",
                    "net_quantity": 75,
                    "product": "NRML",
                },
            ]
        }
        nrml = t._active_positions_map(product="NRML")
        self.assertEqual(list(nrml.keys()), ["NIFTY25O0724500CE"])
        self.assertEqual(t._position_product(nrml["NIFTY25O0724500CE"]), "NRML")
        self.assertEqual(t._position_quantity(nrml["NIFTY25O0724500CE"]), 75)

        mis = t._active_positions_map(product="MIS")
        self.assertEqual(t._position_product(mis["NIFTY25O0724500CE"]), "MIS")

    def test_get_open_position_defaults_to_nrml(self):
        t = _trader()
        t.client = MagicMock()
        t.client.get_positions_for_user.return_value = {
            "positions": [
                {
                    "trading_symbol": "NIFTY25O0724500CE",
                    "net_quantity": 100,
                    "product": "MIS",
                },
            ]
        }
        pos, qty = t._get_open_position_for_symbol("NIFTY25O0724500CE")
        self.assertIsNone(pos)
        self.assertEqual(qty, 0)


class TestProgressIgnoresBrokerNet(unittest.TestCase):
    def test_broker_net_extra_does_not_look_like_t1(self):
        t = _trader()
        rec = {
            "entry_qty": 75,
            "remaining_qty": 75,
            "entry_avg_price": 100.0,
            "stop_loss": 50.0,
            "sl_true": 50.0,
            "signal_targets": [120.0, 150.0],
            "sl_progress_stage": "NONE",
            "oco_legs": [
                {"target_index": 1, "qty": 45, "status": "ACTIVE", "smart_order_id": "a"},
                {"target_index": 2, "qty": 30, "status": "ACTIVE", "smart_order_id": "b"},
            ],
        }
        # Old bug: open_qty=broker_net=150 → closed_qty negative / wrong.
        # New: open_qty=owned=75 → closed=0 → NONE.
        out, stage = t._recompute_leg_progress(rec=rec, open_qty=t._owned_remaining_qty(rec))
        self.assertEqual(stage, "NONE")
        self.assertEqual(out.get("sl_progress_stage"), "NONE")

    def test_align_shrinks_remaining_when_nrml_below_owned(self):
        t = _trader()
        rec = {"entry_qty": 75, "remaining_qty": 75, "product": "NRML"}
        aligned, shrunk = t._align_remaining_to_nrml_broker(rec, 30)
        self.assertTrue(shrunk)
        self.assertEqual(aligned["remaining_qty"], 30)
        out, stage = t._recompute_leg_progress(
            rec={
                **aligned,
                "entry_avg_price": 100.0,
                "stop_loss": 50.0,
                "sl_true": 50.0,
                "signal_targets": [120.0, 150.0],
                "oco_legs": [
                    {"target_index": 1, "qty": 45, "status": "ACTIVE"},
                    {"target_index": 2, "qty": 30, "status": "ACTIVE"},
                ],
            },
            open_qty=30,
        )
        self.assertEqual(stage, "T1_HIT")


class TestBookSyncStatus(unittest.TestCase):
    def test_leftover_is_not_qty_mismatch(self):
        status = Trader._book_sync_status(
            local_open=True,
            broker_open=True,
            local_qty=75,
            broker_net_qty=150,
        )
        self.assertEqual(status, "OK_WITH_LEFTOVER")

    def test_missing_lots_is_qty_mismatch(self):
        status = Trader._book_sync_status(
            local_open=True,
            broker_open=True,
            local_qty=75,
            broker_net_qty=40,
        )
        self.assertEqual(status, "QTY_MISMATCH")

    def test_exact_match_ok(self):
        status = Trader._book_sync_status(
            local_open=True,
            broker_open=True,
            local_qty=75,
            broker_net_qty=75,
        )
        self.assertEqual(status, "OK")


class TestExitAllOwnQty(unittest.TestCase):
    def test_exit_all_sells_owned_not_broker_net(self):
        t = _trader()
        t.dry_run = False
        t.place_entry_orders = True
        t.client = MagicMock()
        t.client.SMART_ORDER_TYPE_OCO = "OCO"
        t.client.get_positions_for_user.return_value = {
            "positions": [
                {
                    "trading_symbol": "NIFTY25O0724500CE",
                    "net_quantity": 150,
                    "product": "NRML",
                    "exchange": "NSE",
                    "segment": "FNO",
                    "ltp": 100.0,
                }
            ]
        }
        t.client.cancel_smart_order.return_value = {}
        t.client.place_order.return_value = {"groww_order_id": "EXIT1"}
        t.client.get_order_status.return_value = {
            "order_status": "COMPLETE",
            "filled_quantity": 75,
            "average_fill_price": 99.0,
        }

        # After sell, broker still shows leftover 75 (other bot).
        call_count = {"n": 0}

        def _positions(**kwargs):
            call_count["n"] += 1
            # First calls (pre-sell): 150; post-sell: 75 leftover.
            qty = 150 if call_count["n"] <= 2 else 75
            return {
                "positions": [
                    {
                        "trading_symbol": "NIFTY25O0724500CE",
                        "net_quantity": qty,
                        "product": "NRML",
                        "exchange": "NSE",
                        "segment": "FNO",
                        "ltp": 100.0,
                    }
                ]
            }

        t.client.get_positions_for_user.side_effect = _positions

        symbol = "NIFTY25O0724500CE"
        t._save_all_trades(
            {
                symbol: {
                    "smart_order_id": "OCO1",
                    "entry_qty": 75,
                    "remaining_qty": 75,
                    "product": "NRML",
                    "entry_side": "BUY",
                    "entry_avg_price": 100.0,
                    "stop_loss": 50.0,
                    "sl_true": 50.0,
                    "oco_legs": [
                        {
                            "smart_order_id": "OCO1",
                            "qty": 75,
                            "target_index": 1,
                            "status": "ACTIVE",
                        }
                    ],
                }
            }
        )

        msg = t.execute_trade(
            {
                "action": "EXIT_ALL",
                "stock": symbol,
                "trading_symbol": symbol,
                "entry_range": [0, 0],
                "buy_range_min": 0.0,
                "buy_range_max": 0.0,
                "stop_loss": 0,
                "target": 0,
                "is_trade": True,
            }
        )
        self.assertIn("EXIT SUCCESS", msg)
        self.assertNotIn("EXIT PARTIAL", msg)
        place_kwargs = t.client.place_order.call_args.kwargs
        self.assertEqual(place_kwargs["quantity"], 75)
        self.assertEqual(place_kwargs["product"], "NRML")
        self.assertEqual(t._load_active_trades(), {})


class TestRearmUsesOwnedQty(unittest.TestCase):
    def test_rearm_plan_sized_to_owned_not_broker_net(self):
        t = _trader()
        rec = {
            "entry_qty": 75,
            "remaining_qty": 75,
            "lot_size": 75,
            "sl_true": 50.0,
            "stop_loss": 50.0,
            "signal_targets": [120.0],
            "sl_progress_stage": "NONE",
            "oco_legs": [
                {
                    "target_index": 1,
                    "qty": 75,
                    "status": "ACTIVE",
                    "target_signal": 120.0,
                    "smart_order_id": "old",
                }
            ],
            "protection_plan_legs": [
                {
                    "logical_leg_id": "L1",
                    "target_index": 1,
                    "allocated_qty": 75,
                    "target_signal_original": 120.0,
                    "target_signal_current": 120.0,
                    "leg_state": "ACTIVE",
                }
            ],
        }
        # Broker net 150 must not inflate the re-arm plan.
        owned = t._owned_remaining_qty(rec)
        protect = t._owned_open_qty_for_protection(rec, 150)
        self.assertEqual(owned, 75)
        self.assertEqual(protect, 75)
        plan = t._build_rearm_plan_for_open_position(rec, protect)
        self.assertTrue(plan)
        self.assertEqual(sum(int(p["qty"]) for p in plan), 75)


class TestReconcileOwnedFlat(unittest.TestCase):
    def test_reconcile_keeps_when_owned_and_nrml_open(self):
        t = _trader()
        t.client = MagicMock()
        symbol = "NIFTY25O0724500CE"
        t._save_all_trades(
            {
                symbol: {
                    "smart_order_id": "OCO1",
                    "entry_qty": 75,
                    "remaining_qty": 75,
                    "product": "NRML",
                }
            }
        )
        t.client.get_positions_for_user.return_value = {
            "positions": [
                {
                    "trading_symbol": symbol,
                    "net_quantity": 75,
                    "product": "NRML",
                },
                {
                    "trading_symbol": symbol,
                    "net_quantity": 75,
                    "product": "MIS",
                },
            ]
        }
        self.assertTrue(t.reconcile_trade_symbol(symbol))
        self.assertIn(symbol, t._load_active_trades())

    def test_reconcile_drops_when_owned_zero_despite_broker_leftover(self):
        t = _trader()
        t.client = MagicMock()
        symbol = "NIFTY25O0724500CE"
        t._save_all_trades(
            {
                symbol: {
                    "smart_order_id": "OCO1",
                    "entry_qty": 75,
                    "remaining_qty": 0,
                    "product": "NRML",
                    "entry_avg_price": 100.0,
                }
            }
        )
        t.client.get_positions_for_user.return_value = {
            "positions": [
                {
                    "trading_symbol": symbol,
                    "net_quantity": 75,
                    "product": "NRML",
                }
            ]
        }
        self.assertFalse(t.reconcile_trade_symbol(symbol))
        self.assertNotIn(symbol, t._load_active_trades())

    def test_reconcile_drops_when_nrml_flat(self):
        t = _trader()
        t.client = MagicMock()
        symbol = "NIFTY25O0724500CE"
        t._save_all_trades(
            {
                symbol: {
                    "smart_order_id": "OCO1",
                    "entry_qty": 75,
                    "remaining_qty": 75,
                    "product": "NRML",
                    "entry_avg_price": 100.0,
                }
            }
        )
        t.client.get_positions_for_user.return_value = {
            "positions": [
                {
                    "trading_symbol": symbol,
                    "net_quantity": 75,
                    "product": "MIS",
                }
            ]
        }
        self.assertFalse(t.reconcile_trade_symbol(symbol))
        self.assertNotIn(symbol, t._load_active_trades())


if __name__ == "__main__":
    unittest.main()
