"""
Unit tests for the TradeState FSM (src/trade_fsm.py).
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from src.trading.state.trade_fsm import (
    TradeState,
    can_transition,
    record_has_fill_evidence,
    state_from_record,
)


class TestCanTransition:
    def test_active_allows_exit(self):
        assert can_transition(TradeState.ACTIVE, "EXIT_ALL")

    def test_active_allows_trail_sl(self):
        assert can_transition(TradeState.ACTIVE, "TRAIL_SL")

    def test_active_allows_update_tgt(self):
        assert can_transition(TradeState.ACTIVE, "UPDATE_TGT")

    def test_closed_rejects_all_actions(self):
        for action in ("EXIT_ALL", "TRAIL_SL", "BUY", "RECONCILE", "UPDATE_TGT"):
            assert not can_transition(TradeState.CLOSED, action)

    def test_exiting_only_allows_reconcile_and_exit(self):
        assert can_transition(TradeState.EXITING, "RECONCILE")
        assert can_transition(TradeState.EXITING, "EXIT_ALL")
        assert not can_transition(TradeState.EXITING, "TRAIL_SL")
        assert not can_transition(TradeState.EXITING, "UPDATE_TGT")

    def test_pending_allows_buy(self):
        assert can_transition(TradeState.PENDING, "BUY")

    def test_pending_rejects_trail_sl(self):
        assert not can_transition(TradeState.PENDING, "TRAIL_SL")


class TestStateFromRecord:
    def test_fsm_state_field_takes_priority(self):
        rec = {"fsm_state": "EXITING", "smart_order_id": "abc"}
        assert state_from_record(rec) == TradeState.EXITING

    def test_infers_active_from_smart_order_id(self):
        rec = {"smart_order_id": "abc"}
        assert state_from_record(rec) == TradeState.ACTIVE

    def test_infers_active_from_oco_legs(self):
        rec = {"oco_legs": [{"smart_order_id": "x"}]}
        assert state_from_record(rec) == TradeState.ACTIVE

    def test_infers_exiting_from_exit_in_progress(self):
        rec = {"exit_in_progress": True, "smart_order_id": "abc"}
        assert state_from_record(rec) == TradeState.EXITING

    def test_empty_record_infers_pending(self):
        assert state_from_record({}) == TradeState.PENDING

    def test_invalid_fsm_state_falls_back_to_inference(self):
        rec = {"fsm_state": "UNKNOWN", "smart_order_id": "abc"}
        assert state_from_record(rec) == TradeState.ACTIVE

    def test_zero_sl_fill_without_oco_is_active(self):
        rec = {
            "smart_order_id": "",
            "oco_legs": [],
            "entry_order_id": "GROWW-1",
            "entry_avg_price": 34.3,
            "entry_qty": 65,
            "opened_at_ist": "2026-09-15T11:11:51+05:30",
            "stop_loss": 0.0,
            "sl_true": 0.0,
        }
        assert record_has_fill_evidence(rec)
        assert state_from_record(rec) == TradeState.ACTIVE
        assert can_transition(TradeState.ACTIVE, "TRAIL_SL")
        assert can_transition(TradeState.ACTIVE, "EXIT_ALL")

    def test_entry_avg_price_alone_infers_active(self):
        rec = {"entry_avg_price": 12.5, "smart_order_id": "", "oco_legs": []}
        assert state_from_record(rec) == TradeState.ACTIVE

    def test_opened_at_infers_active(self):
        rec = {"opened_at_ist": "2026-09-15T11:11:51+05:30"}
        assert state_from_record(rec) == TradeState.ACTIVE

    def test_empty_oco_legs_without_fill_stays_pending(self):
        rec = {"smart_order_id": "", "oco_legs": []}
        assert not record_has_fill_evidence(rec)
        assert state_from_record(rec) == TradeState.PENDING

    def test_stored_active_wins_over_missing_oco(self):
        rec = {"fsm_state": "ACTIVE", "smart_order_id": "", "oco_legs": []}
        assert state_from_record(rec) == TradeState.ACTIVE
