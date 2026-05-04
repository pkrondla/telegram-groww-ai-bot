"""
Unit tests for the TradeState FSM (src/trade_fsm.py).
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from src.trade_fsm import TradeState, can_transition, state_from_record


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
