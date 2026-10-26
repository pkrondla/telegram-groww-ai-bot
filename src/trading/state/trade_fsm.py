"""
Explicit trade lifecycle FSM.

States
------
PENDING  — Signal parsed; waiting for LTP to enter the entry range (pending queue).
ACTIVE   — Entry filled on broker (OCO may or may not be armed yet).
EXITING  — EXIT_ALL dispatched; waiting for broker confirmation.
CLOSED   — This bot's owned remaining qty is 0 (not necessarily broker-symbol flat);
             removed from active book. Leftover lots may belong to another bot.

Allowed transitions per state
------------------------------
The table defines which *signal actions* are valid for each state.
Anything not listed is rejected before it reaches the executor.
"""
from __future__ import annotations
from enum import Enum


class TradeState(str, Enum):
    PENDING = "PENDING"
    ACTIVE  = "ACTIVE"
    EXITING = "EXITING"
    CLOSED  = "CLOSED"


# Actions permitted in each state.
_ALLOWED: dict[TradeState, frozenset[str]] = {
    TradeState.PENDING: frozenset({
        "BUY", "SELL",           # entry
        "EXTEND_RANGE",          # widen entry window
        "UPDATE_TGT",            # adjust target before fill
        "EXIT_ALL",              # cancel pending entry
    }),
    TradeState.ACTIVE: frozenset({
        "EXIT_ALL",
        "TRAIL_SL",
        "RESTORE_SL",
        "UPDATE_TGT",
        "RECONCILE",
        "EXTEND_RANGE",          # acknowledged-only when already active
    }),
    TradeState.EXITING: frozenset({
        "RECONCILE",             # verify closure with broker
        "EXIT_ALL",              # idempotent retry
    }),
    TradeState.CLOSED: frozenset(),
}


def can_transition(state: TradeState, action: str) -> bool:
    """Return True if *action* is permitted from *state*."""
    return action in _ALLOWED.get(state, frozenset())


def record_has_fill_evidence(record: dict | None) -> bool:
    """
    True when the active-book row represents a filled entry.

    OCO / smart_order_id is protection, not fill. A zero-SL fill still has
    entry_order_id, entry_avg_price, and opened_at_ist.
    """
    rec = record if isinstance(record, dict) else {}
    if str(rec.get("smart_order_id") or "").strip():
        return True
    oco_legs = rec.get("oco_legs")
    if isinstance(oco_legs, list) and any(
        isinstance(leg, dict) and str(leg.get("smart_order_id") or "").strip()
        for leg in oco_legs
    ):
        return True
    if str(rec.get("entry_order_id") or "").strip():
        return True
    if str(rec.get("opened_at_ist") or "").strip():
        return True
    try:
        if float(rec.get("entry_avg_price") or 0) > 0:
            return True
    except (TypeError, ValueError):
        pass
    try:
        if int(float(rec.get("entry_qty") or 0)) > 0:
            return True
    except (TypeError, ValueError):
        pass
    return False


def state_from_record(record: dict) -> TradeState:
    """
    Derive the current TradeState from a raw active-trade dict.
    Falls back gracefully for records that pre-date the FSM field.
    """
    stored = str(record.get("fsm_state") or "").upper()
    try:
        return TradeState(stored)
    except ValueError:
        pass
    # Infer from existing fields for records created before the FSM was introduced.
    if record.get("exit_in_progress"):
        return TradeState.EXITING
    if record_has_fill_evidence(record):
        return TradeState.ACTIVE
    return TradeState.PENDING


def next_state_for_action(current: TradeState, action: str) -> TradeState | None:
    """
    Return the resulting state after *action*, or None if the action does
    not change state (management actions that keep the trade ACTIVE).
    """
    if action in ("EXIT_ALL",) and current in (TradeState.ACTIVE, TradeState.PENDING):
        return TradeState.EXITING
    if action == "RECONCILE" and current == TradeState.EXITING:
        return TradeState.CLOSED
    if action in ("BUY", "SELL") and current == TradeState.PENDING:
        return TradeState.ACTIVE
    return None
