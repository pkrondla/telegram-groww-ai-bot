"""
Explicit trade lifecycle FSM.

States
------
PENDING  — Signal parsed; waiting for LTP to enter the entry range (pending queue).
ACTIVE   — Entry filled; OCO protection armed on broker.
EXITING  — EXIT_ALL dispatched; waiting for broker confirmation.
CLOSED   — Position confirmed flat on broker; removed from active book.

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
    if record.get("smart_order_id") or record.get("oco_legs"):
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
