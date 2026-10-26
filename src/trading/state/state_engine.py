from __future__ import annotations
from src.core.models import TradeSignal
from src.core.logger import logger
from src.core.config import Config
from src.core.metrics import metrics
from src.trading.state.trade_fsm import TradeState, can_transition, state_from_record


class StateEngine:
    """
    Trade state engine: the decision layer between the AI parser and the broker executor.

    Responsibilities:
    - Decide whether a parsed signal should be acted on.
    - Enforce idempotency (no duplicate entries for same symbol).
    - Reconcile exit/trail symbols against the active trade book.
    - Enforce bot-level pause/kill switch.
    - Enrich signal with resolved active-book symbol before passing to executor.

    Does NOT touch the broker or Telegram directly.
    """

    def __init__(self, trader):
        self._trader = trader
        self._account_id = str(getattr(trader, "account_id", "") or "").strip()
        if not self._account_id:
            raise ValueError("StateEngine requires trader.account_id")
        self._paused = False
        self._killed = False

    # ------------------------------------------------------------------
    # Runtime control
    # ------------------------------------------------------------------

    def pause(self):
        self._paused = True
        logger.info("StateEngine: bot PAUSED. Signals will be ignored.")

    def resume(self):
        self._paused = False
        logger.info("StateEngine: bot RESUMED.")

    def kill(self):
        self._killed = True
        logger.info("StateEngine: KILL SWITCH activated. No further orders will be placed.")

    def is_active(self) -> bool:
        return not self._killed and not self._paused

    @property
    def status(self) -> str:
        if self._killed:
            return "KILLED"
        if self._paused:
            return "PAUSED"
        return "ACTIVE"

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def process(self, signal: TradeSignal) -> str | None:
        """
        Evaluate a parsed TradeSignal and decide what to execute.
        Returns a result message string, or None if signal was suppressed.
        """
        metrics.increment("signals_received")
        logger.info(f"[STAGE 3][{self._account_id}] StateEngine.process: action={signal.action} symbol={signal.trading_symbol} "
                    f"bot_status={self.status}")

        if self._killed:
            logger.info("[STAGE 3] Signal suppressed — bot is KILLED.")
            return "🔴 Bot is KILLED. Signal ignored."

        if self._paused:
            logger.info("[STAGE 3] Signal suppressed — bot is PAUSED.")
            return "⏸️ Bot is PAUSED. Signal ignored."

        action = signal.action

        if action == "SPREAD":
            return self._handle_spread(signal)

        if action in ("EXIT_ALL", "TRAIL_SL", "RESTORE_SL"):
            return self._handle_management(signal)

        if action == "RESTORE_ALL":
            return self._handle_restore_all()

        if action == "RECONCILE":
            return self._handle_reconcile_only(signal)

        if action in ("BUY", "SELL"):
            blocked = self._blocked_entry_message(signal)
            if blocked:
                return blocked
            return self._handle_entry(signal)

        if action == "EXTEND_RANGE":
            return self._handle_extend_range(signal)

        if action == "UPDATE_TGT":
            return self._handle_update_tgt(signal)

        logger.info(f"[STAGE 3] Unhandled action '{action}' — suppressing.")
        return None

    # ------------------------------------------------------------------
    # Entry
    # ------------------------------------------------------------------

    def _handle_entry(self, signal: TradeSignal) -> str | None:
        symbol = signal.trading_symbol
        if not symbol:
            logger.warning("[STAGE 3] BUY/SELL signal has no trading symbol.")
            return "❌ Signal missing trading symbol."

        active = self._trader._load_active_trades()
        logger.info(f"[STAGE 3] Active book has {len(active)} position(s): {list(active.keys())}")

        if symbol in active:
            logger.info(f"[STAGE 3] DUPLICATE: {symbol} already in active book — skipping.")
            return f"⚠️ DUPLICATE: {symbol} already has an open position. Signal ignored."

        logger.info(f"[STAGE 3] Forwarding {signal.action} {symbol} to executor.")
        metrics.increment("trades_entered")
        return self._trader.execute_trade(signal.to_legacy_dict())

    # ------------------------------------------------------------------
    # Entry range extension
    # ------------------------------------------------------------------

    def _handle_extend_range(self, signal: TradeSignal) -> str | None:
        """
        Handle EXTEND_RANGE: the admin has widened the acceptable buy entry ceiling.

        Rules:
        - If the symbol already has an ACTIVE trade (order placed) → acknowledge only,
          no new order, no modification (position is already open).
        - If the symbol is in the PENDING queue (waiting for LTP to enter range) →
          update the pending item's entry_range max so the queue poll can now fill it.
        - If neither active nor pending → treat as a fresh BUY signal so the trade
          can be entered at the extended range.
        """
        symbol = signal.trading_symbol
        new_max = signal.entry_range[1] if signal.entry_range and len(signal.entry_range) > 1 else None

        if not symbol:
            logger.warning("[STAGE 3] EXTEND_RANGE signal has no trading symbol.")
            return "❌ EXTEND_RANGE: missing trading symbol."

        if new_max is None or new_max <= 0:
            logger.warning("[STAGE 3] EXTEND_RANGE: no valid new max price extracted.")
            return "❌ EXTEND_RANGE: could not extract a valid upper price."

        logger.info(f"[STAGE 3] EXTEND_RANGE requested for {symbol} — new max entry: {new_max}")

        # 1. Already an active (filled) trade — no action needed.
        active = self._trader._load_active_trades()
        logger.info(f"[STAGE 3] Active book: {list(active.keys())}")
        resolved = self._resolve_active_symbol(symbol)
        if resolved:
            logger.info(
                f"[STAGE 3] EXTEND_RANGE: {resolved} already has an active trade — no new order placed."
            )
            return (
                f"ℹ️ EXTEND_RANGE noted for {resolved} (new ceiling: {new_max}), "
                f"but trade is already active — no action taken."
            )

        blocked = self._blocked_entry_message(signal)
        if blocked:
            return blocked

        # 2. Symbol is in the pending queue — update the range ceiling.
        pending = self._trader._load_pending()
        updated = False
        for item in pending:
            item_symbol = item.get("trading_symbol", "")
            if item_symbol != symbol:
                continue
            td = item.get("trade_data") or {}
            old_max = (td.get("entry_range") or [0, 0])[1] if td.get("entry_range") else td.get("buy_range_max", 0)
            td["entry_range"] = [td.get("entry_range", [0, new_max])[0], new_max]
            td["buy_range_max"] = new_max
            item["trade_data"] = td
            updated = True
            logger.info(
                f"[STAGE 3] EXTEND_RANGE: updated pending queue for {symbol}: "
                f"max {old_max} → {new_max}"
            )
        if updated:
            self._trader._save_pending(pending)
            return f"✅ EXTEND_RANGE: pending entry for {symbol} updated — new ceiling: {new_max}."

        # 3. No active trade and not in pending queue.
        if not Config.EXTEND_RANGE_ALLOWS_ENTRY:
            logger.warning(
                f"[STAGE 3] EXTEND_RANGE: no active/pending trade for {symbol} — "
                f"rejected (set EXTEND_RANGE_ALLOWS_ENTRY=true to allow new BUY entries)."
            )
            return (
                f"⚠️ EXTEND_RANGE: no active or pending trade found for {symbol}. "
                f"Signal ignored. (Set EXTEND_RANGE_ALLOWS_ENTRY=true to allow entry.)"
            )
        logger.info(
            f"[STAGE 3] EXTEND_RANGE: no active/pending trade for {symbol} — "
            f"forwarding as fresh BUY with extended range (EXTEND_RANGE_ALLOWS_ENTRY=true)."
        )
        signal.action = "BUY"
        return self._handle_entry(signal)

    # ------------------------------------------------------------------
    # Target update
    # ------------------------------------------------------------------

    def _handle_update_tgt(self, signal: TradeSignal) -> str | None:
        """
        Handle UPDATE_TGT: the admin has changed the profit target for an existing trade.

        Rules:
        - If the symbol has an ACTIVE trade → forward to executor to modify the OCO target.
        - If the symbol is in the PENDING queue (not yet entered) → update the stored
          target so the OCO is placed with the correct value when the entry fills.
        - If neither → acknowledge but take no action (trade may have already exited).
        """
        symbol = signal.trading_symbol
        new_tgt = signal.target
        new_targets = list(signal.targets or [])
        clear_sl = bool(signal.clear_stop_loss)

        if not symbol:
            logger.warning("[STAGE 3] UPDATE_TGT signal has no trading symbol.")
            return "❌ UPDATE_TGT: missing trading symbol."

        if (not new_tgt or new_tgt <= 0) and new_targets:
            try:
                new_tgt = float(new_targets[0])
            except Exception:
                new_tgt = 0.0

        if not new_tgt or new_tgt <= 0:
            logger.warning("[STAGE 3] UPDATE_TGT: no valid new target price.")
            return "❌ UPDATE_TGT: could not extract a valid target price."

        logger.info(
            f"[STAGE 3] UPDATE_TGT requested for {symbol} — new target: {new_tgt}, "
            f"clear_stop_loss={clear_sl}"
        )

        # 1. Active trade — resolve symbol and forward to executor.
        logger.info(f"[STAGE 3] Active book: {list(self._trader._load_active_trades().keys())}")
        resolved = self._resolve_active_symbol(symbol)
        if resolved:
            if resolved != symbol:
                logger.info(f"[STAGE 3] Symbol reconciled: '{symbol}' → '{resolved}'")
                signal.trading_symbol = resolved
            else:
                logger.info(f"[STAGE 3] Symbol matched exactly: {resolved}")
            legacy = signal.to_legacy_dict()
            legacy["stock"] = resolved
            legacy["action"] = "UPDATE_TGT"
            legacy["target"] = new_tgt
            if new_targets:
                legacy["targets"] = new_targets[:3]
            legacy["target_leg_index"] = int(signal.target_leg_index or 0)
            legacy["clear_stop_loss"] = bool(clear_sl)
            logger.info(f"[STAGE 3] Forwarding UPDATE_TGT {resolved} to executor.")
            return self._trader.execute_trade(legacy)

        # 2. Symbol in pending queue — update stored target for when entry fills.
        pending = self._trader._load_pending()
        updated = False
        for item in pending:
            if item.get("trading_symbol", "") != symbol:
                continue
            td = item.get("trade_data") or {}
            old_tgt = td.get("target", 0)
            td["target"] = new_tgt
            if new_targets:
                td["targets"] = new_targets[:3]
            if clear_sl:
                td["stop_loss"] = 0.0
            item["trade_data"] = td
            updated = True
            logger.info(
                f"[STAGE 3] UPDATE_TGT: updated pending queue for {symbol}: "
                f"target {old_tgt} → {new_tgt}"
                + (" and cleared SL" if clear_sl else "")
            )
        if updated:
            self._trader._save_pending(pending)
            return f"✅ UPDATE_TGT: pending entry for {symbol} updated — new target: {new_tgt}."

        # 3. No active or pending trade found.
        logger.info(f"[STAGE 3] UPDATE_TGT: no active/pending trade found for {symbol} — ignoring.")
        return f"⚠️ UPDATE_TGT: no active or pending trade found for {symbol}. Signal ignored."

    # ------------------------------------------------------------------
    # Exit / Trail
    # ------------------------------------------------------------------

    def _handle_reconcile_only(self, signal: TradeSignal) -> str | None:
        symbol = signal.trading_symbol
        resolved = self._resolve_management_symbol(symbol)
        if not resolved:
            if not str(symbol or "").strip():
                return self._unresolved_management_message("RECONCILE")
            return f"⚠️ NO ACTIVE TRADE: Could not find an open position for {symbol}."

        logger.info(f"[STAGE 3] RECONCILE requested for: {resolved}")

        # Immediate broker-validated progression sync for target-hit messages.
        prog_msg = self._trader.sync_target_progression_for_symbol(resolved)
        if prog_msg:
            logger.info(f"[STAGE 3] RECONCILE progression sync: {prog_msg}")

        still_active = self._trader.reconcile_trade_symbol(resolved)
        if still_active:
            if prog_msg:
                return f"{prog_msg}\nℹ️ RECONCILE: {resolved} still active on broker."
            return f"ℹ️ RECONCILE: {resolved} still active on broker."
        return f"✅ RECONCILE: {resolved} no longer active on broker; removed from local active book."

    def _handle_management(self, signal: TradeSignal) -> str | None:
        symbol = signal.trading_symbol
        active = self._trader._load_active_trades()
        logger.info(f"[STAGE 3] {signal.action} requested for: {symbol}")
        logger.info(f"[STAGE 3] Active book: {list(active.keys())}")

        resolved = self._resolve_management_symbol(symbol)

        if not resolved:
            logger.info(f"[STAGE 3] No active trade matched for '{symbol}' — cannot exit/trail.")
            if not str(symbol or "").strip():
                return self._unresolved_management_message(signal.action)
            return f"⚠️ NO ACTIVE TRADE: Could not find an open position for {symbol}."

        # FSM guard — validate the action is permitted for the current trade state.
        rec = active.get(resolved) or {}
        trade_state = state_from_record(rec if isinstance(rec, dict) else {})
        if not can_transition(trade_state, signal.action):
            logger.warning(
                "[STAGE 3] FSM: action '%s' rejected for %s in state %s.",
                signal.action, resolved, trade_state.value,
            )
            return (
                f"⚠️ {signal.action} rejected for {resolved}: "
                f"trade is in state {trade_state.value}."
            )

        if resolved != symbol:
            logger.info(f"[STAGE 3] Symbol reconciled: '{symbol}' → '{resolved}'")
            signal.trading_symbol = resolved
        else:
            logger.info(f"[STAGE 3] Symbol matched exactly: {resolved}")

        still_active = self._trader.reconcile_trade_symbol(resolved)
        if not still_active:
            logger.info(
                f"[STAGE 3] Reconcile-before-{signal.action}: {resolved} not active on broker; "
                "dropping management action."
            )
            return f"ℹ️ {resolved} is already closed on broker. Removed from local active book."

        legacy = signal.to_legacy_dict()
        legacy["stock"] = resolved
        legacy["action"] = signal.action
        if signal.action == "TRAIL_SL":
            legacy["stop_loss"] = signal.stop_loss
            logger.info(f"[STAGE 3] Trail SL price: {signal.stop_loss}")
            logger.info(f"[STAGE 3] Forwarding TRAIL_SL {resolved} to executor.")
            trail_msg = self._trader.execute_trade(legacy)
            trail_text = str(trail_msg or "")
            trail_ok = trail_text.startswith("📈 TRAIL SUCCESS") or trail_text.startswith("🧪 [DRY RUN] TRAIL_SL")

            # Same reply can carry a runner target hint ("HOLD FOR X").
            # Apply target update only when trail SL succeeded.
            if signal.target and signal.target > 0 and trail_ok:
                update_payload = dict(legacy)
                update_payload["action"] = "UPDATE_TGT"
                update_payload["target"] = signal.target
                logger.info(
                    f"[STAGE 3] TRAIL_SL companion target detected for {resolved}: {signal.target}. "
                    "Forwarding UPDATE_TGT."
                )
                tgt_msg = self._trader.execute_trade(update_payload)
                if trail_msg and tgt_msg:
                    return f"{trail_msg}\n{tgt_msg}"
                return tgt_msg or trail_msg
            if signal.target and signal.target > 0 and not trail_ok:
                logger.info(
                    f"[STAGE 3] TRAIL_SL companion target skipped for {resolved}: "
                    f"trail did not succeed. trail_msg={trail_text!r}"
                )
            return trail_msg

        logger.info(f"[STAGE 3] Forwarding {signal.action} {resolved} to executor.")
        return self._trader.execute_trade(legacy)

    def _handle_restore_all(self) -> str | None:
        active = self._trader._load_active_trades()
        symbols = [str(s) for s in active.keys() if str(s)]
        if not symbols:
            logger.info("[STAGE 3] RESTORE_ALL requested but active book is empty.")
            return "ℹ️ RESTORE_ALL: no active trades to re-arm."

        logger.info(f"[STAGE 3] RESTORE_ALL requested for {len(symbols)} active trade(s).")
        out = []
        for symbol in symbols:
            payload = {
                "action": "RESTORE_SL",
                "stock": symbol,
                "trading_symbol": symbol,
                "entry_range": [0, 0],
                "buy_range_min": 0.0,
                "buy_range_max": 0.0,
                "stop_loss": 0,
                "target": 0,
                "is_trade": True,
            }
            try:
                msg = self._trader.execute_trade(payload)
                if msg:
                    out.append(f"{symbol}: {msg}")
            except Exception as e:
                logger.error(f"[STAGE 3] RESTORE_ALL failed for {symbol}: {e}")
                out.append(f"{symbol}: ❌ restore failed ({e})")
        if not out:
            return "ℹ️ RESTORE_ALL: no actionable result."
        return "🔁 RESTORE_ALL completed:\n" + "\n".join(out)

    # ------------------------------------------------------------------
    # Spread
    # ------------------------------------------------------------------

    def _handle_spread(self, signal: TradeSignal) -> str | None:
        if not signal.legs:
            return "❌ Spread signal has no legs."

        leg_actions = {leg.action.upper() for leg in signal.legs}
        logger.info(f"[STAGE 3] SPREAD: {len(signal.legs)} legs, actions={leg_actions}")

        # Spread exit/trail: route each leg as a management action
        if leg_actions.issubset({"EXIT_ALL", "TRAIL_SL", "RESTORE_SL"}):
            out = []
            for i, leg in enumerate(signal.legs):
                logger.info(f"[STAGE 3] Spread management leg {i+1}: {leg.action} {leg.trading_symbol}")
                leg_signal = TradeSignal(
                    action=leg.action,
                    trading_symbol=leg.trading_symbol,
                    official_symbol=leg.official_symbol,
                    strike=leg.strike,
                    option_type=leg.option_type,
                    expiry_day=leg.expiry_day,
                    expiry_month=leg.expiry_month,
                    expiry_year=leg.expiry_year,
                    entry_range=leg.entry_range,
                    stop_loss=leg.stop_loss,
                    target=leg.target,
                )
                msg = self._handle_management(leg_signal)
                if msg:
                    out.append(f"Leg {i + 1}: {msg}")
            return "\n".join(out) if out else "⚠️ Spread management had no actionable legs."

        # Normal spread entry: forward to trader
        blocked = self._blocked_entry_message(signal)
        if blocked:
            return blocked
        leg_summary = ", ".join(f"{l.action} {l.trading_symbol}" for l in signal.legs)
        logger.info(f"[STAGE 3] Forwarding SPREAD entry ({len(signal.legs)} legs): {leg_summary}")
        return self._trader.execute_trade(signal.to_legacy_dict())

    def _resolve_active_symbol(self, symbol: str) -> str | None:
        return self._trader.resolve_active_symbol(symbol)

    @staticmethod
    def unique_filled_active_symbol(active: dict | None) -> str | None:
        """Return the sole filled active-book symbol, else None."""
        filled: list[str] = []
        for sym, rec in (active or {}).items():
            name = str(sym or "").strip()
            if not name:
                continue
            st = state_from_record(rec if isinstance(rec, dict) else {})
            if st in (TradeState.ACTIVE, TradeState.EXITING):
                filled.append(name)
        if len(filled) == 1:
            return filled[0]
        return None

    def _resolve_management_symbol(self, symbol: str) -> str | None:
        resolved = self._resolve_active_symbol(symbol)
        if resolved:
            return resolved
        # Only bind to the unique book when the parser could not name a contract.
        # A concrete unmatched symbol must not silently retarget the sole open trade.
        if str(symbol or "").strip():
            return None
        active = self._trader._load_active_trades()
        unique = self.unique_filled_active_symbol(active)
        if unique:
            logger.info(
                f"[STAGE 3] Unique active-book bind: '{symbol or '(empty)'}' → '{unique}'"
            )
            return unique
        return None

    def _blocked_entry_message(self, signal: TradeSignal) -> str | None:
        """Skip new entries when that F&O segment is disabled. Management is not gated."""
        symbols: list[str] = []
        if signal.is_spread:
            for leg in signal.legs or []:
                symbols.append(str(getattr(leg, "trading_symbol", "") or ""))
                symbols.append(str(getattr(leg, "official_symbol", "") or ""))
        else:
            symbols.append(str(signal.trading_symbol or ""))
            symbols.append(str(signal.official_symbol or ""))
        reason = Config.fno_entry_block_reason(*symbols)
        if not reason:
            return None
        label = str(signal.trading_symbol or "").strip()
        if not label and signal.legs:
            label = str(getattr(signal.legs[0], "trading_symbol", "") or "").strip()
        if not label:
            label = "signal"
        logger.info(f"[STAGE 3] {reason} — skipped {label}.")
        return f"⏭️ {reason} — skipped {label}."

    def _unresolved_management_message(self, action: str) -> str:
        active = self._trader._load_active_trades()
        keys = [str(k) for k in (active or {}).keys() if str(k)]
        if not keys:
            return f"⚠️ {action}: no active trade to bind (instrument unresolved)."
        return (
            f"⚠️ {action}: instrument unresolved and {len(keys)} active trades exist "
            f"({', '.join(keys)}); not guessing."
        )
