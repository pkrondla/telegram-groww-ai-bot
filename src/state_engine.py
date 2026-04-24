from __future__ import annotations
import re
from .models import TradeSignal
from .logger import logger


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
        logger.info(f"[STAGE 3] StateEngine.process: action={signal.action} symbol={signal.trading_symbol} "
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

        # 3. No active trade and not in pending queue — treat as a fresh BUY at the extended range.
        logger.info(
            f"[STAGE 3] EXTEND_RANGE: no active/pending trade for {symbol} — "
            f"forwarding as fresh BUY with extended range."
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

        if not symbol:
            logger.warning("[STAGE 3] UPDATE_TGT signal has no trading symbol.")
            return "❌ UPDATE_TGT: missing trading symbol."

        if not new_tgt or new_tgt <= 0:
            logger.warning("[STAGE 3] UPDATE_TGT: no valid new target price.")
            return "❌ UPDATE_TGT: could not extract a valid target price."

        logger.info(f"[STAGE 3] UPDATE_TGT requested for {symbol} — new target: {new_tgt}")

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
            legacy["target_leg_index"] = int(signal.target_leg_index or 0)
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
            item["trade_data"] = td
            updated = True
            logger.info(
                f"[STAGE 3] UPDATE_TGT: updated pending queue for {symbol}: "
                f"target {old_tgt} → {new_tgt}"
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
        if not symbol:
            return "❌ RECONCILE: missing trading symbol."

        logger.info(f"[STAGE 3] RECONCILE requested for: {symbol}")
        resolved = self._resolve_active_symbol(symbol)
        if not resolved:
            return f"⚠️ NO ACTIVE TRADE: Could not find an open position for {symbol}."

        still_active = self._trader.reconcile_trade_symbol(resolved)
        if still_active:
            return f"ℹ️ RECONCILE: {resolved} still active on broker."
        return f"✅ RECONCILE: {resolved} no longer active on broker; removed from local active book."

    def _handle_management(self, signal: TradeSignal) -> str | None:
        symbol = signal.trading_symbol
        active = self._trader._load_active_trades()
        logger.info(f"[STAGE 3] {signal.action} requested for: {symbol}")
        logger.info(f"[STAGE 3] Active book: {list(active.keys())}")

        resolved = self._resolve_active_symbol(symbol)

        if not resolved:
            logger.info(f"[STAGE 3] No active trade matched for '{symbol}' — cannot exit/trail.")
            return f"⚠️ NO ACTIVE TRADE: Could not find an open position for {symbol}."

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

            # Same reply can carry a runner target hint ("HOLD FOR X").
            # Apply target update after successful trail.
            if signal.target and signal.target > 0:
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
        leg_summary = ", ".join(f"{l.action} {l.trading_symbol}" for l in signal.legs)
        logger.info(f"[STAGE 3] Forwarding SPREAD entry ({len(signal.legs)} legs): {leg_summary}")
        return self._trader.execute_trade(signal.to_legacy_dict())

    # ------------------------------------------------------------------
    # Symbol reconciliation
    # ------------------------------------------------------------------

    @staticmethod
    def _symbol_signature(symbol: str) -> tuple[str, str, str] | None:
        s = str(symbol or "").upper()
        m = re.search(r"(\d{2,6})(CE|PE)$", s)
        if not m:
            return None
        strike = m.group(1)
        opt = m.group(2)
        prefix = s[: m.start()]
        mu = re.match(r"([A-Z&_\-]+)", prefix)
        if not mu:
            return None
        return mu.group(1), strike, opt

    def _resolve_active_symbol(self, symbol: str) -> str | None:
        active = self._trader._load_active_trades()
        if symbol in active:
            return symbol
        if not active:
            return None

        req = self._symbol_signature(symbol)
        if not req:
            return None
        ru, rs, ro = req

        candidates = []
        for k in active:
            sig = self._symbol_signature(k)
            if not sig:
                continue
            ku, ks, ko = sig
            if ks == rs and ko == ro and (ku == ru or ku.startswith(ru) or ru.startswith(ku)):
                candidates.append(k)

        return candidates[0] if len(candidates) == 1 else None
