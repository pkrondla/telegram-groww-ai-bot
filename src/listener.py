from telethon import TelegramClient, events
import asyncio
import datetime
import re
import pytz
from .config import Config
from .logger import logger
from .assistant import Assistant
from .trader import Trader
from .state_engine import StateEngine
from .notifier import Notifier
from .symbol_manager import symbol_manager


# Admin override commands (sent by admin to their own chat or bot)
_OVERRIDE_COMMANDS = {
    "PAUSE":    "pause",
    "RESUME":   "resume",
    "KILL":     "kill",
    "STATUS":   "status",
    "TRADES":   "trades",
    "ACTIVE TRADES": "trades",
    "POSITIONS": "trades",
    "SIZING":   "sizing_status",
    "SIZING MODE": "sizing_status",
    "REARM":   "rearm",
    "HELP":     "help",
    "DRYRUN ON":  "dryrun_on",
    "DRYRUN OFF": "dryrun_off",
    "CLEAR PENDING": "clear_pending",
}
_SIZE_OVERRIDE_FIXED_PHRASES = (
    "RISKY TRADE",
    "HIGH RISK",
    "TRADE LESS QTY",
    "LESS QTY",
    "LOW QTY",
    "SMALL QTY",
    "REDUCE QTY",
    "TRADE SMALL",
)
_INDEX_UNDERLYINGS = frozenset({"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"})


def _startup_command_reference() -> str:
    """Compact admin command/help text shown in startup notification."""
    return "Send HELP to view all available commands."


class BotListener:
    """
    Connects to Telegram via Telethon and coordinates the full pipeline:
      Telegram -> Message listener -> AI parser -> State engine -> Trade executor -> Notifier
    """

    def __init__(self):
        self.api_id = Config.API_ID
        self.api_hash = Config.API_HASH
        self.session_name = Config.SESSION_NAME
        self.target_chat_id = Config.TARGET_CHAT_ID

        self.client = TelegramClient(
            self.session_name,
            self.api_id,
            self.api_hash,
            connection_retries=-1,   # retry forever on disconnect (covers WinError 121)
            retry_delay=5,           # wait 5s between reconnect attempts
            auto_reconnect=True,     # re-establish MTProto session automatically
            flood_sleep_threshold=60,
        )
        self.assistant = Assistant()
        self.trader = Trader(dry_run=Config.DRY_RUN)
        self.state_engine = StateEngine(self.trader)
        self.notifier = Notifier()
        self._pending_size_override_mode = ""
        self._pending_size_override_reason = ""
        self._pending_size_override_scope = "ALL"
        self._pending_size_override_until_ts = 0.0
        self._groww_ready_last_state: bool | None = None

    # ------------------------------------------------------------------
    # Background tasks
    # ------------------------------------------------------------------

    async def _pending_entry_watcher(self):
        if not Config.ENTRY_QUEUE_ENABLED:
            return
        interval = max(5, Config.ENTRY_QUEUE_POLL_SEC)
        logger.info(f"Pending entry watcher started (every {interval}s).")
        while True:
            try:
                await asyncio.sleep(interval)
                if not self.state_engine.is_active():
                    continue
                pending_items = await asyncio.to_thread(self.trader._load_pending)
                pending_count = len(pending_items)
                if pending_count:
                    logger.info(f"[PENDING] Watcher polling {pending_count} queued item(s)...")
                messages = await asyncio.to_thread(self.trader.process_pending_queue)
                for msg in messages:
                    logger.info(f"PENDING: {msg}")
                    try:
                        await self.notifier.send(f"Pending entry\n\n{msg}")
                    except Exception as notify_err:
                        logger.error(f"Pending notify error: {notify_err}")
                        try:
                            if not self.client.is_connected():
                                await self.client.connect()
                        except Exception as reconnect_err:
                            logger.error(f"Pending watcher reconnect error: {reconnect_err}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Pending watcher error: {e}")

    async def _scheduled_alerts(self):
        ist = pytz.timezone("Asia/Kolkata")
        logger.info("Market Alert Scheduler Started.")
        while True:
            try:
                now = datetime.datetime.now(ist)
                current_time = now.strftime("%H:%M")
                if current_time in ["14:45", "15:15"]:
                    active_trades = await asyncio.to_thread(self.trader._load_active_trades)
                    if active_trades:
                        trades_list = "\n".join([f"🔹 {s}" for s in active_trades.keys()])
                        body = (
                            f"You have {len(active_trades)} active trades:\n{trades_list}\n\n"
                            "Reply 'EXIT ALL' to liquidate everything, or ignore to Carry Forward (NRML)."
                        )
                    else:
                        body = "No active trades currently open. All clear! ✅"
                    await self.notifier.send_alert(f"MARKET ALERT ({current_time} IST)", body)
                    logger.info(f"Pushed Bot Notification for {current_time}.")
                    await asyncio.sleep(61)
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Alert Scheduler Error: {str(e)}")
                await asyncio.sleep(60)

    async def _vol_sl_scheduler(self):
        if not Config.VOL_SL_SCHEDULE_ENABLED:
            return
        interval = max(10, Config.VOL_SL_SCHEDULE_POLL_SEC)
        logger.info(f"Volatility SL scheduler started (every {interval}s, IST widen/restore).")
        while True:
            try:
                await asyncio.sleep(interval)
                if not self.state_engine.is_active():
                    continue
                lines = await asyncio.to_thread(self.trader.run_volatility_sl_schedule_tick)
                for line in lines:
                    logger.info(f"[VOL_SL] {line}")
                    try:
                        await self.notifier.send(line)
                    except Exception as notify_err:
                        logger.error(f"VOL_SL notify error: {notify_err}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"VOL_SL scheduler error: {e}")
                await asyncio.sleep(interval)

    async def _eod_summary_scheduler(self):
        if not Config.EOD_SUMMARY_ENABLED:
            return
        interval = max(10, Config.EOD_SUMMARY_POLL_SEC)
        logger.info(f"EOD summary scheduler started (every {interval}s).")
        while True:
            try:
                await asyncio.sleep(interval)
                msg = await asyncio.to_thread(self.trader.run_eod_summary_tick)
                if msg:
                    logger.info("[EOD] Sending end-of-day summary alert.")
                    await self.notifier.send_alert("Market Closing Summary", msg)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"EOD summary scheduler error: {e}")
                await asyncio.sleep(interval)

    async def _active_trade_reconciler(self):
        if not Config.ACTIVE_RECONCILE_ENABLED:
            return
        interval = max(10, Config.ACTIVE_RECONCILE_POLL_SEC)
        logger.info(f"Active trade reconciler started (every {interval}s).")
        while True:
            try:
                await asyncio.sleep(interval)
                if not self.state_engine.is_active():
                    continue
                active_trades = await asyncio.to_thread(self.trader._load_active_trades)
                active_count = len(active_trades)
                if not active_count:
                    continue
                logger.info(f"[RECONCILE] Polling broker for {active_count} active trade(s)...")
                stats = await asyncio.to_thread(self.trader.maintain_active_trades)
                if stats.get("removed") or stats.get("rearmed"):
                    logger.info(
                        f"[RECONCILE] Maintenance result: active={stats.get('active', 0)} "
                        f"removed={stats.get('removed', 0)} rearmed={stats.get('rearmed', 0)}"
                    )
                for alert in (stats.get("alerts") or []):
                    try:
                        await self.notifier.send(f"Protection Maintenance\n\n{alert}")
                    except Exception as notify_err:
                        logger.error(f"Active maintenance alert notify error: {notify_err}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Active trade reconciler error: {e}")

    async def _management_queue_watcher(self):
        if not Config.MGMT_QUEUE_ENABLED:
            return
        interval = max(5, Config.MGMT_QUEUE_POLL_SEC)
        logger.info(f"Management queue watcher started (every {interval}s).")
        while True:
            try:
                await asyncio.sleep(interval)
                if not self.state_engine.is_active():
                    continue
                messages = await asyncio.to_thread(self.trader.process_management_queue)
                for msg in messages:
                    logger.info(f"[MGMT_QUEUE] {msg}")
                    try:
                        await self.notifier.send(f"Management Queue\n\n{msg}")
                    except Exception as notify_err:
                        logger.error(f"Management queue notify error: {notify_err}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Management queue watcher error: {e}")
                await asyncio.sleep(interval)

    async def _groww_auth_watcher(self):
        """
        Keep probing Groww readiness in background so the bot can start before
        daily manual approval/IP readiness and recover automatically when ready.
        """
        interval = max(15, int(getattr(Config, "GROWW_AUTH_POLL_SEC", 60)))
        logger.info(f"Groww auth watcher started (every {interval}s).")
        while True:
            try:
                await asyncio.sleep(interval)
                conn_ok = await asyncio.to_thread(self.trader.check_connection, log_fail=False)
                trade_ok = await asyncio.to_thread(self.trader.check_trading_access, log_fail=False) if conn_ok else False
                ready = bool(conn_ok and trade_ok)
                if self._groww_ready_last_state is None:
                    self._groww_ready_last_state = ready
                    if not ready:
                        await self.notifier.send(
                            "⚠️ Groww not ready yet. Bot is running and will retry automatically."
                        )
                    continue
                if ready == self._groww_ready_last_state:
                    continue
                self._groww_ready_last_state = ready
                if ready:
                    await self.notifier.send(
                        "✅ Groww connection and trading access restored. Broker actions are live."
                    )
                    try:
                        boot_stats = await asyncio.to_thread(self.trader.maintain_active_trades)
                        logger.info(
                            f"[GROWW] Post-recovery maintenance: active={boot_stats.get('active', 0)} "
                            f"removed={boot_stats.get('removed', 0)} rearmed={boot_stats.get('rearmed', 0)}"
                        )
                        for alert in (boot_stats.get("alerts") or []):
                            try:
                                await self.notifier.send(f"Protection Maintenance\n\n{alert}")
                            except Exception as notify_err:
                                logger.error(f"Post-recovery maintenance alert notify error: {notify_err}")
                    except Exception as e:
                        logger.error(f"[GROWW] Post-recovery maintenance failed: {e}")
                else:
                    await self.notifier.send(
                        "⚠️ Groww became unavailable. Bot remains online and will keep retrying."
                    )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Groww auth watcher error: {e}")
                await asyncio.sleep(interval)

    # ------------------------------------------------------------------
    # Admin override handler
    # ------------------------------------------------------------------

    def _parse_override(self, text: str) -> str | None:
        """Returns override command key if text matches, else None."""
        t = re.sub(r"\s+", " ", str(text or "").strip().upper())
        # Accept bot-style slash commands: /status, /help, /pause@my_bot
        if t.startswith("/"):
            t = t[1:]
            if " " in t:
                first, rest = t.split(" ", 1)
            else:
                first, rest = t, ""
            if "@" in first:
                first = first.split("@", 1)[0]
            t = f"{first} {rest}".strip()
        # HELP accepts suffixes like "HELP SYMBOL", "HELP COMMANDS", etc.
        if t == "HELP" or t.startswith("HELP "):
            return "help"
        # Position sizing mode controls (runtime):
        # SIZING / SIZING MODE                            -> show current modes
        # SIZING INDEX FIXED|RISK / SIZING STOCK ...      -> set per segment
        # SIZING FIXED|RISK                               -> set both segments
        if t in ("SIZING", "SIZING MODE", "POSITION SIZING", "POSITION_SIZING_MODE"):
            return "sizing_status"
        m = re.fullmatch(
            r"(?:SIZING|SIZING MODE|POSITION SIZING|POSITION_SIZING_MODE)\s+(INDEX|STOCK)\s+(FIXED|RISK)",
            t,
        )
        if m:
            return f"sizing_{m.group(1).lower()}_{m.group(2).lower()}"
        m = re.fullmatch(
            r"(?:SIZING|SIZING MODE|POSITION SIZING|POSITION_SIZING_MODE)\s+(FIXED|RISK)",
            t,
        )
        if m:
            return f"sizing_all_{m.group(1).lower()}"
        # Admin fast exits:
        # - EXIT <TRADING_SYMBOL> : close one position
        # - EXIT ALL              : close every active position
        if t == "EXIT ALL":
            return "exit_all_positions"
        m_exit_symbol = re.fullmatch(r"EXIT\s+([A-Z0-9]+)", t)
        if m_exit_symbol:
            return f"exit_symbol:{m_exit_symbol.group(1)}"
        for cmd in _OVERRIDE_COMMANDS:
            if t == cmd:
                return _OVERRIDE_COMMANDS[cmd]
        return None

    @staticmethod
    def _format_active_trades(active: dict) -> str:
        if not active:
            return "Active Trades\nNo active trades."

        lines = [f"Active Trades ({len(active)})"]
        for symbol, info in active.items():
            data = info if isinstance(info, dict) else {}
            side = str(data.get("entry_side", "BUY")).upper()
            qty = data.get("entry_qty", data.get("quantity", "-"))
            entry = data.get("entry_avg_price", data.get("entry_price", "-"))
            sl = data.get("sl_true", data.get("stop_loss", "-"))
            tgt = data.get("target", "-")
            lines.append(
                f"- {symbol} | {side} | qty={qty} | entry={entry} | sl={sl} | tgt={tgt}"
            )
        return "\n".join(lines)

    async def _handle_override(self, command: str):
        if command == "pause":
            self.state_engine.pause()
            await self.notifier.send_status("PAUSED", "Bot will ignore all signals until RESUME.")
        elif command == "resume":
            self.state_engine.resume()
            await self.notifier.send_status("ACTIVE", "Bot is now active and processing signals.")
        elif command == "kill":
            self.state_engine.kill()
            await self.notifier.send_status("KILLED", "Kill switch activated. Restart bot to reset.")
        elif command == "status":
            active = self.trader._load_active_trades()
            pending = self.trader._load_pending()
            pending_mgmt = self.trader._load_pending_management()
            dry = "ON" if not self.trader.execute_orders else "OFF"
            index_mode = str(Config.INDEX_POSITION_SIZING_MODE or "fixed").strip().lower()
            stock_mode = str(Config.STOCK_POSITION_SIZING_MODE or "fixed").strip().lower()
            detail = (
                f"State: {self.state_engine.status}\n"
                f"Dry Run (entry orders only): {dry}\n"
                f"Position sizing: index={index_mode}, stock={stock_mode}\n"
                f"Active trades: {len(active)}\n"
                f"Pending entry queue: {len(pending)}\n"
                f"Pending management queue: {len(pending_mgmt)}"
            )
            await self.notifier.send_status(self.state_engine.status, detail)
        elif command == "sizing_status":
            index_mode = str(Config.INDEX_POSITION_SIZING_MODE or "fixed").strip().lower()
            stock_mode = str(Config.STOCK_POSITION_SIZING_MODE or "fixed").strip().lower()
            await self.notifier.send(
                "Position Sizing Modes\n"
                f"Index: {index_mode}\n"
                f"Stock: {stock_mode}\n\n"
                "Use SIZING INDEX FIXED|RISK, SIZING STOCK FIXED|RISK, or SIZING FIXED|RISK (both)."
            )
        elif command == "sizing_all_fixed":
            Config.INDEX_POSITION_SIZING_MODE = "fixed"
            Config.STOCK_POSITION_SIZING_MODE = "fixed"
            await self.notifier.send(
                "Position sizing set to fixed for index and stock.\n"
                "Applies immediately for new entries."
            )
        elif command == "sizing_all_risk":
            Config.INDEX_POSITION_SIZING_MODE = "risk"
            Config.STOCK_POSITION_SIZING_MODE = "risk"
            await self.notifier.send(
                "Position sizing set to risk for index and stock.\n"
                "Applies immediately for new entries."
            )
        elif command == "sizing_index_fixed":
            Config.INDEX_POSITION_SIZING_MODE = "fixed"
            await self.notifier.send("Index position sizing set to fixed.")
        elif command == "sizing_index_risk":
            Config.INDEX_POSITION_SIZING_MODE = "risk"
            await self.notifier.send("Index position sizing set to risk.")
        elif command == "sizing_stock_fixed":
            Config.STOCK_POSITION_SIZING_MODE = "fixed"
            await self.notifier.send("Stock position sizing set to fixed.")
        elif command == "sizing_stock_risk":
            Config.STOCK_POSITION_SIZING_MODE = "risk"
            await self.notifier.send("Stock position sizing set to risk.")
        elif command == "trades":
            active = self.trader._load_active_trades()
            await self.notifier.send(self._format_active_trades(active))
        elif command == "exit_all_positions":
            active = self.trader._load_active_trades()
            symbols = [str(s) for s in active.keys() if str(s)]
            if not symbols:
                await self.notifier.send("ℹ️ EXIT ALL: no active trades to close.")
                return

            out = []
            for symbol in symbols:
                payload = {
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
                msg = await asyncio.to_thread(self.trader.execute_trade, payload)
                out.append(f"- {symbol}: {msg}")
            await self.notifier.send(
                "EXIT ALL completed\n"
                f"Processed {len(symbols)} active trade(s).\n\n" + "\n".join(out)
            )
        elif command.startswith("exit_symbol:"):
            symbol = command.split(":", 1)[1].strip().upper()
            payload = {
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
            msg = await asyncio.to_thread(self.trader.execute_trade, payload)
            await self.notifier.send(f"EXIT {symbol}\n\n{msg}")
        elif command == "rearm":
            msg = await asyncio.to_thread(self.trader.admin_rearm_protection_for_active_trades)
            await self.notifier.send(msg)
        elif command == "help":
            await self.notifier.send(
                "Bot Command Help\n\n"
                "Admin override commands\n"
                "- HELP — show this help\n"
                "- STATUS — bot state, dry-run, active/pending counts\n"
                "- TRADES — list all active trades\n"
                "- EXIT <TRADING_SYMBOL> — close one active trade\n"
                "- EXIT ALL — close all active trades\n"
                "- REARM — re-arm missing OCO protection for active trades\n"
                "- CLEAR PENDING — clear pending entry queue\n"
                "- SIZING — show current position sizing modes\n"
                "- SIZING INDEX FIXED|RISK — set index sizing mode\n"
                "- SIZING STOCK FIXED|RISK — set stock sizing mode\n"
                "- SIZING FIXED|RISK — set both index and stock modes\n"
                "- PAUSE — ignore all incoming signals\n"
                "- RESUME — resume signal processing\n"
                "- KILL — emergency stop (restart required)\n"
                "- DRYRUN ON — no live entry orders\n"
                "- DRYRUN OFF — live entry orders enabled\n\n"
                "Single-trade management (no reply context needed)\n"
                "- EXIT <TRADING_SYMBOL>\n"
                "- TSL TO <PRICE> <TRADING_SYMBOL>\n"
                "- CHANGE TARGET TO <PRICE> <TRADING_SYMBOL>\n"
                "- RESTORE SL <TRADING_SYMBOL>\n\n"
                "Example: EXIT NIFTY26424300PE"
            )
        elif command == "dryrun_on":
            self.trader.execute_orders = False
            await self.notifier.send(
                "DRY RUN enabled. Entry BUY/SELL (market/limit) are not placed. "
                "LTP/quotes still use Groww if configured. EXIT/trail/target use the broker only "
                "for positions opened with a real order id; simulated trades stay local-only."
            )
        elif command == "dryrun_off":
            self.trader.execute_orders = True
            await self.notifier.send("DRY RUN disabled. LIVE entry orders are now active.")
        elif command == "clear_pending":
            removed = await asyncio.to_thread(self.trader.clear_pending_entries)
            await self.notifier.send(
                "Pending entry queue cleared\n"
                f"Removed {removed} queued item(s)."
            )
            logger.info(f"[STAGE 1] Admin clear_pending executed: removed={removed}")

    # ------------------------------------------------------------------
    # Main signal handler
    # ------------------------------------------------------------------

    async def _handle_message(self, event):
        child_text = str(event.raw_text or "")
        parent_text = ""
        is_reply = False
        sender_id = getattr(event, "sender_id", None)

        is_target_chat = (
            event.chat_id == self.target_chat_id
            or str(event.chat_id) == str(self.target_chat_id)
        )
        is_admin_context = (
            event.chat_id == Config.ADMIN_ID
            or str(event.chat_id) == str(Config.ADMIN_ID)
            or sender_id == Config.ADMIN_ID
            or str(sender_id) == str(Config.ADMIN_ID)
        )

        # Ignore unrelated chats; process only channel signals and admin-origin messages.
        if not (is_target_chat or is_admin_context):
            return

        # Ignore own bot notifications to prevent feedback loop
        if child_text.startswith("🤖") or "BOT ACTION" in child_text:
            return

        # Ignore blank/non-text messages early to avoid unnecessary parser/LLM calls.
        if not child_text.strip():
            logger.info("[STAGE 1] Empty message body — skipping parse.")
            return

        # Admin override commands:
        # accept when message is in admin chat OR sent by admin user (e.g. bot DM).
        if is_admin_context:
            override = self._parse_override(child_text)
            if override:
                logger.info(f"[STAGE 1] Admin override command: {override}")
                await self._handle_override(override)
                return

        if event.is_reply:
            reply_msg = await event.get_reply_message()
            if reply_msg:
                is_reply = True
                parent_text = reply_msg.raw_text or ""

        raw_text = f"[CONTEXT: {parent_text}] {child_text}" if is_reply else child_text
        advisory_mode, advisory_reason, advisory_scope = self._detect_size_override_advisory(child_text)
        if advisory_mode:
            self._pending_size_override_mode = advisory_mode
            self._pending_size_override_reason = advisory_reason
            self._pending_size_override_scope = advisory_scope
            self._pending_size_override_until_ts = datetime.datetime.now().timestamp() + 900
            logger.info(
                f"[STAGE 1] Size advisory armed for next signal: mode={advisory_mode} "
                f"scope={advisory_scope} reason={advisory_reason}"
            )
        logger.info("─" * 60)
        logger.info(f"[STAGE 1] MESSAGE RECEIVED")
        logger.info(f"[STAGE 1] is_reply={is_reply} | child_len={len(child_text)} | parent_len={len(parent_text)}")
        logger.info(f"[STAGE 1] child : {child_text[:200]}")
        if is_reply:
            logger.info(f"[STAGE 1] parent: {parent_text[:200]}")

        # --- Stage 2: AI Parser ---
        signal = self.assistant.parse_signal_as_event(
            {
                "is_reply": is_reply,
                "parent_text": parent_text,
                "child_text": child_text,
                "full_text": raw_text,
            }
        )

        if not signal:
            logger.info("[STAGE 2] Parser returned no signal — message ignored.")
            logger.info("─" * 60)
            return

        signal_scope = self._signal_trade_scope(signal)
        # Guard direct in-message advisory against cross-scope application.
        if signal.size_override_mode:
            direct_mode, direct_reason, direct_scope = self._detect_size_override_advisory(child_text)
            if direct_mode and not self._scope_matches_trade(direct_scope, signal_scope):
                logger.info(
                    f"[STAGE 2] Ignored direct size override due to scope mismatch: "
                    f"scope={direct_scope} trade_scope={signal_scope}"
                )
                signal.size_override_mode = ""
                signal.size_override_reason = ""

        if self._pending_size_override_mode and datetime.datetime.now().timestamp() <= self._pending_size_override_until_ts:
            if (not signal.size_override_mode) and self._scope_matches_trade(self._pending_size_override_scope, signal_scope):
                signal.size_override_mode = self._pending_size_override_mode
                signal.size_override_reason = self._pending_size_override_reason
                logger.info(
                    f"[STAGE 2] Applied pending size override: {signal.size_override_mode} "
                    f"(scope={self._pending_size_override_scope}, reason={signal.size_override_reason})"
                )
                self._pending_size_override_mode = ""
                self._pending_size_override_reason = ""
                self._pending_size_override_scope = "ALL"
                self._pending_size_override_until_ts = 0.0
            elif not self._scope_matches_trade(self._pending_size_override_scope, signal_scope):
                logger.info(
                    f"[STAGE 2] Pending size override not applied (scope mismatch): "
                    f"pending_scope={self._pending_size_override_scope} trade_scope={signal_scope}"
                )

        if signal.is_spread:
            symbol_summary = ", ".join(f"{l.action} {l.trading_symbol}" for l in signal.legs)
            logger.info(f"[STAGE 2] Signal: SPREAD ({len(signal.legs)} legs) {symbol_summary}")
        else:
            logger.info(
                f"[STAGE 2] Signal: action={signal.action} | symbol={signal.trading_symbol} "
                f"| range={signal.buy_range_min}-{signal.buy_range_max} "
                f"| sl={signal.stop_loss} | tgt={signal.target}"
            )

        # --- Stage 3: State Engine ---
        result_msg = self.state_engine.process(signal)

        # --- Stage 4: Notify ---
        if result_msg:
            logger.info(f"[STAGE 4] RESULT: {result_msg}")
            action = signal.action
            if signal.is_spread:
                symbol = ", ".join(leg.trading_symbol for leg in signal.legs)
            else:
                symbol = signal.trading_symbol
            await self.notifier.send_signal_result(action, symbol, result_msg)
        else:
            logger.info("[STAGE 4] No result returned — signal suppressed by state engine.")
        logger.info("─" * 60)

    @staticmethod
    def _detect_size_override_advisory(text: str) -> tuple[str, str, str]:
        t = str(text or "").upper()
        scope = "ALL"
        has_index_hint = (" INDEX " in f" {t} ") or ("INDEX TRADES" in t) or ("INDEX OPTIONS" in t)
        has_stock_hint = (" STOCK " in f" {t} ") or ("STOCK OPTIONS" in t)
        if has_index_hint and not has_stock_hint:
            scope = "INDEX"
        elif has_stock_hint and not has_index_hint:
            scope = "STOCK"
        elif has_index_hint and has_stock_hint:
            # Most mixed advisories in channels are "less qty in index, focus stocks".
            scope = "INDEX"
        for phrase in _SIZE_OVERRIDE_FIXED_PHRASES:
            if phrase in t:
                return "FIXED", phrase, scope
        return "", "", "ALL"

    @staticmethod
    def _signal_trade_scope(signal) -> str:
        ts = str(getattr(signal, "trading_symbol", "") or "").upper()
        if not ts:
            return "UNKNOWN"
        for idx in _INDEX_UNDERLYINGS:
            if ts.startswith(idx):
                return "INDEX"
        return "STOCK"

    @staticmethod
    def _scope_matches_trade(scope: str, trade_scope: str) -> bool:
        s = str(scope or "ALL").upper()
        t = str(trade_scope or "UNKNOWN").upper()
        if s == "ALL" or t == "UNKNOWN":
            return True
        return s == t

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    async def start(self):
        await self.client.start()
        self.notifier.set_client(self.client)

        # Resolve "me" to a real ID if needed
        if Config.ADMIN_ID == "me":
            me = await self.client.get_me()
            Config.ADMIN_ID = me.id
            logger.info(f"Resolved 'me' to ID: {Config.ADMIN_ID}")

        # Refresh symbol map (downloads from Groww or loads cache)
        symbol_manager.refresh(trader=self.trader)

        try:
            target_entity = await self.client.get_entity(self.target_chat_id)
            admin_entity = await self.client.get_entity(Config.ADMIN_ID)
            target_name = getattr(target_entity, "title", "Private Channel")
            admin_name = f"{getattr(admin_entity, 'first_name', 'User')} {getattr(admin_entity, 'last_name', '')}"
            logger.info(f"Connected to Telegram as: {(await self.client.get_me()).first_name}")
            logger.info(f"📡 LISTENING TO: {target_name} ({self.target_chat_id})")
            logger.info(f"🔔 NOTIFYING ADMIN: {admin_name} ({Config.ADMIN_ID})")
            startup_detail = (
                f"Monitoring signals from: {target_name}\n\n"
                f"{_startup_command_reference()}"
            )
            await self.notifier.send_status("STARTED", startup_detail)
        except Exception as e:
            logger.warning(f"Metadata Fetch Error: {str(e)}")

        asyncio.create_task(self._scheduled_alerts())
        asyncio.create_task(self._vol_sl_scheduler())
        asyncio.create_task(self._eod_summary_scheduler())
        asyncio.create_task(self._pending_entry_watcher())
        asyncio.create_task(self._management_queue_watcher())
        asyncio.create_task(self._active_trade_reconciler())
        asyncio.create_task(self._groww_auth_watcher())

        # Listen to both incoming and outgoing so admin commands typed from the
        # same logged-in account (Saved Messages / bot DM) are also captured.
        @self.client.on(events.NewMessage())
        async def handler(event):
            await self._handle_message(event)

        logger.info("Bot Connection Established.")
        await self.client.run_until_disconnected()
