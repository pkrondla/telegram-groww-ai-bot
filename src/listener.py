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
    "HELP":     "help",
    "DRYRUN ON":  "dryrun_on",
    "DRYRUN OFF": "dryrun_off",
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


def _startup_command_reference() -> str:
    """Compact admin command/help text shown in startup notification."""
    return "Send `HELP` to view all available commands."


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
        self._pending_size_override_until_ts = 0.0

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
                pending_count = len(self.trader._load_pending())
                if pending_count:
                    logger.info(f"[PENDING] Watcher polling {pending_count} queued item(s)...")
                messages = self.trader.process_pending_queue()
                for msg in messages:
                    logger.info(f"PENDING: {msg}")
                    try:
                        await self.notifier.send(f"⏳ **Pending entry**\n\n{msg}")
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
                    active_trades = self.trader._load_active_trades()
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
                lines = self.trader.run_volatility_sl_schedule_tick()
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
                msg = self.trader.run_eod_summary_tick()
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
                active_count = len(self.trader._load_active_trades())
                if not active_count:
                    continue
                logger.info(f"[RECONCILE] Polling broker for {active_count} active trade(s)...")
                stats = self.trader.maintain_active_trades()
                if stats.get("removed") or stats.get("rearmed"):
                    logger.info(
                        f"[RECONCILE] Maintenance result: active={stats.get('active', 0)} "
                        f"removed={stats.get('removed', 0)} rearmed={stats.get('rearmed', 0)}"
                    )
                for alert in (stats.get("alerts") or []):
                    try:
                        await self.notifier.send(f"🛡️ **Protection Maintenance**\n\n{alert}")
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
                messages = self.trader.process_management_queue()
                for msg in messages:
                    logger.info(f"[MGMT_QUEUE] {msg}")
                    try:
                        await self.notifier.send(f"🧠 **Management Queue**\n\n{msg}")
                    except Exception as notify_err:
                        logger.error(f"Management queue notify error: {notify_err}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Management queue watcher error: {e}")
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
        # SIZING / SIZING MODE                -> show current mode
        # SIZING FIXED / SIZING RISK          -> set mode
        # SIZING MODE FIXED / ... RISK        -> set mode
        # POSITION SIZING FIXED / ... RISK    -> set mode
        if t in ("SIZING", "SIZING MODE", "POSITION SIZING", "POSITION_SIZING_MODE"):
            return "sizing_status"
        m = re.fullmatch(
            r"(?:SIZING|SIZING MODE|POSITION SIZING|POSITION_SIZING_MODE)\s+(FIXED|RISK)",
            t,
        )
        if m:
            return f"sizing_{m.group(1).lower()}"
        for cmd in _OVERRIDE_COMMANDS:
            if t == cmd:
                return _OVERRIDE_COMMANDS[cmd]
        return None

    @staticmethod
    def _format_active_trades(active: dict) -> str:
        if not active:
            return "📭 **Active Trades**\nNo active trades."

        lines = [f"📊 **Active Trades ({len(active)})**"]
        for symbol, info in active.items():
            data = info if isinstance(info, dict) else {}
            side = str(data.get("entry_side", "BUY")).upper()
            qty = data.get("entry_qty", data.get("quantity", "-"))
            entry = data.get("entry_avg_price", data.get("entry_price", "-"))
            sl = data.get("sl_true", data.get("stop_loss", "-"))
            tgt = data.get("target", "-")
            lines.append(
                f"• `{symbol}` | {side} | qty={qty} | entry={entry} | sl={sl} | tgt={tgt}"
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
            sizing_mode = str(Config.POSITION_SIZING_MODE or "fixed").strip().lower()
            detail = (
                f"State: {self.state_engine.status}\n"
                f"Dry Run (entry orders only): {dry}\n"
                f"Position sizing mode: {sizing_mode}\n"
                f"Active trades: {len(active)}\n"
                f"Pending entry queue: {len(pending)}\n"
                f"Pending management queue: {len(pending_mgmt)}"
            )
            await self.notifier.send_status(self.state_engine.status, detail)
        elif command == "sizing_status":
            mode = str(Config.POSITION_SIZING_MODE or "fixed").strip().lower()
            await self.notifier.send(
                "⚙️ **Position Sizing Mode**\n"
                f"Current: `{mode}`\n\n"
                "Use `SIZING FIXED` or `SIZING RISK` to change."
            )
        elif command == "sizing_fixed":
            Config.POSITION_SIZING_MODE = "fixed"
            await self.notifier.send(
                "⚙️ Position sizing mode set to `fixed`.\n"
                "Applies immediately for new entries."
            )
        elif command == "sizing_risk":
            Config.POSITION_SIZING_MODE = "risk"
            await self.notifier.send(
                "⚙️ Position sizing mode set to `risk`.\n"
                "Applies immediately for new entries."
            )
        elif command == "trades":
            active = self.trader._load_active_trades()
            await self.notifier.send(self._format_active_trades(active))
        elif command == "help":
            await self.notifier.send(
                "📘 **Bot Command Help**\n\n"
                "**Admin override commands**\n"
                "• `HELP` — show this help\n"
                "• `STATUS` — bot state, dry-run, active/pending counts\n"
                "• `TRADES` — list all active trades\n"
                "• `SIZING` — show current position sizing mode\n"
                "• `SIZING FIXED` — use configured fixed lots\n"
                "• `SIZING RISK` — use risk-based sizing\n"
                "• `PAUSE` — ignore all incoming signals\n"
                "• `RESUME` — resume signal processing\n"
                "• `KILL` — emergency stop (restart required)\n"
                "• `DRYRUN ON` — no live entry orders\n"
                "• `DRYRUN OFF` — live entry orders enabled\n\n"
                "**Single-trade management (no reply context needed)**\n"
                "• `EXIT ALL <TRADING_SYMBOL>`\n"
                "• `TSL TO <PRICE> <TRADING_SYMBOL>`\n"
                "• `CHANGE TARGET TO <PRICE> <TRADING_SYMBOL>`\n"
                "• `RESTORE SL <TRADING_SYMBOL>`\n\n"
                "Example: `EXIT ALL NIFTY26424300PE`"
            )
        elif command == "dryrun_on":
            self.trader.execute_orders = False
            await self.notifier.send(
                "🧪 **DRY RUN enabled.** Entry BUY/SELL (market/limit) are not placed. "
                "LTP/quotes still use Groww if configured. EXIT/trail/target use the broker only "
                "for positions opened with a real order id; simulated trades stay local-only."
            )
        elif command == "dryrun_off":
            self.trader.execute_orders = True
            await self.notifier.send("🔴 **DRY RUN disabled.** LIVE entry orders are now active.")

    # ------------------------------------------------------------------
    # Main signal handler
    # ------------------------------------------------------------------

    async def _handle_message(self, event):
        child_text = event.raw_text
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
        advisory_mode, advisory_reason = self._detect_size_override_advisory(child_text)
        if advisory_mode:
            self._pending_size_override_mode = advisory_mode
            self._pending_size_override_reason = advisory_reason
            self._pending_size_override_until_ts = datetime.datetime.now().timestamp() + 900
            logger.info(
                f"[STAGE 1] Size advisory armed for next signal: mode={advisory_mode} reason={advisory_reason}"
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

        if (
            not signal.size_override_mode
            and self._pending_size_override_mode
            and datetime.datetime.now().timestamp() <= self._pending_size_override_until_ts
        ):
            signal.size_override_mode = self._pending_size_override_mode
            signal.size_override_reason = self._pending_size_override_reason
            logger.info(
                f"[STAGE 2] Applied pending size override: {signal.size_override_mode} "
                f"({signal.size_override_reason})"
            )
            self._pending_size_override_mode = ""
            self._pending_size_override_reason = ""
            self._pending_size_override_until_ts = 0.0

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
    def _detect_size_override_advisory(text: str) -> tuple[str, str]:
        t = str(text or "").upper()
        for phrase in _SIZE_OVERRIDE_FIXED_PHRASES:
            if phrase in t:
                return "FIXED", phrase
        return "", ""

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

        # Listen to both incoming and outgoing so admin commands typed from the
        # same logged-in account (Saved Messages / bot DM) are also captured.
        @self.client.on(events.NewMessage())
        async def handler(event):
            await self._handle_message(event)

        logger.info("Bot Connection Established.")
        await self.client.run_until_disconnected()
