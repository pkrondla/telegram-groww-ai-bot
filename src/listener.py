from telethon import TelegramClient, events
import asyncio
import datetime
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
    "DRYRUN ON":  "dryrun_on",
    "DRYRUN OFF": "dryrun_off",
}


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

    # ------------------------------------------------------------------
    # Admin override handler
    # ------------------------------------------------------------------

    def _parse_override(self, text: str) -> str | None:
        """Returns override command key if text matches, else None."""
        t = text.strip().upper()
        for cmd in _OVERRIDE_COMMANDS:
            if t == cmd:
                return _OVERRIDE_COMMANDS[cmd]
        return None

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
            dry = "ON" if not self.trader.execute_orders else "OFF"
            detail = (
                f"State: {self.state_engine.status}\n"
                f"Dry Run (entry orders only): {dry}\n"
                f"Active trades: {len(active)}\n"
                f"Pending queue: {len(pending)}"
            )
            await self.notifier.send_status(self.state_engine.status, detail)
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

        # Ignore own bot notifications to prevent feedback loop
        if child_text.startswith("🤖") or "BOT ACTION" in child_text:
            return

        # Admin override commands (from admin chat only)
        if event.chat_id == Config.ADMIN_ID or str(event.chat_id) == str(Config.ADMIN_ID):
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
            await self.notifier.send_status("STARTED", f"Monitoring signals from: {target_name}")
        except Exception as e:
            logger.warning(f"Metadata Fetch Error: {str(e)}")

        asyncio.create_task(self._scheduled_alerts())
        asyncio.create_task(self._pending_entry_watcher())
        asyncio.create_task(self._active_trade_reconciler())

        @self.client.on(events.NewMessage(chats=[self.target_chat_id, Config.ADMIN_ID]))
        async def handler(event):
            await self._handle_message(event)

        logger.info("Bot Connection Established.")
        await self.client.run_until_disconnected()
