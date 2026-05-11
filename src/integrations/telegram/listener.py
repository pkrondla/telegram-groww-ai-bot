from telethon import TelegramClient, events
import asyncio
import datetime
import re
import hashlib
import pytz
from src.core.config import Config
from src.core.logger import logger
from src.integrations.ai.assistant import Assistant
from src.trading.execution.multi_account_manager import MultiAccountExecutionManager
from src.integrations.telegram.notifier import Notifier
from src.trading.reference.symbol_manager import symbol_manager


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
    "DAY SUMMARY": "day_summary",
    "EOD SUMMARY": "day_summary",
    "MARKET CLOSING SUMMARY": "day_summary",
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


def _telegram_login_prompt() -> str:
    return input(
        "Please enter your phone in international format (e.g. +91XXXXXXXXXX), "
        "or bot token: "
    )


class BotListener:
    """
    Connects to Telegram via Telethon and coordinates the full pipeline:
      Telegram -> Message listener -> AI parser -> State engine -> Trade executor -> Notifier
    """

    def __init__(self):
        self.api_id = Config.API_ID
        self.api_hash = Config.API_HASH
        self.session_name = Config.SESSION_PATH
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
        self.multi_account = MultiAccountExecutionManager(Config.groww_accounts())
        self.notifier = Notifier()
        self._pending_size_override_mode = ""
        self._pending_size_override_reason = ""
        self._pending_size_override_scope = "ALL"
        self._pending_size_override_until_ts = 0.0
        self._groww_ready_last_state: dict[str, bool] = {}
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    @staticmethod
    def _parse_hhmm_ist(raw: str) -> tuple[int, int] | None:
        s = str(raw or "").strip()
        m = re.match(r"^(\d{1,2}):(\d{2})$", s)
        if not m:
            return None
        hh, mm = int(m.group(1)), int(m.group(2))
        if hh < 0 or hh > 23 or mm < 0 or mm > 59:
            return None
        return hh, mm

    def _is_broker_window_open_ist(self) -> bool:
        ist = pytz.timezone("Asia/Kolkata")
        now = datetime.datetime.now(ist)
        if now.weekday() >= 5:
            return False
        open_t = self._parse_hhmm_ist(Config.MARKET_OPEN_TIME_IST)
        close_t = self._parse_hhmm_ist(Config.MARKET_CLOSE_TIME_IST)
        if not open_t or not close_t:
            return False
        market_open = now.replace(hour=open_t[0], minute=open_t[1], second=0, microsecond=0)
        market_close = now.replace(hour=close_t[0], minute=close_t[1], second=0, microsecond=0)
        window_start = market_open - datetime.timedelta(minutes=int(Config.BROKER_WINDOW_PREOPEN_MIN))
        window_end = market_close + datetime.timedelta(minutes=int(Config.BROKER_WINDOW_POSTCLOSE_MIN))
        return window_start <= now <= window_end

    # ------------------------------------------------------------------
    # Background tasks
    # ------------------------------------------------------------------

    async def _pending_entry_watcher(self):
        if not Config.ENTRY_QUEUE_ENABLED:
            return
        interval = max(5, Config.ENTRY_QUEUE_POLL_SEC)
        logger.info(f"Pending entry watcher started (every {interval}s).")
        while not self._stop.is_set():
            try:
                await asyncio.sleep(interval)
                if not self.multi_account.any_active():
                    continue
                if not self._is_broker_window_open_ist():
                    continue
                for account_id, rt in self.multi_account.runtimes().items():
                    pending_items = await asyncio.to_thread(rt.trader._load_pending)
                    pending_count = len(pending_items)
                    if pending_count:
                        logger.info(f"[PENDING][{account_id}] Watcher polling {pending_count} queued item(s)...")
                    messages = await asyncio.to_thread(rt.trader.process_pending_queue)
                    for msg in messages:
                        logger.info(f"PENDING[{account_id}]: {msg}")
                        try:
                            await self.notifier.send(f"Pending entry [{account_id}]\n\n{msg}")
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
        while not self._stop.is_set():
            try:
                if not self._is_broker_window_open_ist():
                    await asyncio.sleep(30)
                    continue
                now = datetime.datetime.now(ist)
                current_time = now.strftime("%H:%M")
                if current_time in ["14:45", "15:15"]:
                    total_active = 0
                    body_lines = []
                    for account_id, rt in self.multi_account.runtimes().items():
                        active_trades = await asyncio.to_thread(rt.trader._load_active_trades)
                        total_active += len(active_trades)
                        if active_trades:
                            trades_list = "\n".join([f"🔹 {s}" for s in active_trades.keys()])
                            body_lines.append(f"[{account_id}] {len(active_trades)} active:\n{trades_list}")
                    body = (
                        "\n\n".join(body_lines)
                        if body_lines
                        else "No active trades currently open across all accounts. All clear! ✅"
                    )
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
        while not self._stop.is_set():
            try:
                await asyncio.sleep(interval)
                if not self.multi_account.any_active():
                    continue
                if not self._is_broker_window_open_ist():
                    continue
                for account_id, rt in self.multi_account.runtimes().items():
                    lines = await asyncio.to_thread(rt.trader.run_volatility_sl_schedule_tick)
                    for line in lines:
                        logger.info(f"[VOL_SL][{account_id}] {line}")
                        try:
                            await self.notifier.send(f"[{account_id}] {line}")
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
        while not self._stop.is_set():
            try:
                await asyncio.sleep(interval)
                if not self._is_broker_window_open_ist():
                    continue
                parts = []
                for account_id, rt in self.multi_account.runtimes().items():
                    msg = await asyncio.to_thread(rt.trader.run_eod_summary_tick)
                    if msg:
                        parts.append(f"[{account_id}]\n{msg}")
                if parts:
                    logger.info("[EOD] Sending end-of-day summary alert.")
                    await self.notifier.send_alert("Market Closing Summary", "\n\n".join(parts))
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
        while not self._stop.is_set():
            try:
                await asyncio.sleep(interval)
                if not self.multi_account.any_active():
                    continue
                if not self._is_broker_window_open_ist():
                    continue
                for account_id, rt in self.multi_account.runtimes().items():
                    active_trades = await asyncio.to_thread(rt.trader._load_active_trades)
                    active_count = len(active_trades)
                    if not active_count:
                        continue
                    logger.info(f"[RECONCILE][{account_id}] Polling broker for {active_count} active trade(s)...")
                    stats = await asyncio.to_thread(rt.trader.maintain_active_trades, include_rearm=False)
                    if stats.get("removed"):
                        logger.info(
                            f"[RECONCILE][{account_id}] Maintenance result: active={stats.get('active', 0)} "
                            f"removed={stats.get('removed', 0)} rearmed={stats.get('rearmed', 0)}"
                        )
                    for alert in (stats.get("alerts") or []):
                        try:
                            await self.notifier.send(f"Protection Maintenance [{account_id}]\n\n{alert}")
                        except Exception as notify_err:
                            logger.error(f"Active maintenance alert notify error: {notify_err}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Active trade reconciler error: {e}")

    async def _auto_rearm_scheduler(self):
        if not Config.AUTO_REARM_PROTECTION:
            return
        interval = max(10, Config.AUTO_REARM_POLL_SEC)
        logger.info(f"Auto re-arm scheduler started (every {interval}s).")
        while not self._stop.is_set():
            try:
                await asyncio.sleep(interval)
                if not self.multi_account.any_active():
                    continue
                if not self._is_broker_window_open_ist():
                    continue
                for account_id, rt in self.multi_account.runtimes().items():
                    active_trades = await asyncio.to_thread(rt.trader._load_active_trades)
                    if not active_trades:
                        continue
                    logger.debug(f"[REARM][{account_id}] Polling broker for {len(active_trades)} active trade(s)...")
                    stats = await asyncio.to_thread(rt.trader.run_auto_rearm_protection_tick)
                    if stats.get("rearmed"):
                        logger.info(
                            f"[REARM][{account_id}] Maintenance result: active={stats.get('active', 0)} "
                            f"rearmed={stats.get('rearmed', 0)}"
                        )
                    for alert in (stats.get("alerts") or []):
                        try:
                            await self.notifier.send(f"Protection Maintenance [{account_id}]\n\n{alert}")
                        except Exception as notify_err:
                            logger.error(f"Auto re-arm alert notify error: {notify_err}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Auto re-arm scheduler error: {e}")

    async def _management_queue_watcher(self):
        if not Config.MGMT_QUEUE_ENABLED:
            return
        interval = max(5, Config.MGMT_QUEUE_POLL_SEC)
        logger.info(f"Management queue watcher started (every {interval}s).")
        while not self._stop.is_set():
            try:
                await asyncio.sleep(interval)
                if not self.multi_account.any_active():
                    continue
                if not self._is_broker_window_open_ist():
                    continue
                for account_id, rt in self.multi_account.runtimes().items():
                    messages = await asyncio.to_thread(rt.trader.process_management_queue)
                    for msg in messages:
                        logger.info(f"[MGMT_QUEUE][{account_id}] {msg}")
                        try:
                            await self.notifier.send(f"Management Queue [{account_id}]\n\n{msg}")
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
        while not self._stop.is_set():
            try:
                await asyncio.sleep(interval)
                if not self._is_broker_window_open_ist():
                    continue
                for account_id, rt in self.multi_account.runtimes().items():
                    conn_ok = await asyncio.to_thread(rt.trader.check_connection, log_fail=False)
                    trade_ok = await asyncio.to_thread(rt.trader.check_trading_access, log_fail=False) if conn_ok else False
                    ready = bool(conn_ok and trade_ok)
                    prev = self._groww_ready_last_state.get(account_id, None)
                    if prev is None:
                        self._groww_ready_last_state[account_id] = ready
                        if not ready:
                            await self.notifier.send(
                                f"⚠️ Groww not ready yet for [{account_id}]. Bot is running and will retry automatically."
                            )
                        continue
                    if ready == prev:
                        continue
                    self._groww_ready_last_state[account_id] = ready
                    if ready:
                        await self.notifier.send(
                            f"✅ Groww connection and trading access restored for [{account_id}]."
                        )
                        try:
                            boot_stats = await asyncio.to_thread(rt.trader.maintain_active_trades, include_rearm=False)
                            logger.info(
                                f"[GROWW][{account_id}] Post-recovery maintenance: active={boot_stats.get('active', 0)} "
                                f"removed={boot_stats.get('removed', 0)} rearmed={boot_stats.get('rearmed', 0)}"
                            )
                            for alert in (boot_stats.get("alerts") or []):
                                try:
                                    await self.notifier.send(f"Protection Maintenance [{account_id}]\n\n{alert}")
                                except Exception as notify_err:
                                    logger.error(f"Post-recovery maintenance alert notify error: {notify_err}")
                        except Exception as e:
                            logger.error(f"[GROWW][{account_id}] Post-recovery maintenance failed: {e}")
                    else:
                        await self.notifier.send(
                            f"⚠️ Groww became unavailable for [{account_id}]. Bot remains online and will keep retrying."
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
        m_tsl = re.fullmatch(r"TSL(?:\s+TO)?\s+(\d+(?:\.\d+)?)\s+([A-Z0-9]+)", t)
        if m_tsl:
            return f"admin_tsl:{m_tsl.group(2).upper()}:{m_tsl.group(1)}"
        m_ut = re.fullmatch(
            r"(?:CHANGE\s+TARGET(?:\s+TO)?|TARGET)\s+(\d+(?:\.\d+)?)\s+([A-Z0-9]+)",
            t,
        )
        if m_ut:
            return f"admin_target:{m_ut.group(2).upper()}:{m_ut.group(1)}"
        if t in ("PROGRESS", "PROGRESS ALL", "SL PROGRESS", "SL PROGRESS ALL"):
            return "progress_all"
        m_prog = re.fullmatch(r"(?:SL\s+)?PROGRESS\s+([A-Z0-9]+)", t)
        if m_prog and m_prog.group(1) != "ALL":
            return f"progress_symbol:{m_prog.group(1)}"
        if t in ("VOLSL WIDEN", "VOL_SL WIDEN", "VOL SL WIDEN"):
            return "volsl_widen"
        if t in ("VOLSL RESTORE", "VOL_SL RESTORE", "VOL SL RESTORE"):
            return "volsl_restore"
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

    @staticmethod
    def _format_multi_results(title: str, results: dict[str, str]) -> str:
        lines = [title]
        for account_id, msg in sorted(results.items()):
            lines.append(f"[{account_id}] {msg}")
        return "\n".join(lines)

    async def _handle_override(self, command: str):
        if command == "pause":
            self.multi_account.pause_all()
            await self.notifier.send_status("PAUSED", "Bot will ignore all signals until RESUME.")
        elif command == "resume":
            self.multi_account.resume_all()
            await self.notifier.send_status("ACTIVE", "Bot is now active and processing signals.")
        elif command == "kill":
            self.multi_account.kill_all()
            await self.notifier.send_status("KILLED", "Kill switch activated. Restart bot to reset.")
        elif command == "status":
            lines = [f"State: {self.multi_account.status_summary()}"]
            for account_id, rt in self.multi_account.runtimes().items():
                active = rt.trader._load_active_trades()
                pending = rt.trader._load_pending()
                pending_mgmt = rt.trader._load_pending_management()
                dry = "ON" if not rt.trader.execute_orders else "OFF"
                lines.append(
                    f"[{account_id}] DryRun={dry} Active={len(active)} Pending={len(pending)} MgmtPending={len(pending_mgmt)}"
                )
            detail = "\n".join(lines)
            await self.notifier.send_status(self.multi_account.status_summary(), detail)
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
            parts = []
            for account_id, rt in self.multi_account.runtimes().items():
                active = rt.trader._load_active_trades()
                parts.append(f"[{account_id}]\n{self._format_active_trades(active)}")
            await self.notifier.send("\n\n".join(parts))
        elif command == "exit_all_positions":
            out = []
            total = 0
            for account_id, rt in self.multi_account.runtimes().items():
                active = rt.trader._load_active_trades()
                symbols = [str(s) for s in active.keys() if str(s)]
                if not symbols:
                    out.append(f"[{account_id}] no active trades.")
                    continue
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
                    msg = await asyncio.to_thread(rt.trader.execute_trade, payload)
                    out.append(f"[{account_id}] {symbol}: {msg}")
                    total += 1
            await self.notifier.send(
                "EXIT ALL completed\n"
                f"Processed {total} active trade(s).\n\n" + "\n".join(out)
            )
        elif command.startswith("exit_symbol:"):
            symbol = command.split(":", 1)[1].strip().upper()
            out = []
            for account_id, rt in self.multi_account.runtimes().items():
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
                msg = await asyncio.to_thread(rt.trader.execute_trade, payload)
                out.append(f"[{account_id}] {msg}")
            await self.notifier.send(f"EXIT {symbol}\n\n" + "\n".join(out))
        elif command.startswith("admin_tsl:"):
            parts = command.split(":", 2)
            if len(parts) < 3:
                await self.notifier.send("⚠️ TSL: invalid command format.")
                return
            sym = parts[1].strip().upper()
            try:
                slv = float(parts[2].strip())
            except ValueError:
                await self.notifier.send(f"⚠️ TSL: invalid price {parts[2]!r}.")
                return
            logger.info(f"[STAGE 1] Admin override: TSL {sym} @ {slv}")
            payload = {
                "action": "TRAIL_SL",
                "stock": sym,
                "trading_symbol": sym,
                "entry_range": [0, 0],
                "buy_range_min": 0.0,
                "buy_range_max": 0.0,
                "stop_loss": slv,
                "target": 0,
                "is_trade": True,
            }
            out = []
            for account_id, rt in self.multi_account.runtimes().items():
                msg = await asyncio.to_thread(rt.trader.execute_trade, payload)
                out.append(f"[{account_id}] {msg}")
            await self.notifier.send(f"TSL {sym} @ {slv}\n\n" + "\n".join(out))
        elif command.startswith("admin_target:"):
            parts = command.split(":", 2)
            if len(parts) < 3:
                await self.notifier.send("⚠️ TARGET: invalid command format.")
                return
            sym = parts[1].strip().upper()
            try:
                tgt = float(parts[2].strip())
            except ValueError:
                await self.notifier.send(f"⚠️ TARGET: invalid price {parts[2]!r}.")
                return
            logger.info(f"[STAGE 1] Admin override: TARGET {sym} @ {tgt}")
            payload = {
                "action": "UPDATE_TGT",
                "stock": sym,
                "trading_symbol": sym,
                "entry_range": [0, 0],
                "buy_range_min": 0.0,
                "buy_range_max": 0.0,
                "stop_loss": 0,
                "target": tgt,
                "target_leg_index": 0,
                "is_trade": True,
            }
            out = []
            for account_id, rt in self.multi_account.runtimes().items():
                msg = await asyncio.to_thread(rt.trader.execute_trade, payload)
                out.append(f"[{account_id}] {msg}")
            await self.notifier.send(f"TARGET {sym} @ {tgt}\n\n" + "\n".join(out))
        elif command == "rearm":
            out = []
            for account_id, rt in self.multi_account.runtimes().items():
                msg = await asyncio.to_thread(rt.trader.admin_rearm_protection_for_active_trades)
                out.append(f"[{account_id}] {msg}")
            await self.notifier.send("\n".join(out))
        elif command == "progress_all":
            logger.info("[STAGE 1] Admin override command: PROGRESS (all active trades)")
            out = []
            for account_id, rt in self.multi_account.runtimes().items():
                msg = await asyncio.to_thread(rt.trader.admin_sync_target_progression, None)
                out.append(f"[{account_id}] {msg}")
            await self.notifier.send("\n\n".join(out))
        elif command.startswith("progress_symbol:"):
            sym = command.split(":", 1)[1].strip()
            logger.info(f"[STAGE 1] Admin override command: PROGRESS {sym}")
            out = []
            for account_id, rt in self.multi_account.runtimes().items():
                msg = await asyncio.to_thread(rt.trader.admin_sync_target_progression, sym)
                out.append(f"[{account_id}] {msg}")
            await self.notifier.send("\n".join(out))
        elif command == "volsl_widen":
            logger.info("[STAGE 1] Admin override command: VOLSL WIDEN")
            parts = []
            for account_id, rt in self.multi_account.runtimes().items():
                lines = await asyncio.to_thread(
                    lambda r=rt: r.trader.run_volatility_sl_schedule_tick(admin_widen=True)
                )
                msg = (
                    "\n".join(lines)
                    if lines
                    else "ℹ️ VOLSL WIDEN: no eligible active trades (need OCO ids, open broker qty, sl_true > 0)."
                )
                parts.append(f"[{account_id}] {msg}")
            msg = "\n".join(parts)
            await self.notifier.send(msg)
        elif command == "volsl_restore":
            logger.info("[STAGE 1] Admin override command: VOLSL RESTORE")
            parts = []
            for account_id, rt in self.multi_account.runtimes().items():
                lines = await asyncio.to_thread(
                    lambda r=rt: r.trader.run_volatility_sl_schedule_tick(admin_restore=True)
                )
                msg = (
                    "\n".join(lines)
                    if lines
                    else "ℹ️ VOLSL RESTORE: no eligible trades (need widened state: vol_sl_needs_restore, or use RESTORE SL <SYMBOL>)."
                )
                parts.append(f"[{account_id}] {msg}")
            msg = "\n".join(parts)
            await self.notifier.send(msg)
        elif command == "day_summary":
            # On-demand summary uses the same formatter as scheduled market close alert.
            parts = []
            for account_id, rt in self.multi_account.runtimes().items():
                msg = await asyncio.to_thread(rt.trader._build_eod_summary_text)
                parts.append(f"[{account_id}]\n{msg}")
            await self.notifier.send_alert("Market Closing Summary", "\n\n".join(parts))
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
                "- VOLSL WIDEN — run scheduled widen now (VOL_SL_WIDEN_PCT / sl_true; ignores today's widen idempotency)\n"
                "- VOLSL RESTORE — run scheduled restore now (requires prior widen / vol_sl_needs_restore)\n"
                "- PROGRESS — multi-target SL progression sync for all active trades\n"
                "- PROGRESS <TRADING_SYMBOL> — progression sync for one active trade\n"
                "- DAY SUMMARY — send current Market Closing Summary on demand\n"
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
                "- PROGRESS <TRADING_SYMBOL>\n"
                "- EXIT <TRADING_SYMBOL>\n"
                "- TSL <PRICE> <TRADING_SYMBOL> (legacy: TSL TO <PRICE> <SYMBOL>)\n"
                "- TARGET <PRICE> <TRADING_SYMBOL> (legacy: CHANGE TARGET TO <PRICE> <SYMBOL>)\n"
                "- RESTORE SL <TRADING_SYMBOL>\n\n"
                "Examples: EXIT NIFTY26424300PE | TSL 40 NIFTY26424300PE | TARGET 55 NIFTY26424300PE"
            )
        elif command == "dryrun_on":
            for _account_id, rt in self.multi_account.runtimes().items():
                rt.trader.execute_orders = False
            await self.notifier.send(
                "DRY RUN enabled. Entry BUY/SELL (market/limit) are not placed. "
                "LTP/quotes still use Groww if configured. EXIT/trail/target use the broker only "
                "for positions opened with a real order id; simulated trades stay local-only."
            )
        elif command == "dryrun_off":
            for _account_id, rt in self.multi_account.runtimes().items():
                rt.trader.execute_orders = True
            await self.notifier.send("DRY RUN disabled. LIVE entry orders are now active.")
        elif command == "clear_pending":
            parts = []
            total_removed = 0
            for account_id, rt in self.multi_account.runtimes().items():
                removed = await asyncio.to_thread(rt.trader.clear_pending_entries)
                total_removed += int(removed or 0)
                parts.append(f"[{account_id}] removed={removed}")
            await self.notifier.send(
                "Pending entry queue cleared\n"
                f"Removed {total_removed} queued item(s).\n" + "\n".join(parts)
            )
            logger.info(f"[STAGE 1] Admin clear_pending executed: removed={total_removed}")

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
            self._pending_size_override_until_ts = (
                datetime.datetime.now().timestamp() + int(Config.SIZE_ADVISORY_TTL_SEC)
            )
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

        # --- Stage 2: AI Parser (off event loop — OpenAI call can take 1-3s) ---
        signal = await asyncio.to_thread(
            self.assistant.parse_signal_as_event,
            {
                "is_reply": is_reply,
                "parent_text": parent_text,
                "child_text": child_text,
                "full_text": raw_text,
            },
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

        # --- Stage 3: Multi-account fan-out ---
        symbol = ", ".join(leg.trading_symbol for leg in signal.legs) if signal.is_spread else signal.trading_symbol
        raw_sid = f"{event.chat_id}:{getattr(event, 'id', '')}:{signal.action}:{symbol}"
        signal_id = hashlib.sha1(raw_sid.encode("utf-8")).hexdigest()[:20]
        account_results = await asyncio.to_thread(
            self.multi_account.process_signal,
            signal,
            signal_id=signal_id,
        )

        # --- Stage 4: Notify ---
        if account_results:
            for account_id, msg in account_results.items():
                logger.info(f"[STAGE 4][{account_id}] RESULT: {msg}")
            await self.notifier.send_signal_result(
                signal.action,
                symbol,
                self._format_multi_results("Account execution results", account_results),
            )
        else:
            logger.info("[STAGE 4] No result returned — signal suppressed by all account engines.")
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
        await self.client.start(phone=Config.PHONE or _telegram_login_prompt)
        self.notifier.set_client(self.client)

        # Resolve "me" to a real ID if needed
        if Config.ADMIN_ID == "me":
            me = await self.client.get_me()
            Config.ADMIN_ID = me.id
            logger.info(f"Resolved 'me' to ID: {Config.ADMIN_ID}")

        # Refresh symbol map (downloads from Groww or loads cache)
        traders = [rt.trader for rt in self.multi_account.runtimes().values()]
        symbol_manager.refresh(traders=traders)

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

        self._tasks = [
            asyncio.create_task(self._scheduled_alerts()),
            asyncio.create_task(self._vol_sl_scheduler()),
            asyncio.create_task(self._eod_summary_scheduler()),
            asyncio.create_task(self._pending_entry_watcher()),
            asyncio.create_task(self._management_queue_watcher()),
            asyncio.create_task(self._active_trade_reconciler()),
            asyncio.create_task(self._auto_rearm_scheduler()),
            asyncio.create_task(self._groww_auth_watcher()),
        ]

        # Listen to both incoming and outgoing so admin commands typed from the
        # same logged-in account (Saved Messages / bot DM) are also captured.
        @self.client.on(events.NewMessage())
        async def handler(event):
            await self._handle_message(event)

        logger.info("Bot Connection Established.")
        await self.client.run_until_disconnected()

    async def stop(self):
        """Gracefully cancel all background tasks and disconnect from Telegram."""
        logger.info("Graceful shutdown initiated...")
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        try:
            await self.notifier.send_status("STOPPED", "Bot shutdown completed gracefully.")
        except Exception as e:
            logger.warning(f"Failed to send shutdown status notification: {e}")
        try:
            await self.client.disconnect()
        except Exception as e:
            logger.warning(f"Disconnect error during shutdown: {e}")
        logger.info("Bot stopped cleanly.")
