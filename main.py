import asyncio
import signal
from pathlib import Path
from src.listener import BotListener
from src.logger import logger
from src.config import Config

_bot: BotListener | None = None


async def run_bot():
    """
    Main entry point for the Industrial-Standard AI Trading Bot.
    """
    global _bot
    logger.info("Initializing Industrial-Standard Trading Bot...")

    try:
        accounts = Config.groww_accounts()
        any_key = any(str(a.api_key or "").strip() for a in accounts)
        if not any_key:
            logger.warning("GROWW_API credentials missing. Groww client features may not work.")
    except Exception:
        # Config.groww_accounts() can raise if GROWW_ACCOUNTS_JSON is malformed.
        logger.warning("GROWW accounts config invalid/missing. Groww client features may not work.")

    bot = BotListener()
    _bot = bot
    stop_file = Path(__file__).resolve().parent / "runtime" / "bot.stop"
    stop_watcher_task: asyncio.Task | None = None

    loop = asyncio.get_running_loop()
    shutdown_lock = asyncio.Lock()
    shutdown_done = False

    async def _graceful_shutdown(reason: str):
        nonlocal shutdown_done
        async with shutdown_lock:
            if shutdown_done:
                return
            shutdown_done = True
            logger.info(f"Shutdown requested ({reason}) — stopping bot gracefully...")
            try:
                # Shield cleanup from cancellation during interpreter shutdown.
                await asyncio.shield(bot.stop())
            except Exception as e:
                logger.warning(f"Graceful shutdown encountered an error: {e}")

    def _schedule_shutdown(reason: str):
        loop.create_task(_graceful_shutdown(reason))

    def _sync_signal_handler(signum, _frame):
        try:
            signal_name = signal.Signals(signum).name
        except Exception:
            signal_name = str(signum)
        # Safe to call from synchronous signal handlers.
        loop.call_soon_threadsafe(_schedule_shutdown, f"signal {signal_name}")

    async def _stop_file_watcher():
        # Poll a local stop-file to support scheduler-driven graceful stop
        # on platforms where external signals are unreliable (especially Windows).
        while True:
            await asyncio.sleep(1)
            if stop_file.exists():
                _schedule_shutdown(f"stop file {stop_file}")
                return

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _schedule_shutdown, f"signal {sig.name}")
        except (NotImplementedError, RuntimeError):
            # Windows fallback: install synchronous handler and bridge to loop.
            signal.signal(sig, _sync_signal_handler)

    try:
        # Non-blocking Groww checks: bot should still start and process Telegram messages.
        statuses = bot.multi_account.check_connections()
        all_ready = all(conn and trade for conn, trade in statuses.values())
        if not all_ready:
            logger.warning(
                "Groww auth/trading access not ready at startup. "
                "Bot will keep running and retry in background."
            )
        else:
            logger.info(
                "Startup: skipping active-trade maintenance (handled by scheduled "
                "reconcile / re-arm / VOL_SL tasks after listener starts)."
            )

        # Start Telegram listener regardless of Groww readiness.
        stop_watcher_task = asyncio.create_task(_stop_file_watcher())
        await bot.start()
    except Exception as e:
        logger.error(f"Bot Crash Encountered: {str(e)}", exc_info=True)
    finally:
        if stop_watcher_task:
            stop_watcher_task.cancel()
            await asyncio.gather(stop_watcher_task, return_exceptions=True)
        await _graceful_shutdown("run_bot finalizer")
        logger.info("Bot Process Terminated.")


if __name__ == "__main__":
    Config.validate()
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info("Bot Manually Stopped by User.")
