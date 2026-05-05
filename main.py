import asyncio
import signal
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

    if not Config.GROWW_API_KEY or not Config.GROWW_API_SECRET:
        logger.warning("GROWW_API credentials missing. Groww client features may not work.")

    bot = BotListener()
    _bot = bot

    def _signal_handler():
        logger.info("Shutdown signal received — stopping bot gracefully...")
        asyncio.create_task(bot.stop())

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows does not support add_signal_handler for all signals.
            pass

    try:
        # Non-blocking Groww checks: bot should still start and process Telegram messages.
        groww_ok = bot.trader.check_connection(log_fail=True)
        trading_ok = bot.trader.check_trading_access(log_fail=True) if groww_ok else False
        if not (groww_ok and trading_ok):
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
        await bot.start()
    except Exception as e:
        logger.error(f"Bot Crash Encountered: {str(e)}", exc_info=True)
    finally:
        logger.info("Bot Process Terminated.")


if __name__ == "__main__":
    Config.validate()
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info("Bot Manually Stopped by User.")
