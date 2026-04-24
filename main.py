import asyncio
from src.listener import BotListener
from src.logger import logger
from src.config import Config

async def run_bot():
    """
    Main entry point for the Industrial-Standard AI Trading Bot.
    """
    logger.info("Initializing Industrial-Standard Trading Bot...")
    
    # Check for critical secrets before starting
    if not Config.OPENAI_API_KEY:
        logger.error("CRITICAL: OPENAI_API_KEY is missing. AI parsing will fail.")
        return
        
    if not Config.GROWW_API_KEY or not Config.GROWW_API_SECRET:
        logger.warning("GROWW_API credentials missing. Groww client features may not work.")

    try:
        bot = BotListener()

        # Non-blocking Groww checks: bot should still start and process Telegram messages.
        groww_ok = bot.trader.check_connection(log_fail=True)
        trading_ok = bot.trader.check_trading_access(log_fail=True) if groww_ok else False
        if not (groww_ok and trading_ok):
            logger.warning(
                "Groww auth/trading access not ready at startup. "
                "Bot will keep running and retry in background."
            )
        else:
            # Active-trade maintenance on startup only when broker access is available.
            boot_stats = bot.trader.maintain_active_trades()
            logger.info(
                f"Startup active-trade maintenance: active={boot_stats.get('active', 0)} "
                f"removed={boot_stats.get('removed', 0)} rearmed={boot_stats.get('rearmed', 0)}"
            )

        # Start Telegram listener regardless of Groww readiness.
        await bot.start()
    except Exception as e:
        logger.error(f"Bot Crash Encountered: {str(e)}")
    finally:
        logger.info("Bot Process Terminated.")

if __name__ == "__main__":
    # Start the asyncio event loop
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info("Bot Manually Stopped by User.")
