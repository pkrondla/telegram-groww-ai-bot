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
        
        # 1. Verify Groww API before listening
        if not bot.trader.check_connection():
            logger.error("STOP: Groww Login Failed. Please check your .env key at 6 AM IST.")
            return

        # 2. Verify trading endpoint access/IP whitelist before listening
        if not bot.trader.check_trading_access():
            logger.error("STOP: Groww trading access check failed (likely static IP whitelist issue).")
            return

        # 3. Active-trade maintenance on startup (reconcile + protection rearm if needed)
        boot_stats = bot.trader.maintain_active_trades()
        logger.info(
            f"Startup active-trade maintenance: active={boot_stats.get('active', 0)} "
            f"removed={boot_stats.get('removed', 0)} rearmed={boot_stats.get('rearmed', 0)}"
        )

        # 4. Start the Telegram Listener
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
