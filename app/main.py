import asyncio
import os
import shutil
import signal
from telethon.errors import AuthKeyDuplicatedError, PhoneNumberInvalidError
from src.integrations.telegram.listener import BotListener
from src.core.logger import logger
from src.core.config import Config
from app.bootstrap import runtime_dir


def _session_file(path: str) -> str:
    return path + ".session"


def _restore_session() -> None:
    """Copy session file from persistent storage (Azure Files) to local path on startup.

    Telethon's SQLite session uses its own sqlite3 connection which has no
    retry logic.  On Azure Files SMB that connection fails with 'database is
    locked'.  Keeping the active session file on local /tmp storage avoids the
    SMB locking entirely; this function seeds /tmp from the durable copy on
    container start.
    """
    persistent = Config.SESSION_PERSISTENT_PATH
    local = Config.SESSION_PATH
    if not persistent or persistent == local:
        return
    src = _session_file(persistent)
    dst = _session_file(local)
    os.makedirs(os.path.dirname(os.path.abspath(dst)), exist_ok=True)
    if os.path.exists(src):
        shutil.copy2(src, dst)
        logger.info("Session restored from persistent storage: %s → %s", src, dst)
    else:
        logger.warning(
            "No persistent session found at %s — Telegram re-auth may be required.", src
        )


def _save_session() -> None:
    """Copy local session file back to persistent storage (Azure Files) on shutdown."""
    persistent = Config.SESSION_PERSISTENT_PATH
    local = Config.SESSION_PATH
    if not persistent or persistent == local:
        return
    src = _session_file(local)
    dst = _session_file(persistent)
    if os.path.exists(src):
        try:
            shutil.copy2(src, dst)
            logger.info("Session saved to persistent storage: %s → %s", src, dst)
        except Exception as e:
            logger.warning("Failed to save session to persistent storage: %s", e)

_bot: BotListener | None = None


async def run_bot():
    """
    Main entry point for the Industrial-Standard AI Trading Bot.
    """
    global _bot
    logger.info("Initializing Industrial-Standard Trading Bot...")
    _restore_session()

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
    stop_file = runtime_dir() / "bot.stop"
    stop_watcher_task: asyncio.Task | None = None

    # If a previous stop task left the flag behind, clear it so startup can proceed.
    if stop_file.exists():
        try:
            stop_file.unlink()
            logger.info(f"Removed stale stop flag at startup: {stop_file}")
        except Exception as e:
            logger.warning(f"Unable to remove stale stop flag ({stop_file}): {e}")

    loop = asyncio.get_running_loop()
    shutdown_lock = asyncio.Lock()
    shutdown_done = False
    auth_key_duplicated = False

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
    except asyncio.CancelledError:
        # Graceful shutdown can cancel in-flight Telethon startup calls.
        logger.info("Bot startup cancelled due to shutdown request.")
    except AuthKeyDuplicatedError:
        auth_key_duplicated = True
        logger.error(
            "Telegram rejected the Telethon session because the same .session "
            "auth key was used concurrently from another IP. Session file: %s. "
            "Stop any other bot instance that uses this exact session file, "
            "configure each environment with its own SESSION_PATH, then delete "
            "this invalid session file and log in again.",
            _session_file(Config.SESSION_PATH),
            exc_info=True,
        )
    except PhoneNumberInvalidError:
        logger.error(
            "Telegram rejected the phone number. Use E.164 international format "
            "for user login, for example +919894627197 for an Indian number, "
            "or set TELEGRAM_PHONE in .env with that format.",
            exc_info=True,
        )
    except Exception as e:
        logger.error(f"Bot Crash Encountered: {str(e)}", exc_info=True)
    finally:
        if stop_watcher_task:
            stop_watcher_task.cancel()
            await asyncio.gather(stop_watcher_task, return_exceptions=True)
        await _graceful_shutdown("run_bot finalizer")
        if auth_key_duplicated:
            logger.warning(
                "Skipping session persistence because Telegram invalidated this "
                "auth key after duplicate concurrent use."
            )
        else:
            _save_session()
        logger.info("Bot Process Terminated.")


if __name__ == "__main__":
    Config.validate()
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info("Bot Manually Stopped by User.")
