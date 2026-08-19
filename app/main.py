from __future__ import annotations

import asyncio
import os
import shutil
import signal
from typing import TYPE_CHECKING

from telethon.errors import AuthKeyDuplicatedError, PhoneNumberInvalidError
from src.core.logger import logger
from src.core.config import Config
from app.bootstrap import (
    clear_restart_flag,
    repo_root,
    restart_flag_path,
    restart_requested,
    runtime_dir,
    spawn_bot_process,
    stop_flag_path,
)
from app.process_lock import acquire_singleton_lock, terminate_other_bot_processes

if TYPE_CHECKING:
    from src.integrations.telegram.listener import BotListener


def _session_file(path: str) -> str:
    if str(path).endswith(".session"):
        return path
    return path + ".session"


def _is_session_locked_error(exc: BaseException) -> bool:
    return "database is locked" in str(exc).lower()


_durable_session_path: str | None = None


def _remove_sqlite_sidecars(session_file: str) -> None:
    for suffix in ("-journal", "-wal", "-shm"):
        sidecar = session_file + suffix
        try:
            os.remove(sidecar)
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.warning("Could not remove SQLite sidecar %s: %s", sidecar, e)


def _copy_session_file(src: str, dst: str) -> bool:
    if not os.path.exists(src):
        return False
    os.makedirs(os.path.dirname(os.path.abspath(dst)), exist_ok=True)
    shutil.copy2(src, dst)
    return True


def _restore_session() -> None:
    """Seed the durable session from persistent storage when configured."""
    persistent = Config.SESSION_PERSISTENT_PATH
    local = _durable_session_path or Config.SESSION_PATH
    if not persistent or persistent == local:
        return
    src = _session_file(persistent)
    dst = _session_file(local)
    if _copy_session_file(src, dst):
        logger.info("Session restored from persistent storage: %s → %s", src, dst)
    else:
        logger.warning(
            "No persistent session found at %s — Telegram re-auth may be required.", src
        )


def _prepare_working_session() -> None:
    """Copy the durable Telethon session to a private runtime file.

    Telethon can keep ``data/session_name.session`` locked on Windows long after
    a crashed process. Connecting against ``runtime/telegram.session`` avoids
    that leftover handle, as long as no other bot is still using the same
    Telegram auth key.
    """
    global _durable_session_path
    _durable_session_path = Config.SESSION_PATH
    _restore_session()
    work = str(runtime_dir() / "telegram")
    src = _session_file(_durable_session_path)
    dst = _session_file(work)
    try:
        _remove_sqlite_sidecars(dst)
        if os.path.exists(dst):
            os.remove(dst)
        if _copy_session_file(src, dst):
            logger.info("Telegram session working copy: %s → %s", src, dst)
            Config.SESSION_PATH = work
            return
        logger.warning(
            "No durable Telegram session at %s — a new login may be required.", src
        )
        Config.SESSION_PATH = work
    except OSError as e:
        logger.warning(
            "Could not create working session copy (%s). Using durable session %s.",
            e,
            src,
        )
        Config.SESSION_PATH = _durable_session_path


def _save_session() -> None:
    """Copy the working session back to durable / persistent storage."""
    work = _session_file(Config.SESSION_PATH)
    durable_base = _durable_session_path or Config.SESSION_PATH
    durable = _session_file(durable_base)
    if os.path.exists(work) and os.path.abspath(work) != os.path.abspath(durable):
        try:
            if _copy_session_file(work, durable):
                logger.info("Session saved to durable storage: %s → %s", work, durable)
        except Exception as e:
            logger.warning("Failed to save working session to durable path: %s", e)
    persistent = Config.SESSION_PERSISTENT_PATH
    if not persistent or persistent == durable_base:
        return
    dst = _session_file(persistent)
    src = durable if os.path.exists(durable) else work
    if os.path.exists(src):
        try:
            _copy_session_file(src, dst)
            logger.info("Session saved to persistent storage: %s → %s", src, dst)
        except Exception as e:
            logger.warning("Failed to save session to persistent storage: %s", e)

_bot: BotListener | None = None


async def run_bot():
    """
    Main entry point for the Industrial-Standard AI Trading Bot.
    """
    acquire_singleton_lock()
    terminate_other_bot_processes(wait_sec=8.0)
    _prepare_working_session()
    # Import after the process lock so Database() / Telethon session are not
    # opened while another instance is still shutting down.
    from src.integrations.telegram.listener import BotListener

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
    stop_file = stop_flag_path()
    stop_watcher_task: asyncio.Task | None = None

    # If a previous stop/restart task left flags behind, clear them so startup can proceed.
    for stale_flag in (stop_file, restart_flag_path()):
        if stale_flag.exists():
            try:
                stale_flag.unlink()
                logger.info(f"Removed stale runtime flag at startup: {stale_flag}")
            except Exception as e:
                logger.warning(f"Unable to remove stale runtime flag ({stale_flag}): {e}")

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
        if _is_session_locked_error(e):
            logger.error(
                "Telegram session file is still locked after retries: %s. "
                "A leftover python process is still using this Telegram login. "
                "On the server run scripts/stop_bot_windows.ps1, then in Task "
                "Manager end every python.exe whose command line contains app.main, "
                "wait 10 seconds, and start once.",
                _session_file(Config.SESSION_PATH),
                exc_info=True,
            )
            await asyncio.sleep(20)
        else:
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
        if restart_requested():
            clear_restart_flag()
            new_pid = spawn_bot_process()
            if new_pid:
                logger.info(
                    "Bot restart spawned new process PID=%s (repo=%s).",
                    new_pid,
                    repo_root(),
                )
            else:
                logger.error(
                    "Bot restart requested but failed to spawn a new process. "
                    "Start manually with scripts/start_bot_windows.ps1 or start_bot_linux.sh."
                )
        logger.info("Bot Process Terminated.")


if __name__ == "__main__":
    Config.validate()
    acquire_singleton_lock()
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info("Bot Manually Stopped by User.")
