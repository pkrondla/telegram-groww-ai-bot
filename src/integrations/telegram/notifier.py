from __future__ import annotations
import asyncio
import requests
from src.core.config import Config
from src.core.logger import logger


class Notifier:
    """
    Centralized notification service.

    Sends admin alerts via:
    - Telegram Bot API (preferred, bypasses silent folder)
    - Telegram user session fallback (when no bot token)

    All modules call Notifier instead of making raw HTTP/Telegram calls directly.
    """

    _CHANNEL_ECHO_MAX = 3500

    def __init__(self, telegram_client=None):
        self._client = telegram_client  # Telethon client; injected after connection

    def set_client(self, client):
        self._client = client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def send(self, message: str, *, parse_mode: str | None = None):
        """Send a notification to the admin."""
        if Config.BOT_TOKEN:
            await self._send_via_bot(message, parse_mode=parse_mode)
        else:
            await self._send_via_session(message)

    async def send_signal_result(self, action: str, symbol: str, result: str):
        """Formatted trade action result notification."""
        icon = {
            "BUY": "📈",
            "SELL": "📉",
            "EXIT_ALL": "🚨",
            "TRAIL_SL": "📌",
            "RESTORE_SL": "🔁",
            "RESTORE_ALL": "🔁",
            "UPDATE_TGT": "🎯",
            "EXTEND_RANGE": "📶",
            "RECONCILE": "🧾",
            "SPREAD": "🔀",
        }.get(action.upper(), "🤖")
        symbol_text = str(symbol or "").strip()
        action_text = str(action or "").upper().strip()
        if symbol_text and symbol_text.upper() != "NONE":
            # Send high-churn trade results as plain text to avoid Telegram entity parse failures.
            msg = f"{icon} BOT ACTION: {action_text}\nSYMBOL: {symbol_text}\n\n{result}"
        else:
            msg = f"{icon} BOT ACTION: {action_text}\n\n{result}"
        await self.send(msg, parse_mode=None)

    async def send_status(self, status: str, detail: str = ""):
        """Send a bot status update (pause/resume/kill/startup)."""
        icons = {
            "ACTIVE": "🟢",
            "PAUSED": "⏸️",
            "KILLED": "🔴",
            "STARTED": "🚀",
            "STOPPED": "🛑",
        }
        icon = icons.get(status.upper(), "ℹ️")
        msg = f"{icon} Bot Status: {status}"
        if detail:
            msg += f"\n{detail}"
        await self.send(msg, parse_mode=None)

    async def send_alert(self, title: str, body: str):
        """Generic scheduled alert (e.g. market close warnings)."""
        msg = f"🔔 {title}\n\n{body}"
        await self.send(msg, parse_mode=None)

    async def send_error(self, context: str, error: str):
        """Send an error notification."""
        msg = f"❌ Error in {context}\n\n{error}"
        await self.send(msg, parse_mode=None)

    async def send_channel_echo(self, text: str):
        """Forward a raw source-channel message verbatim to the admin as a plain alert."""
        body = (text or "").strip()
        if not body:
            return
        if len(body) > self._CHANNEL_ECHO_MAX:
            body = body[: self._CHANNEL_ECHO_MAX] + "\n… [truncated]"
        msg = f"📩 CHANNEL\n\n{body}"
        await self.send(msg, parse_mode=None)

    # ------------------------------------------------------------------
    # Internal delivery
    # ------------------------------------------------------------------

    async def _send_via_bot(self, message: str, *, parse_mode: str | None = "Markdown"):
        try:
            url = f"https://api.telegram.org/bot{Config.BOT_TOKEN}/sendMessage"
            payload = {"chat_id": Config.ADMIN_ID, "text": message}
            if parse_mode:
                payload["parse_mode"] = str(parse_mode)
            resp = await asyncio.to_thread(
                requests.post,
                url,
                data=payload,
                timeout=10,
            )
            if not resp.ok:
                body = (resp.text or "")[:500]
                logger.warning(f"Notifier: Bot API returned {resp.status_code}: {body[:200]}")
                # Some runtime messages contain Markdown-sensitive characters
                # (e.g. underscores in [VOL_SL]). Retry as plain text.
                if "can't parse entities" in body.lower():
                    plain_resp = await asyncio.to_thread(
                        requests.post,
                        url,
                        data={"chat_id": Config.ADMIN_ID, "text": message},
                        timeout=10,
                    )
                    if not plain_resp.ok:
                        logger.warning(
                            f"Notifier: Plain-text retry failed {plain_resp.status_code}: "
                            f"{(plain_resp.text or '')[:200]}"
                        )
        except Exception as e:
            logger.error(f"Notifier: Bot API send failed: {e}")
            await self._send_via_session(message)

    async def _send_via_session(self, message: str):
        if self._client is None:
            logger.warning("Notifier: No Telegram client available for session send.")
            return
        try:
            await self._client.send_message("me", message)
        except Exception as e:
            logger.error(f"Notifier: Session send failed: {e}")
