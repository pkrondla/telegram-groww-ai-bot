"""
QueueManager — owns all read/write operations on the two pending queues.

  pending_entries    — BUY/SELL signals waiting for price to enter range.
  pending_management — EXIT/TRAIL/RESTORE/UPDATE_TGT retries with backoff.

The actual queue *processing* logic (process_pending_queue,
process_management_queue) remains in Trader for now because it calls many
Trader-internal methods. Moving it here is planned for a future sprint once
there is sufficient test coverage.

Usage inside Trader (or anywhere):
    from .queue_manager import queue_manager
    items = queue_manager.load_entries()
    queue_manager.save_entries(items)
"""
from __future__ import annotations

import logging
import time
import uuid

from .db import db

logger = logging.getLogger(__name__)


class QueueManager:
    # ------------------------------------------------------------------
    # Pending entries (LTP queue)
    # ------------------------------------------------------------------

    def load_entries(self) -> list[dict]:
        return db.get_pending_entries()

    def save_entries(self, items: list[dict]) -> None:
        db.replace_pending_entries(items)

    def append_entry(self, item: dict) -> None:
        items = self.load_entries()
        items.append(item)
        self.save_entries(items)

    def clear_entries(self) -> int:
        items = self.load_entries()
        count = len(items)
        self.save_entries([])
        logger.info("QueueManager: cleared %d pending entry item(s).", count)
        return count

    # ------------------------------------------------------------------
    # Pending management queue (EXIT/TRAIL/etc retries)
    # ------------------------------------------------------------------

    def load_management(self) -> list[dict]:
        return db.get_pending_management()

    def save_management(self, items: list[dict]) -> None:
        db.replace_pending_management(items)

    def append_management(self, item: dict) -> None:
        items = self.load_management()
        items.append(item)
        self.save_management(items)

    def build_management_item(
        self,
        *,
        symbol: str,
        action: str,
        payload: dict,
        reason: str,
        priority: int = 5,
    ) -> dict:
        """Build a standardised management queue entry."""
        return {
            "id": str(uuid.uuid4())[:8],
            "symbol": symbol,
            "action": action,
            "payload": payload,
            "reason": reason,
            "priority": priority,
            "attempt_count": 0,
            "created_at": time.time(),
            "next_retry_at": time.time(),
            "dedupe_key": f"{symbol}:{action}",
        }

    def enqueue_management(
        self,
        *,
        symbol: str,
        action: str,
        payload: dict,
        reason: str,
        priority: int = 5,
    ) -> None:
        """Add a management action to the retry queue (idempotent by dedupe_key)."""
        items = self.load_management()
        dedupe_key = f"{symbol}:{action}"
        if any(i.get("dedupe_key") == dedupe_key for i in items):
            logger.debug(
                "QueueManager: skipped duplicate mgmt enqueue %s %s", action, symbol
            )
            return
        item = self.build_management_item(
            symbol=symbol, action=action, payload=payload,
            reason=reason, priority=priority,
        )
        items.append(item)
        self.save_management(items)
        logger.info("QueueManager: enqueued %s %s (reason: %s)", action, symbol, reason)


# Module-level singleton.
queue_manager = QueueManager()
