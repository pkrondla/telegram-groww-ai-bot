"""
Lightweight in-memory metrics for the trading bot.

Usage:
    from .metrics import metrics
    metrics.increment("signals_received")
    metrics.snapshot()  # -> {"signals_received": 5, ...}

All operations are thread-safe. No external dependencies.
"""
import threading
from collections import defaultdict


class Metrics:
    def __init__(self):
        self._lock = threading.Lock()
        self._counters: dict[str, int] = defaultdict(int)

    def increment(self, name: str, by: int = 1) -> None:
        with self._lock:
            self._counters[name] += by

    def increment_labeled(self, name: str, *, label: str, by: int = 1) -> None:
        key = f"{name}.{str(label or 'unknown')}"
        with self._lock:
            self._counters[key] += by

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counters)

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()

    def summary_text(self) -> str:
        snap = self.snapshot()
        if not snap:
            return "No metrics recorded."
        lines = [f"{k}: {v}" for k, v in sorted(snap.items())]
        return "\n".join(lines)


# Module-level singleton — import and use anywhere in the codebase.
metrics = Metrics()
