from __future__ import annotations

import copy
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.trading.execution.accounts import AccountConfig
from src.core.config import Config
from src.persistence.db import db
from src.core.logger import logger
from src.core.metrics import metrics
from src.trading.state.state_engine import StateEngine
from src.trading.execution.trader import Trader


@dataclass
class AccountRuntime:
    config: AccountConfig
    trader: Trader
    state_engine: StateEngine


class MultiAccountExecutionManager:
    def __init__(self, accounts: list[AccountConfig]):
        self._runtimes: dict[str, AccountRuntime] = {}
        for acc in accounts:
            if not acc.enabled:
                continue
            eff_dry_run = Config.DRY_RUN if acc.dry_run is None else bool(acc.dry_run)
            trader = Trader(
                dry_run=eff_dry_run,
                account_id=acc.account_id,
                account_name=acc.display_name,
                api_key=acc.api_key,
                api_secret=acc.api_secret,
                account_capital_inr=acc.account_capital_inr,
            )
            self._runtimes[acc.account_id] = AccountRuntime(
                config=acc,
                trader=trader,
                state_engine=StateEngine(trader),
            )
        if not self._runtimes:
            raise ValueError("No enabled Groww accounts configured.")

    def runtimes(self) -> dict[str, AccountRuntime]:
        return dict(self._runtimes)

    # ------------------------------------------------------------------
    # Global control (applies to all accounts)
    # ------------------------------------------------------------------

    def pause_all(self) -> None:
        for rt in self._runtimes.values():
            rt.state_engine.pause()

    def resume_all(self) -> None:
        for rt in self._runtimes.values():
            rt.state_engine.resume()

    def kill_all(self) -> None:
        for rt in self._runtimes.values():
            rt.state_engine.kill()

    def any_active(self) -> bool:
        return any(rt.state_engine.is_active() for rt in self._runtimes.values())

    def status_summary(self) -> str:
        # Derive a single global status: KILLED > PAUSED > ACTIVE
        statuses = {rt.state_engine.status for rt in self._runtimes.values()}
        if "KILLED" in statuses:
            return "KILLED"
        if "PAUSED" in statuses:
            return "PAUSED"
        return "ACTIVE"

    def process_signal(self, signal, *, signal_id: str = "") -> dict[str, str]:
        """
        Fan-out one parsed signal to all enabled account state engines.
        Returns account_id -> result text.
        """
        out: dict[str, str] = {}
        max_workers = max(1, min(4, len(self._runtimes)))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {}
            for account_id, rt in self._runtimes.items():
                action = str(getattr(signal, "action", "") or "").upper()
                if signal_id and action and db.has_execution(signal_id, account_id, action):
                    metrics.increment_labeled("signals_deduped", label=account_id)
                    out[account_id] = f"↩️ Duplicate suppressed for {action} ({signal_id})."
                    continue
                sig_copy = copy.deepcopy(signal)
                futures[pool.submit(rt.state_engine.process, sig_copy)] = (account_id, action)
            for fut in as_completed(futures):
                account_id, action = futures[fut]
                try:
                    res = fut.result() or "No action."
                    metrics.increment_labeled("signals_processed", label=account_id)
                except Exception as e:
                    logger.error("Account %s processing failed: %s", account_id, e, exc_info=True)
                    metrics.increment_labeled("signals_failed", label=account_id)
                    res = f"❌ Execution error: {e}"
                out[account_id] = str(res)
                if signal_id and action and not str(res).startswith("↩️ Duplicate suppressed"):
                    db.record_execution(signal_id, account_id, action)
        return out

    def check_connections(self) -> dict[str, tuple[bool, bool]]:
        out: dict[str, tuple[bool, bool]] = {}
        for account_id, rt in self._runtimes.items():
            conn = rt.trader.check_connection(log_fail=True)
            trading = rt.trader.check_trading_access(log_fail=True) if conn else False
            out[account_id] = (bool(conn), bool(trading))
        return out
