from __future__ import annotations

import copy
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.trading.execution.accounts import AccountConfig
from src.core.config import Config
from src.core import env_file
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

    def investment_summary(self) -> str:
        lines = ["Account investment (account_capital_inr)"]
        for account_id, rt in sorted(self._runtimes.items()):
            cap = float(rt.trader.account_capital_inr or 0.0)
            per_trade = cap * float(Config.RISK_PCT_PER_TRADE)
            portfolio = cap * float(Config.MAX_TOTAL_OPEN_RISK_PCT)
            lines.append(
                f"[{account_id}] {rt.config.display_name}\n"
                f"  capital: INR {cap:,.0f}\n"
                f"  per-trade budget: INR {per_trade:,.0f} "
                f"({float(Config.RISK_PCT_PER_TRADE):.2%})\n"
                f"  portfolio budget: INR {portfolio:,.0f} "
                f"({float(Config.MAX_TOTAL_OPEN_RISK_PCT):.2%})"
            )
        lines.append(
            "\nUpdate with INVESTMENT <AMOUNT> (all accounts) or "
            "INVESTMENT <ACCOUNT_ID> <AMOUNT>."
        )
        return "\n\n".join(lines)

    def update_investment(
        self,
        capital_inr: float,
        account_id: str | None = None,
    ) -> str:
        new_json, changed_ids, env_file_written = env_file.persist_account_capital(
            Config.GROWW_ACCOUNTS_JSON,
            capital_inr,
            account_id,
        )
        Config.GROWW_ACCOUNTS_JSON = new_json
        cap = max(0.0, float(capital_inr))
        targets = {aid.lower() for aid in changed_ids}
        for aid, rt in self._runtimes.items():
            if aid.lower() in targets:
                rt.config.account_capital_inr = cap
                rt.trader.account_capital_inr = cap

        scope = account_id or "all accounts"
        persist_note = (
            f"Saved to {env_file.dotenv_path()}."
            if env_file_written
            else f"No .env file at {env_file.dotenv_path()} — applied for this process only."
        )
        changed_text = ", ".join(changed_ids)
        return (
            "Investment updated\n"
            f"Scope: {scope}\n"
            f"New capital: INR {cap:,.0f}\n"
            f"Accounts: {changed_text}\n"
            f"{persist_note}\n"
            "Applies immediately to risk-based sizing for new entries."
        )

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
