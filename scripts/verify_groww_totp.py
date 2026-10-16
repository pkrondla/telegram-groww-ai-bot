"""
Verify the Groww TOTP credentials in GROWW_ACCOUNTS_JSON (read from the environment / .env).

For every enabled account it performs the real TOTP login, then calls the profile and FNO
positions endpoints. Secrets and tokens are never printed.

Usage:  python scripts/verify_groww_totp.py
"""
from __future__ import annotations

import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core.config import Config  # noqa: E402
from src.trading.execution.trader import Trader  # noqa: E402


def main() -> int:
    try:
        accounts = [a for a in Config.groww_accounts() if a.enabled]
    except Exception as e:
        print(f"CONFIG ERROR: {e}")
        return 2

    failed = 0
    for acc in accounts:
        trader = Trader(
            dry_run=True,
            account_id=acc.account_id,
            account_name=acc.display_name,
            api_key=acc.api_key,
            totp_secret=acc.totp_secret,
        )
        login = trader.client is not None
        profile = trader.check_connection(log_fail=False) if login else False
        trading = trader.check_trading_access(log_fail=False) if profile else False
        print(
            f"[{acc.account_id}] login={'OK' if login else 'FAIL'} "
            f"profile={'OK' if profile else 'FAIL'} "
            f"trading_endpoint={'OK' if trading else 'FAIL'}"
        )
        if not (login and profile and trading):
            failed += 1
    if failed:
        print(
            "\nSome checks failed. Re-check api_key (TOTP token) / totp_secret, the server clock, "
            "and the static-IP whitelist (trading_endpoint)."
        )
        return 1
    print("\nAll accounts OK. No daily approval is needed with TOTP.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
