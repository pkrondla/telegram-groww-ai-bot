from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class AccountConfig:
    account_id: str
    display_name: str
    enabled: bool = True
    api_key: str = ""
    api_secret: str = ""
    dry_run: bool | None = None
    account_capital_inr: float = 0.0

    @property
    def safe_name(self) -> str:
        return self.display_name or self.account_id


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    s = str(value).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def parse_accounts_json(raw: str) -> list[AccountConfig]:
    payload = str(raw or "").strip()
    if not payload:
        return []
    data = json.loads(payload)
    if not isinstance(data, list):
        raise ValueError("GROWW_ACCOUNTS_JSON must be a JSON array.")
    out: list[AccountConfig] = []
    seen: set[str] = set()
    for i, item in enumerate(data):
        if not isinstance(item, dict):
            raise ValueError(f"Account entry at index {i} is not an object.")
        account_id = str(item.get("account_id") or "").strip()
        if not account_id:
            raise ValueError(f"Missing account_id at index {i}.")
        if account_id in seen:
            raise ValueError(f"Duplicate account_id '{account_id}'.")
        seen.add(account_id)
        display_name = str(item.get("display_name") or account_id).strip()
        try:
            cap = float(item.get("account_capital_inr") or 0)
        except Exception:
            cap = 0.0
        out.append(
            AccountConfig(
                account_id=account_id,
                display_name=display_name,
                enabled=_to_bool(item.get("enabled"), True),
                api_key=str(item.get("api_key") or "").strip(),
                api_secret=str(item.get("api_secret") or "").strip(),
                dry_run=(
                    _to_bool(item.get("dry_run"), False)
                    if item.get("dry_run") is not None
                    else None
                ),
                account_capital_inr=max(0.0, cap),
            )
        )
    return out

