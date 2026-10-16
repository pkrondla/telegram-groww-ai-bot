from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class AccountConfig:
    account_id: str
    display_name: str
    enabled: bool = True
    # Groww "TOTP token" (Groww Cloud API Keys > Generate TOTP token). Sent as the bearer
    # credential when exchanging a TOTP code for the daily access token.
    api_key: str = ""
    # Base32 secret that goes with the TOTP token (shown once by Groww; or the QR secret).
    totp_secret: str = ""
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


def normalize_totp_secret(value: Any) -> str:
    """Strip whitespace/dashes and upper-case a base32 TOTP secret."""
    return "".join(str(value or "").split()).replace("-", "").upper()


def is_valid_totp_secret(secret: str) -> bool:
    """True when *secret* is non-empty, valid base32 (what pyotp expects)."""
    s = normalize_totp_secret(secret)
    if not s:
        return False
    try:
        base64.b32decode(s + "=" * (-len(s) % 8), casefold=True)
    except (binascii.Error, ValueError):
        return False
    return True


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
        enabled = _to_bool(item.get("enabled"), True)
        totp_secret = normalize_totp_secret(item.get("totp_secret"))
        if enabled:
            if str(item.get("api_secret") or "").strip() and not totp_secret:
                raise ValueError(
                    f"Account '{account_id}' uses the old api_key/api_secret login, which is no "
                    "longer supported (it needed a daily manual approval). In Groww Cloud API Keys "
                    "use 'Generate TOTP token', then set \"api_key\" to the TOTP token and add "
                    "\"totp_secret\" with its secret; remove \"api_secret\"."
                )
            if totp_secret and not is_valid_totp_secret(totp_secret):
                raise ValueError(
                    f"Account '{account_id}' has an invalid totp_secret (expected a base32 string)."
                )
        out.append(
            AccountConfig(
                account_id=account_id,
                display_name=display_name,
                enabled=enabled,
                api_key=str(item.get("api_key") or "").strip(),
                totp_secret=totp_secret,
                dry_run=(
                    _to_bool(item.get("dry_run"), False)
                    if item.get("dry_run") is not None
                    else None
                ),
                account_capital_inr=max(0.0, cap),
            )
        )
    return out
