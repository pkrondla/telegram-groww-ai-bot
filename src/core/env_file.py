from __future__ import annotations

import json
import os
import re
from pathlib import Path

from app.bootstrap import repo_root


def dotenv_path() -> Path:
    custom = os.getenv("DOTENV_PATH", "").strip()
    if custom:
        return Path(custom)
    return repo_root() / ".env"


def _quote_env_value(value: str) -> str:
    if not value:
        return '""'
    if re.fullmatch(r"[\w.\-/+%@:,]+", value):
        return value
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def update_env_var(key: str, value: str, path: Path | None = None) -> bool:
    """Update or append KEY=VALUE in a dotenv file. Returns True if the file was written."""
    env_path = path or dotenv_path()
    key_pattern = re.compile(rf"^{re.escape(key)}=")
    new_line = f"{key}={_quote_env_value(value)}\n"
    lines: list[str] = []

    if env_path.exists():
        lines = env_path.read_text(encoding="utf-8").splitlines(keepends=True)

    found = False
    for index, line in enumerate(lines):
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#"):
            continue
        if key_pattern.match(stripped):
            lines[index] = new_line
            found = True
            break

    if not found:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] += "\n"
        lines.append(new_line)

    env_path.parent.mkdir(parents=True, exist_ok=True)
    env_path.write_text("".join(lines), encoding="utf-8")
    return True


def update_account_capital_in_json(
    raw_json: str,
    capital_inr: float,
    account_id: str | None = None,
) -> tuple[str, list[str]]:
    """Return compact JSON and the account_ids that were updated."""
    data = json.loads(str(raw_json or "").strip())
    if not isinstance(data, list):
        raise ValueError("GROWW_ACCOUNTS_JSON must be a JSON array.")

    cap = max(0.0, float(capital_inr))
    changed: list[str] = []
    target = str(account_id or "").strip().lower()

    for item in data:
        if not isinstance(item, dict):
            continue
        aid = str(item.get("account_id") or "").strip()
        if not aid:
            continue
        if target and aid.lower() != target:
            continue
        item["account_capital_inr"] = cap
        changed.append(aid)

    if target and not changed:
        raise ValueError(f"Unknown account_id '{account_id}'.")

    return json.dumps(data, separators=(",", ":")), changed


def persist_env_bool(key: str, value: bool, path: Path | None = None) -> bool:
    """
    Update a boolean env var in os.environ and in .env when that file exists.

    Returns True when the dotenv file was written.
    """
    text = "true" if value else "false"
    os.environ[str(key)] = text
    env_path = path or dotenv_path()
    if env_path.exists():
        update_env_var(str(key), text, env_path)
        return True
    return False


def persist_account_capital(
    raw_json: str,
    capital_inr: float,
    account_id: str | None = None,
) -> tuple[str, list[str], bool]:
    """
    Update account_capital_inr in JSON, os.environ, and .env when available.

    Returns (new_json, changed_account_ids, env_file_written).
    """
    new_json, changed = update_account_capital_in_json(raw_json, capital_inr, account_id)
    os.environ["GROWW_ACCOUNTS_JSON"] = new_json

    env_file_written = False
    env_path = dotenv_path()
    if env_path.exists():
        update_env_var("GROWW_ACCOUNTS_JSON", new_json, env_path)
        env_file_written = True

    return new_json, changed, env_file_written
