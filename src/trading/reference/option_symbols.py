"""NSE/Groww index option symbol helpers (weekly month codes + monthly expiry day)."""
from __future__ import annotations

import datetime
import re

# Weekly expiry weekday per index (0=Mon … 4=Fri). Source: NSE/BSE circulars.
INDEX_EXPIRY_WEEKDAY: dict[str, int] = {
    "NIFTY": 1,  # Tuesday
    "FINNIFTY": 1,  # Tuesday
    "MIDCPNIFTY": 0,  # Monday
    "BANKNIFTY": 3,  # Thursday
    "SENSEX": 3,  # Thursday
    "BANKEX": 0,  # Monday
}

INDEX_UNDERLYINGS = frozenset(INDEX_EXPIRY_WEEKDAY.keys())

_MONTH_ABBR = {
    1: "JAN",
    2: "FEB",
    3: "MAR",
    4: "APR",
    5: "MAY",
    6: "JUN",
    7: "JUL",
    8: "AUG",
    9: "SEP",
    10: "OCT",
    11: "NOV",
    12: "DEC",
}

# NSE weekly month codes after YY: Jan–Sep digits; Oct/Nov/Dec letters.
_WEEKLY_MONTH_TOKEN = {
    1: "1",
    2: "2",
    3: "3",
    4: "4",
    5: "5",
    6: "6",
    7: "7",
    8: "8",
    9: "9",
    10: "O",
    11: "N",
    12: "D",
}

_WEEKLY_TOKEN_TO_MONTH = {v: k for k, v in _WEEKLY_MONTH_TOKEN.items()}


def weekly_month_token(month_num: int) -> str:
    """Return NSE weekly month token for calendar month 1–12."""
    mm = int(month_num)
    token = _WEEKLY_MONTH_TOKEN.get(mm)
    if not token:
        raise ValueError(f"invalid month_num for weekly token: {month_num!r}")
    return token


def weekly_token_to_month(token: str) -> int | None:
    """Map weekly month token ('1'..'9', 'O','N','D', or legacy '10'..'12') to month num."""
    t = str(token or "").upper().strip()
    if t in _WEEKLY_TOKEN_TO_MONTH:
        return _WEEKLY_TOKEN_TO_MONTH[t]
    if t in ("10", "11", "12"):
        return int(t)
    if t.isdigit() and 1 <= int(t) <= 9:
        return int(t)
    return None


def month_abbr(month_num: int) -> str:
    return _MONTH_ABBR[int(month_num)]


def last_weekday_of_month(year: int, month: int, weekday: int) -> datetime.date:
    """Last date in (year, month) whose weekday matches (0=Mon … 6=Sun)."""
    if month == 12:
        d = datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)
    else:
        d = datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
    while d.weekday() != int(weekday):
        d -= datetime.timedelta(days=1)
    return d


def index_monthly_expiry_date(underlying: str, year: int, month: int) -> datetime.date | None:
    """
    Nominal monthly expiry = last weekly-expiry weekday of that month.
    Holiday shifts may move the real contract earlier; callers that can read
    Groww instruments should still confirm expiry_date when promoting monthly.
    """
    u = str(underlying or "").upper().strip()
    wd = INDEX_EXPIRY_WEEKDAY.get(u)
    if wd is None:
        return None
    try:
        return last_weekday_of_month(int(year), int(month), wd)
    except (TypeError, ValueError):
        return None


def is_index_monthly_expiry_day(
    underlying: str,
    *,
    day: int,
    month: int,
    year: int,
) -> bool:
    """True when (day, month, year) matches the nominal monthly expiry weekday."""
    expected = index_monthly_expiry_date(underlying, year, month)
    if expected is None:
        return False
    try:
        return expected == datetime.date(int(year), int(month), int(day))
    except ValueError:
        return False


def build_index_weekly_symbol(
    underlying: str,
    year_yy: str,
    month_num: int,
    day: int,
    strike: str,
    option_type: str,
) -> str:
    """Build canonical Groww/NSE index weekly trading symbol."""
    u = str(underlying or "").upper().strip()
    yy = str(year_yy or "")[-2:].zfill(2)
    token = weekly_month_token(month_num)
    dd = str(int(day)).zfill(2)
    strike_s = re.sub(r"[^\d]", "", str(strike or ""))
    ot = str(option_type or "").upper().strip()
    return f"{u}{yy}{token}{dd}{strike_s}{ot}"


def build_index_monthly_symbol(
    underlying: str,
    year_yy: str,
    month_num: int,
    strike: str,
    option_type: str,
) -> str:
    """Build Groww/NSE index monthly (alpha month) trading symbol."""
    u = str(underlying or "").upper().strip()
    yy = str(year_yy or "")[-2:].zfill(2)
    mon = month_abbr(month_num)
    strike_s = re.sub(r"[^\d]", "", str(strike or ""))
    ot = str(option_type or "").upper().strip()
    return f"{u}{yy}{mon}{strike_s}{ot}"


def parse_expiry_date_field(raw) -> datetime.date | None:
    """Parse Groww instrument expiry_date (YYYY-MM-DD or datetime-like)."""
    if raw is None:
        return None
    if isinstance(raw, datetime.datetime):
        return raw.date()
    if isinstance(raw, datetime.date):
        return raw
    s = str(raw).strip()
    if not s:
        return None
    try:
        return datetime.date.fromisoformat(s[:10])
    except ValueError:
        return None
