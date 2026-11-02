"""Shared helpers: time parsing, numeric coercion, SQL quoting, byte formatting."""
from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any

KB = 1024
MB = KB * 1024
GB = MB * 1024
TB = GB * 1024


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def to_dt(v: Any) -> datetime | None:
    """Parse datetime / date / epoch (s or ms) / ISO string into an aware UTC datetime."""
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day, tzinfo=timezone.utc)
    if isinstance(v, (int, float)):
        ts = float(v)
        if ts > 1e11:  # epoch millis
            ts /= 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            return to_dt(int(s))
        s = s.replace("Z", "+00:00")
        if " " in s and "T" not in s:
            s = s.replace(" ", "T", 1)
        try:
            return to_dt(datetime.fromisoformat(s))
        except ValueError:
            return None
    return None


def days_since(dt: datetime | None, now: datetime | None = None) -> float | None:
    if dt is None:
        return None
    now = now or utcnow()
    return max(0.0, (now - dt).total_seconds() / 86400.0)


def to_float(v: Any, default: float | None = None) -> float | None:
    if v is None or v == "":
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def to_int(v: Any, default: int | None = None) -> int | None:
    f = to_float(v)
    return default if f is None else int(f)


_INTERVAL_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(week|day|hour|minute|second)s?", re.I)
_UNIT_DAYS = {"week": 7.0, "day": 1.0, "hour": 1 / 24, "minute": 1 / 1440, "second": 1 / 86400}


def parse_interval_days(v: Any, default: float) -> float:
    """Parse Delta interval strings like 'interval 7 days' / '168 hours' into days."""
    if v is None:
        return default
    matches = _INTERVAL_RE.findall(str(v))
    if not matches:
        return default
    return sum(float(n) * _UNIT_DAYS[u.lower()] for n, u in matches)


def quote_ident(part: str) -> str:
    return "`" + str(part).strip("`").replace("`", "``") + "`"


def quote_fqn(name: str) -> str:
    """catalog.schema.table -> `catalog`.`schema`.`table`"""
    return ".".join(quote_ident(p) for p in str(name).split("."))


def sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return repr(v)
    s = str(v).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def human_bytes(n: float | None) -> str:
    if n is None:
        return "n/a"
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(n) < 1024 or unit == "PB":
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} PB"
