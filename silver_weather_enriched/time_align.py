from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def parse_iso_to_utc(value: Any) -> datetime | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None

    if dt.tzinfo is None:
        # Existing project assumption: timestamps without tz are already UTC-aligned in Silver.
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt


def floor_hour_utc_iso(value: Any) -> str | None:
    dt = parse_iso_to_utc(value)
    if dt is None:
        return None
    floored = dt.replace(minute=0, second=0, microsecond=0)
    return floored.isoformat().replace("+00:00", "Z")


def _round_date_midday_utc(round_date_interp: Any) -> str | None:
    if round_date_interp is None:
        return None
    text = str(round_date_interp).strip()
    if not text:
        return None
    # deterministic fallback anchor at noon UTC for date-only fallback
    return floor_hour_utc_iso(f"{text}T12:00:00Z")


def resolve_round_observation_hour_utc(row: dict[str, Any]) -> str | None:
    # Preferred: tee_time_join_ts
    ts = row.get("tee_time_join_ts")
    hour = floor_hour_utc_iso(ts)
    if hour:
        return hour

    # Fallback: round_date_interp
    return _round_date_midday_utc(row.get("round_date_interp"))


def resolve_hole_observation_hour_utc(row: dict[str, Any]) -> str | None:
    # Preferred order based on likely availability
    for key in ("hole_time_est_ts", "tee_time_join_ts", "round_date_interp"):
        if key == "round_date_interp":
            hour = _round_date_midday_utc(row.get(key))
        else:
            hour = floor_hour_utc_iso(row.get(key))
        if hour:
            return hour
    return None