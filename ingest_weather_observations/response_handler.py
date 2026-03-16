from __future__ import annotations

from datetime import date
from typing import Any, Mapping

from ingest_weather_observations.utils import sha256_obj
from ingest_weather_observations.windowing import (
    build_daylight_bounds_by_date,
    filter_hourly_rows_to_daylight,
)


def compute_payload_sha256(payload: dict[str, Any]) -> str:
    return sha256_obj(payload)


def _expand_hourly(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    hourly = payload.get("hourly")
    if not isinstance(hourly, Mapping):
        return []

    times = hourly.get("time")
    if not isinstance(times, list):
        return []

    out: list[dict[str, Any]] = []
    for i, ts in enumerate(times):
        row: dict[str, Any] = {"time": ts}
        for key, values in hourly.items():
            if key == "time":
                continue
            if isinstance(values, list) and i < len(values):
                row[key] = values[i]
            else:
                row[key] = None
        out.append(row)
    return out


def _expand_daily(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    daily = payload.get("daily")
    if not isinstance(daily, Mapping):
        return []

    times = daily.get("time")
    if not isinstance(times, list):
        return []

    out: list[dict[str, Any]] = []
    for i, day in enumerate(times):
        row: dict[str, Any] = {"time": day}
        for key, values in daily.items():
            if key == "time":
                continue
            if isinstance(values, list) and i < len(values):
                row[key] = values[i]
            else:
                row[key] = None
        out.append(row)
    return out


def extract_daylight_hourly_rows(
    *,
    payload: Mapping[str, Any],
    target_dates: set[date] | None = None,
) -> list[dict[str, Any]]:
    hourly_rows = _expand_hourly(payload)
    daily_rows = _expand_daily(payload)

    bounds = build_daylight_bounds_by_date(daily_rows, sunrise_key="sunrise", sunset_key="sunset")
    daylight_rows = filter_hourly_rows_to_daylight(hourly_rows, daylight_bounds_by_date=bounds, time_key="time")

    if not target_dates:
        return daylight_rows

    out: list[dict[str, Any]] = []
    for row in daylight_rows:
        ts = str(row.get("time", ""))
        day = ts[:10]
        if len(day) == 10:
            try:
                yyyy, mm, dd = day.split("-")
                d = date(int(yyyy), int(mm), int(dd))
            except Exception:
                continue
            if d in target_dates:
                out.append(row)
    return out