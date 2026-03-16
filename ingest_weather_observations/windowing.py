from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping

from ingest_weather_observations.models import WeatherFetchWindow


def _parse_iso_date(raw: str | None) -> date | None:
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _coerce_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return _parse_iso_date(value)
    return None


def _parse_iso_datetime_local(raw: Any) -> datetime | None:
    """
    Parse local wall-clock datetime values from Silver/Open-Meteo style strings.

    Assumption:
    - tee_time_join_ts is already local time for the event.
    - If an offset/Z is present, we parse it and then drop tzinfo so comparisons
      are done as local wall-clock values consistently.
    """
    if raw is None:
        return None

    if isinstance(raw, datetime):
        dt = raw
    else:
        text = str(raw).strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None

    if dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt


def build_round_date_overrides_from_silver_rows(
    player_round_rows: Iterable[Mapping[str, Any]],
    *,
    round_number_key: str = "round_number",
    round_date_key: str = "round_date_interp",
) -> dict[int, date]:
    """
    Build deterministic per-round date overrides from Silver player_round rows.

    Strategy:
    - Group by round_number and candidate round_date_interp
    - Pick most frequent date per round
    - On frequency tie, pick earliest date (deterministic)
    """
    counts: dict[int, dict[date, int]] = defaultdict(lambda: defaultdict(int))

    for row in player_round_rows:
        raw_round = row.get(round_number_key)
        try:
            round_number = int(raw_round)
        except (TypeError, ValueError):
            continue
        if round_number <= 0:
            continue

        round_dt = _coerce_date(row.get(round_date_key))
        if round_dt is None:
            continue

        counts[round_number][round_dt] += 1

    out: dict[int, date] = {}
    for round_number, date_counts in counts.items():
        ranked = sorted(date_counts.items(), key=lambda x: (-x[1], x[0].isoformat()))
        out[round_number] = ranked[0][0]
    return out


def extract_local_play_dates_from_tee_times(
    rows: Iterable[Mapping[str, Any]],
    *,
    tee_time_key: str = "tee_time_join_ts",
) -> list[date]:
    """
    Return sorted unique local play dates derived from tee_time_join_ts.
    """
    dates: set[date] = set()
    for row in rows:
        dt = _parse_iso_datetime_local(row.get(tee_time_key))
        if dt is None:
            continue
        dates.add(dt.date())
    return sorted(dates)


def build_round_date_overrides_from_tee_times(
    rows: Iterable[Mapping[str, Any]],
    *,
    round_number_key: str = "round_number",
    tee_time_key: str = "tee_time_join_ts",
) -> dict[int, date]:
    """
    Build per-round date overrides using tee_time_join_ts values.

    Strategy:
    - Group tee_time local dates by round_number
    - Pick most frequent date
    - Tie-break by earliest date for deterministic output
    """
    counts: dict[int, dict[date, int]] = defaultdict(lambda: defaultdict(int))

    for row in rows:
        raw_round = row.get(round_number_key)
        try:
            round_number = int(raw_round)
        except (TypeError, ValueError):
            continue
        if round_number <= 0:
            continue

        dt = _parse_iso_datetime_local(row.get(tee_time_key))
        if dt is None:
            continue

        counts[round_number][dt.date()] += 1

    out: dict[int, date] = {}
    for round_number, date_counts in counts.items():
        ranked = sorted(date_counts.items(), key=lambda x: (-x[1], x[0].isoformat()))
        out[round_number] = ranked[0][0]
    return out


def build_fetch_date_span_from_play_dates(
    play_dates: Iterable[date],
    *,
    padding_days: int = 0,
) -> tuple[date, date] | None:
    if padding_days < 0:
        raise ValueError("padding_days must be >= 0")

    unique = sorted(set(play_dates))
    if not unique:
        return None

    start_date = unique[0] - timedelta(days=padding_days)
    end_date = unique[-1] + timedelta(days=padding_days)
    return start_date, end_date


def build_fetch_date_span_from_tee_times(
    rows: Iterable[Mapping[str, Any]],
    *,
    tee_time_key: str = "tee_time_join_ts",
    padding_days: int = 0,
) -> tuple[date, date] | None:
    play_dates = extract_local_play_dates_from_tee_times(rows, tee_time_key=tee_time_key)
    return build_fetch_date_span_from_play_dates(play_dates, padding_days=padding_days)


def build_daylight_bounds_by_date(
    daily_rows: Iterable[Mapping[str, Any]],
    *,
    sunrise_key: str = "sunrise",
    sunset_key: str = "sunset",
) -> dict[date, tuple[datetime, datetime]]:
    """
    Build {date: (sunrise_dt, sunset_dt)} from Open-Meteo daily rows.
    """
    out: dict[date, tuple[datetime, datetime]] = {}

    for row in daily_rows:
        sunrise = _parse_iso_datetime_local(row.get(sunrise_key))
        sunset = _parse_iso_datetime_local(row.get(sunset_key))
        if sunrise is None or sunset is None:
            continue
        if sunset <= sunrise:
            continue
        out[sunrise.date()] = (sunrise, sunset)

    return out


def filter_hourly_rows_to_daylight(
    hourly_rows: Iterable[Mapping[str, Any]],
    *,
    daylight_bounds_by_date: Mapping[date, tuple[datetime, datetime]],
    time_key: str = "time",
) -> list[dict[str, Any]]:
    """
    Keep hourly rows that overlap [sunrise, sunset) local daylight window.

    Hour row interval is treated as [hour_start, hour_start + 1h).
    """
    out: list[dict[str, Any]] = []

    for row in hourly_rows:
        hour_start = _parse_iso_datetime_local(row.get(time_key))
        if hour_start is None:
            continue

        bounds = daylight_bounds_by_date.get(hour_start.date())
        if not bounds:
            continue

        sunrise, sunset = bounds
        hour_end = hour_start + timedelta(hours=1)

        # Interval overlap test for partial sunrise/sunset hours.
        if hour_start < sunset and hour_end > sunrise:
            out.append(dict(row))

    return out


def infer_round_date(
    *,
    start_date_str: str | None,
    end_date_str: str | None,
    round_number: int,
    max_rounds: int,
    round_date_overrides: Mapping[int, date | str] | None = None,
) -> date:
    if round_number <= 0:
        raise ValueError("round_number must be >= 1")
    if max_rounds <= 0:
        raise ValueError("max_rounds must be >= 1")
    if round_number > max_rounds:
        raise ValueError("round_number cannot exceed max_rounds")

    if round_date_overrides:
        override = _coerce_date(round_date_overrides.get(round_number))
        if override is not None:
            return override

    start_date = _parse_iso_date(start_date_str)
    end_date = _parse_iso_date(end_date_str)

    if start_date and end_date:
        if end_date < start_date:
            end_date = start_date
        span_days = (end_date - start_date).days
        offset_days = min(round_number - 1, span_days)
        return start_date + timedelta(days=offset_days)

    if start_date:
        return start_date + timedelta(days=round_number - 1)

    if end_date:
        backfill_days = max_rounds - round_number
        return end_date - timedelta(days=backfill_days)

    raise ValueError("cannot infer round date without start_date or end_date")


def build_fetch_window(*, round_number: int, round_date: date, padding_days: int = 1) -> WeatherFetchWindow:
    if padding_days < 0:
        raise ValueError("padding_days must be >= 0")
    return WeatherFetchWindow(
        round_number=round_number,
        round_date=round_date,
        start_date=round_date - timedelta(days=padding_days),
        end_date=round_date + timedelta(days=padding_days),
    )


def infer_round_dates(
    *,
    start_date_str: str | None,
    end_date_str: str | None,
    max_rounds: int,
    padding_days: int = 1,
    round_date_overrides: Mapping[int, date | str] | None = None,
) -> list[WeatherFetchWindow]:
    if max_rounds <= 0:
        return []

    windows: list[WeatherFetchWindow] = []
    for round_number in range(1, max_rounds + 1):
        round_date = infer_round_date(
            start_date_str=start_date_str,
            end_date_str=end_date_str,
            round_number=round_number,
            max_rounds=max_rounds,
            round_date_overrides=round_date_overrides,
        )
        windows.append(build_fetch_window(round_number=round_number, round_date=round_date, padding_days=padding_days))
    return windows