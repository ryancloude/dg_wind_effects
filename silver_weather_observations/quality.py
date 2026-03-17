from __future__ import annotations

from typing import Any

from silver_weather_observations.models import LINEAGE_REQUIRED_COLS


def _as_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = _as_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _append_error(errors: list[dict[str, Any]], *, rule: str, message: str, sample: dict[str, Any] | None = None) -> None:
    payload = {"rule": rule, "message": message}
    if sample is not None:
        payload["sample"] = sample
    errors.append(payload)


def validate_quality(observation_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []

    if not observation_rows:
        _append_error(
            errors,
            rule="non_empty_dataset",
            message="normalized observations set is empty",
        )
        return errors

    # Not-null checks for core PK fields + timestamp
    required_not_null = (
        "weather_obs_pk",
        "event_id",
        "round_number",
        "provider",
        "source_id",
        "observation_hour_utc",
    )
    for col in required_not_null:
        bad = [r for r in observation_rows if _as_text(r.get(col)) == ""]
        if bad:
            _append_error(
                errors,
                rule=f"not_null:{col}",
                message=f"column '{col}' contains null/blank values",
                sample={k: bad[0].get(k) for k in required_not_null},
            )

    # Lineage required columns
    for col in LINEAGE_REQUIRED_COLS:
        bad = [r for r in observation_rows if _as_text(r.get(col)) == ""]
        if bad:
            _append_error(
                errors,
                rule=f"lineage_required:{col}",
                message=f"lineage column '{col}' contains null/blank values",
                sample={"weather_obs_pk": bad[0].get("weather_obs_pk"), col: bad[0].get(col)},
            )

    # Uniqueness on weather_obs_pk
    seen: set[str] = set()
    dupes = 0
    dupe_sample: dict[str, Any] | None = None
    for row in observation_rows:
        pk = _as_text(row.get("weather_obs_pk"))
        if not pk:
            continue
        if pk in seen:
            dupes += 1
            if dupe_sample is None:
                dupe_sample = {
                    "weather_obs_pk": pk,
                    "event_id": row.get("event_id"),
                    "round_number": row.get("round_number"),
                    "observation_hour_utc": row.get("observation_hour_utc"),
                }
        else:
            seen.add(pk)

    if dupes > 0:
        _append_error(
            errors,
            rule="unique:weather_obs_pk",
            message=f"duplicate weather_obs_pk rows detected (count={dupes})",
            sample=dupe_sample,
        )

    # Numeric range checks
    def _range_check(col: str, *, min_value: float | None, max_value: float | None) -> None:
        bad_rows: list[dict[str, Any]] = []
        for row in observation_rows:
            value = _as_float(row.get(col))
            if value is None:
                continue
            if min_value is not None and value < min_value:
                bad_rows.append(row)
                continue
            if max_value is not None and value > max_value:
                bad_rows.append(row)
                continue

        if bad_rows:
            _append_error(
                errors,
                rule=f"range:{col}",
                message=f"column '{col}' contains out-of-range values",
                sample={
                    "weather_obs_pk": bad_rows[0].get("weather_obs_pk"),
                    col: bad_rows[0].get(col),
                },
            )

    _range_check("relative_humidity_pct", min_value=0.0, max_value=100.0)
    _range_check("wind_speed_mps", min_value=0.0, max_value=None)
    _range_check("wind_gust_mps", min_value=0.0, max_value=None)
    _range_check("wind_dir_deg", min_value=0.0, max_value=360.0)
    _range_check("precip_mm", min_value=0.0, max_value=None)

    # Completeness: at least one hourly row per (event_id, round_number, provider, source_id)
    groups: dict[tuple[Any, Any, Any, Any], int] = {}
    for row in observation_rows:
        key = (
            row.get("event_id"),
            row.get("round_number"),
            row.get("provider"),
            row.get("source_id"),
        )
        groups[key] = groups.get(key, 0) + 1

    empty_groups = [k for k, count in groups.items() if count <= 0]
    if empty_groups:
        _append_error(
            errors,
            rule="completeness:per_round_source",
            message="one or more round-source groups have no rows",
            sample={"group": empty_groups[0]},
        )

    return errors