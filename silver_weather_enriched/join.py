from __future__ import annotations

import hashlib
import json
from typing import Any

from silver_weather_enriched.models import (
    ENRICHED_HOLE_WEATHER_COLS,
    ENRICHED_ROUND_WEATHER_COLS,
    JOIN_POLICY_VERSION,
)
from silver_weather_enriched.time_align import (
    resolve_hole_observation_hour_utc,
    resolve_round_observation_hour_utc,
)

WEATHER_TIEBREAK_COLS = (
    "source_fetched_at_utc",
    "source_json_key",
    "weather_obs_pk",
)


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


def _is_newer_weather(candidate: dict[str, Any], current: dict[str, Any]) -> bool:
    cand_rank = tuple(_as_text(candidate.get(col)) for col in WEATHER_TIEBREAK_COLS)
    curr_rank = tuple(_as_text(current.get(col)) for col in WEATHER_TIEBREAK_COLS)
    return cand_rank > curr_rank


def build_weather_lookup(weather_rows: list[dict[str, Any]]) -> dict[tuple[int, int, str], dict[str, Any]]:
    """
    Build deterministic lookup keyed by:
      (event_id, round_number, observation_hour_utc)
    """
    best: dict[tuple[int, int, str], dict[str, Any]] = {}

    for row in weather_rows:
        try:
            event_id = int(row.get("event_id"))
            round_number = int(row.get("round_number"))
        except (TypeError, ValueError):
            continue

        observation_hour_utc = _as_text(row.get("observation_hour_utc"))
        if not observation_hour_utc:
            continue

        key = (event_id, round_number, observation_hour_utc)
        current = best.get(key)
        if current is None or _is_newer_weather(row, current):
            best[key] = row

    return best


def _weather_cols_from_row(weather_row: dict[str, Any] | None, observation_hour_utc: str | None) -> dict[str, Any]:
    if weather_row is None:
        return {
            "wx_observation_hour_utc": observation_hour_utc or "",
            "wx_wind_speed_mps": None,
            "wx_wind_gust_mps": None,
            "wx_wind_dir_deg": None,
            "wx_temperature_c": None,
            "wx_pressure_hpa": None,
            "wx_relative_humidity_pct": None,
            "wx_precip_mm": None,
            "wx_provider": "",
            "wx_source_id": "",
            "wx_source_json_key": "",
            "wx_source_content_sha256": "",
            "wx_weather_obs_pk": "",
            "wx_weather_missing_flag": True,
        }

    return {
        "wx_observation_hour_utc": observation_hour_utc or _as_text(weather_row.get("observation_hour_utc")),
        "wx_wind_speed_mps": weather_row.get("wind_speed_mps"),
        "wx_wind_gust_mps": weather_row.get("wind_gust_mps"),
        "wx_wind_dir_deg": weather_row.get("wind_dir_deg"),
        "wx_temperature_c": weather_row.get("temperature_c"),
        "wx_pressure_hpa": weather_row.get("pressure_hpa"),
        "wx_relative_humidity_pct": weather_row.get("relative_humidity_pct"),
        "wx_precip_mm": weather_row.get("precip_mm"),
        "wx_provider": _as_text(weather_row.get("provider")),
        "wx_source_id": _as_text(weather_row.get("source_id")),
        "wx_source_json_key": _as_text(weather_row.get("source_json_key")),
        "wx_source_content_sha256": _as_text(weather_row.get("source_content_sha256")),
        "wx_weather_obs_pk": _as_text(weather_row.get("weather_obs_pk")),
        "wx_weather_missing_flag": False,
    }


def _merge_with_weather(row: dict[str, Any], weather_cols: dict[str, Any], expected_cols: tuple[str, ...]) -> dict[str, Any]:
    out = dict(row)
    # Ensure all expected weather cols always exist
    for col in expected_cols:
        out[col] = weather_cols.get(col)
    return out


def enrich_player_round_rows(
    round_rows: list[dict[str, Any]],
    weather_lookup: dict[tuple[int, int, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    for row in round_rows:
        try:
            event_id = int(row.get("tourn_id") if row.get("tourn_id") is not None else row.get("event_id"))
            round_number = int(row.get("round_number"))
        except (TypeError, ValueError):
            weather_cols = _weather_cols_from_row(None, None)
            out.append(_merge_with_weather(row, weather_cols, ENRICHED_ROUND_WEATHER_COLS))
            continue

        observation_hour_utc = resolve_round_observation_hour_utc(row)
        weather_row = None
        if observation_hour_utc:
            weather_row = weather_lookup.get((event_id, round_number, observation_hour_utc))

        weather_cols = _weather_cols_from_row(weather_row, observation_hour_utc)
        out.append(_merge_with_weather(row, weather_cols, ENRICHED_ROUND_WEATHER_COLS))

    return out


def enrich_player_hole_rows(
    hole_rows: list[dict[str, Any]],
    weather_lookup: dict[tuple[int, int, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    for row in hole_rows:
        try:
            event_id = int(row.get("tourn_id") if row.get("tourn_id") is not None else row.get("event_id"))
            round_number = int(row.get("round_number"))
        except (TypeError, ValueError):
            weather_cols = _weather_cols_from_row(None, None)
            out.append(_merge_with_weather(row, weather_cols, ENRICHED_HOLE_WEATHER_COLS))
            continue

        observation_hour_utc = resolve_hole_observation_hour_utc(row)
        weather_row = None
        if observation_hour_utc:
            weather_row = weather_lookup.get((event_id, round_number, observation_hour_utc))

        weather_cols = _weather_cols_from_row(weather_row, observation_hour_utc)
        out.append(_merge_with_weather(row, weather_cols, ENRICHED_HOLE_WEATHER_COLS))

    return out


def compute_enriched_event_fingerprint(
    *,
    round_rows: list[dict[str, Any]],
    hole_rows: list[dict[str, Any]],
    weather_rows: list[dict[str, Any]],
    join_policy_version: str = JOIN_POLICY_VERSION,
) -> str:
    """
    Deterministic event fingerprint for checkpointing incremental/idempotent runs.
    """
    def _project_round(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "tourn_id": r.get("tourn_id"),
            "round_number": r.get("round_number"),
            "player_key": r.get("player_key"),
            "tee_time_join_ts": r.get("tee_time_join_ts"),
            "round_date_interp": r.get("round_date_interp"),
            "source_json_key": r.get("source_json_key"),
            "source_content_sha256": r.get("source_content_sha256"),
        }

    def _project_hole(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "tourn_id": r.get("tourn_id"),
            "round_number": r.get("round_number"),
            "hole_number": r.get("hole_number"),
            "player_key": r.get("player_key"),
            "hole_time_est_ts": r.get("hole_time_est_ts"),
            "tee_time_join_ts": r.get("tee_time_join_ts"),
            "round_date_interp": r.get("round_date_interp"),
            "source_json_key": r.get("source_json_key"),
            "source_content_sha256": r.get("source_content_sha256"),
        }

    def _project_weather(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "event_id": r.get("event_id"),
            "round_number": r.get("round_number"),
            "observation_hour_utc": r.get("observation_hour_utc"),
            "provider": r.get("provider"),
            "source_id": r.get("source_id"),
            "weather_obs_pk": r.get("weather_obs_pk"),
            "source_json_key": r.get("source_json_key"),
            "source_content_sha256": r.get("source_content_sha256"),
            "source_fetched_at_utc": r.get("source_fetched_at_utc"),
        }

    round_proj = sorted((_project_round(r) for r in round_rows), key=lambda x: json.dumps(x, sort_keys=True))
    hole_proj = sorted((_project_hole(r) for r in hole_rows), key=lambda x: json.dumps(x, sort_keys=True))
    weather_proj = sorted((_project_weather(r) for r in weather_rows), key=lambda x: json.dumps(x, sort_keys=True))

    payload = {
        "join_policy_version": join_policy_version,
        "round_rows": round_proj,
        "hole_rows": hole_proj,
        "weather_rows": weather_proj,
    }

    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()