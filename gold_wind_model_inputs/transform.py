from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from gold_wind_model_inputs.models import MODEL_INPUTS_POLICY_VERSION


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _wind_speed_bucket(speed: float | None) -> str:
    if speed is None:
        return "unknown"
    if speed < 2.0:
        return "calm"
    if speed < 5.0:
        return "light"
    if speed < 8.0:
        return "moderate"
    if speed < 12.0:
        return "strong"
    return "very_strong"


def _wind_gust_bucket(speed: float | None) -> str:
    if speed is None:
        return "unknown"
    if speed < 3.0:
        return "low"
    if speed < 6.0:
        return "mild"
    if speed < 10.0:
        return "high"
    return "very_high"


def _json_hash(payload: dict[str, Any]) -> str:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _first_non_null(rows: list[dict[str, Any]], field: str) -> Any:
    for row in rows:
        value = row.get(field)
        if value is not None and value != "":
            return value
    return None


def _sum_int(rows: list[dict[str, Any]], field: str) -> int | None:
    values = [_to_int(r.get(field)) for r in rows]
    values = [v for v in values if v is not None]
    if not values:
        return None
    return int(sum(values))


def _sum_float(rows: list[dict[str, Any]], field: str) -> float | None:
    values = [_to_float(r.get(field)) for r in rows]
    values = [v for v in values if v is not None]
    if not values:
        return None
    return float(sum(values))


def _mean_float(rows: list[dict[str, Any]], field: str) -> float | None:
    values = [_to_float(r.get(field)) for r in rows]
    values = [v for v in values if v is not None]
    if not values:
        return None
    return float(sum(values) / len(values))


def _max_float(rows: list[dict[str, Any]], field: str) -> float | None:
    values = [_to_float(r.get(field)) for r in rows]
    values = [v for v in values if v is not None]
    if not values:
        return None
    return float(max(values))


def _distinct_count(rows: list[dict[str, Any]], field: str) -> int:
    values = {r.get(field) for r in rows if r.get(field) is not None}
    return len(values)


def build_round_model_inputs(
    hole_feature_rows: list[dict[str, Any]],
    *,
    run_id: str,
    processed_at_utc: str | None = None,
    model_inputs_version: str = MODEL_INPUTS_POLICY_VERSION,
) -> list[dict[str, Any]]:
    ts = processed_at_utc or _utc_now_iso()

    grouped: dict[tuple[Any, Any, Any], list[dict[str, Any]]] = defaultdict(list)
    for row in hole_feature_rows:
        grouped[(row.get("tourn_id"), row.get("round_number"), row.get("player_key"))].append(dict(row))

    out: list[dict[str, Any]] = []

    for (tourn_id, round_number, player_key), rows in grouped.items():
        actual_round_strokes = _sum_int(rows, "actual_strokes")
        round_strokes_over_par = _sum_int(rows, "strokes_over_par")

        round_total_hole_length = _sum_float(rows, "hole_length")
        round_total_par = _sum_int(rows, "hole_par")

        if round_strokes_over_par is None and actual_round_strokes is not None and round_total_par is not None:
            round_strokes_over_par = actual_round_strokes - round_total_par

        hole_count = _distinct_count(rows, "hole_number")

        round_avg_hole_length = None
        if round_total_hole_length is not None and hole_count > 0:
            round_avg_hole_length = float(round_total_hole_length / hole_count)

        round_avg_hole_par = None
        if round_total_par is not None and hole_count > 0:
            round_avg_hole_par = float(round_total_par / hole_count)

        round_length_over_par = None
        if round_total_hole_length is not None and round_total_par not in (None, 0):
            round_length_over_par = float(round_total_hole_length / round_total_par)

        weather_flags = [bool(r.get("weather_available_flag")) for r in rows if r.get("weather_available_flag") is not None]
        weather_available_flag = bool(weather_flags) and all(weather_flags)

        round_wind_speed_mps_mean = _mean_float(rows, "wx_wind_speed_mps")
        round_wind_speed_mps_max = _max_float(rows, "wx_wind_speed_mps")
        round_wind_gust_mps_mean = _mean_float(rows, "wx_wind_gust_mps")
        round_wind_gust_mps_max = _max_float(rows, "wx_wind_gust_mps")
        round_temp_c_mean = _mean_float(rows, "wx_temperature_c")
        round_precip_mm_sum = _sum_float(rows, "wx_precip_mm")
        round_precip_mm_mean = _mean_float(rows, "wx_precip_mm")
        round_pressure_hpa_mean = _mean_float(rows, "wx_pressure_hpa")
        round_humidity_pct_mean = _mean_float(rows, "wx_relative_humidity_pct")

        rec = {
            "event_year": _to_int(_first_non_null(rows, "event_year")),
            "tourn_id": _to_int(tourn_id),
            "round_number": _to_int(round_number),
            "player_key": player_key,
            "course_id": _first_non_null(rows, "course_id"),
            "layout_id": _first_non_null(rows, "layout_id"),
            "division": _first_non_null(rows, "division"),
            "player_rating": _to_float(_first_non_null(rows, "player_rating")),
            "model_inputs_grain": "round",
            "model_inputs_version": model_inputs_version,
            "model_inputs_run_id": run_id,
            "model_inputs_processed_at_utc": ts,
            "actual_round_strokes": actual_round_strokes,
            "round_strokes_over_par": round_strokes_over_par,
            "weather_available_flag": weather_available_flag,
            "hole_count": hole_count,
            "round_total_hole_length": round_total_hole_length,
            "round_avg_hole_length": round_avg_hole_length,
            "round_total_par": round_total_par,
            "round_avg_hole_par": round_avg_hole_par,
            "round_length_over_par": round_length_over_par,
            "round_wind_speed_mps_mean": round_wind_speed_mps_mean,
            "round_wind_speed_mps_max": round_wind_speed_mps_max,
            "round_wind_gust_mps_mean": round_wind_gust_mps_mean,
            "round_wind_gust_mps_max": round_wind_gust_mps_max,
            "round_temp_c_mean": round_temp_c_mean,
            "round_precip_mm_sum": round_precip_mm_sum,
            "round_precip_mm_mean": round_precip_mm_mean,
            "round_pressure_hpa_mean": round_pressure_hpa_mean,
            "round_humidity_pct_mean": round_humidity_pct_mean,
            "round_wind_speed_bucket": _wind_speed_bucket(round_wind_speed_mps_mean),
            "round_wind_gust_bucket": _wind_gust_bucket(round_wind_gust_mps_mean),
        }

        rec["row_hash_sha256"] = _json_hash(
            {
                "grain": "round",
                "tourn_id": rec["tourn_id"],
                "round_number": rec["round_number"],
                "player_key": rec["player_key"],
                "course_id": rec["course_id"],
                "layout_id": rec["layout_id"],
                "division": rec["division"],
                "player_rating": rec["player_rating"],
                "actual_round_strokes": rec["actual_round_strokes"],
                "round_strokes_over_par": rec["round_strokes_over_par"],
                "hole_count": rec["hole_count"],
                "round_total_hole_length": rec["round_total_hole_length"],
                "round_total_par": rec["round_total_par"],
                "round_wind_speed_mps_mean": rec["round_wind_speed_mps_mean"],
                "round_wind_speed_mps_max": rec["round_wind_speed_mps_max"],
                "round_wind_gust_mps_mean": rec["round_wind_gust_mps_mean"],
                "round_wind_gust_mps_max": rec["round_wind_gust_mps_max"],
                "round_temp_c_mean": rec["round_temp_c_mean"],
                "round_precip_mm_sum": rec["round_precip_mm_sum"],
                "round_precip_mm_mean": rec["round_precip_mm_mean"],
                "round_pressure_hpa_mean": rec["round_pressure_hpa_mean"],
                "round_humidity_pct_mean": rec["round_humidity_pct_mean"],
                "model_inputs_version": rec["model_inputs_version"],
            }
        )

        out.append(rec)

    out.sort(key=lambda r: (r.get("tourn_id"), r.get("round_number"), str(r.get("player_key"))))
    return out


def compute_model_inputs_event_fingerprint(
    *,
    hole_rows: list[dict[str, Any]],
    round_rows: list[dict[str, Any]] | None = None,
    model_inputs_policy_version: str = MODEL_INPUTS_POLICY_VERSION,
) -> str:
    def _project_hole(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "tourn_id": r.get("tourn_id"),
            "round_number": r.get("round_number"),
            "hole_number": r.get("hole_number"),
            "player_key": r.get("player_key"),
            "actual_strokes": r.get("actual_strokes"),
            "strokes_over_par": r.get("strokes_over_par"),
            "hole_length": r.get("hole_length"),
            "hole_par": r.get("hole_par"),
            "course_id": r.get("course_id"),
            "layout_id": r.get("layout_id"),
            "division": r.get("division"),
            "player_rating": r.get("player_rating"),
            "wx_wind_speed_mps": r.get("wx_wind_speed_mps"),
            "wx_wind_gust_mps": r.get("wx_wind_gust_mps"),
            "wx_wind_dir_deg": r.get("wx_wind_dir_deg"),
            "wx_temperature_c": r.get("wx_temperature_c"),
            "wx_precip_mm": r.get("wx_precip_mm"),
            "wx_pressure_hpa": r.get("wx_pressure_hpa"),
            "wx_relative_humidity_pct": r.get("wx_relative_humidity_pct"),
            "weather_available_flag": r.get("weather_available_flag"),
            "source_content_sha256": r.get("source_content_sha256"),
        }

    hole_proj = sorted((_project_hole(r) for r in hole_rows), key=lambda x: json.dumps(x, sort_keys=True))

    payload = {
        "model_inputs_policy_version": model_inputs_policy_version,
        "hole_rows": hole_proj,
    }
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
