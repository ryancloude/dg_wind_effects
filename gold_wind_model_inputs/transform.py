from __future__ import annotations

import hashlib
import json
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


def _json_hash(payload: dict[str, Any]) -> str:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_hole_model_inputs(
    hole_feature_rows: list[dict[str, Any]],
    *,
    run_id: str,
    processed_at_utc: str | None = None,
    model_inputs_version: str = MODEL_INPUTS_POLICY_VERSION,
) -> list[dict[str, Any]]:
    ts = processed_at_utc or _utc_now_iso()
    out: list[dict[str, Any]] = []

    for row in hole_feature_rows:
        rec = dict(row)

        target = _to_int(rec.get("strokes_over_par"))
        if target is None:
            hole_score = _to_int(rec.get("actual_strokes") if rec.get("actual_strokes") is not None else rec.get("hole_score"))
            hole_par = _to_int(rec.get("hole_par"))
            if hole_score is not None and hole_par is not None:
                target = hole_score - hole_par

        wind_speed = _to_float(rec.get("wx_wind_speed_mps"))

        rec["model_inputs_grain"] = "hole"
        rec["model_inputs_version"] = model_inputs_version
        rec["model_inputs_run_id"] = run_id
        rec["model_inputs_processed_at_utc"] = ts

        rec["target_strokes_over_par"] = target
        rec["weather_available_flag"] = not bool(rec.get("wx_weather_missing_flag"))
        rec["wind_speed_bucket"] = _wind_speed_bucket(wind_speed)

        rec["feature_wind_speed_mps"] = _to_float(rec.get("wx_wind_speed_mps"))
        rec["feature_wind_gust_mps"] = _to_float(rec.get("wx_wind_gust_mps"))
        rec["feature_wind_dir_deg"] = _to_float(rec.get("wx_wind_dir_deg"))
        rec["feature_temp_c"] = _to_float(rec.get("wx_temperature_c"))
        rec["feature_precip_mm"] = _to_float(rec.get("wx_precip_mm"))
        rec["feature_pressure_hpa"] = _to_float(rec.get("wx_pressure_hpa"))
        rec["feature_humidity_pct"] = _to_float(rec.get("wx_relative_humidity_pct"))

        rec["feature_hole_par"] = _to_int(rec.get("hole_par"))
        rec["feature_layout_id"] = _to_int(rec.get("layout_id"))
        rec["feature_course_id"] = _to_int(rec.get("course_id"))

        rec["row_hash_sha256"] = _json_hash(
            {
                "grain": "hole",
                "tourn_id": rec.get("tourn_id"),
                "round_number": rec.get("round_number"),
                "hole_number": rec.get("hole_number"),
                "player_key": rec.get("player_key"),
                "target_strokes_over_par": rec.get("target_strokes_over_par"),
                "feature_wind_speed_mps": rec.get("feature_wind_speed_mps"),
                "feature_wind_gust_mps": rec.get("feature_wind_gust_mps"),
                "feature_wind_dir_deg": rec.get("feature_wind_dir_deg"),
                "feature_precip_mm": rec.get("feature_precip_mm"),
                "source_content_sha256": rec.get("source_content_sha256"),
                "model_inputs_version": rec.get("model_inputs_version"),
            }
        )

        out.append(rec)

    return out


def build_round_model_inputs(
    round_feature_rows: list[dict[str, Any]],
    *,
    run_id: str,
    processed_at_utc: str | None = None,
    model_inputs_version: str = MODEL_INPUTS_POLICY_VERSION,
) -> list[dict[str, Any]]:
    ts = processed_at_utc or _utc_now_iso()
    out: list[dict[str, Any]] = []

    for row in round_feature_rows:
        rec = dict(row)

        target = _to_int(rec.get("strokes_over_par"))
        if target is None:
            round_score = _to_int(rec.get("actual_strokes") if rec.get("actual_strokes") is not None else rec.get("round_score"))
            layout_par = _to_int(rec.get("layout_par"))
            if round_score is not None and layout_par is not None:
                target = round_score - layout_par

        wind_speed = _to_float(rec.get("wx_wind_speed_mps"))

        rec["model_inputs_grain"] = "round"
        rec["model_inputs_version"] = model_inputs_version
        rec["model_inputs_run_id"] = run_id
        rec["model_inputs_processed_at_utc"] = ts

        rec["target_strokes_over_par"] = target
        rec["weather_available_flag"] = not bool(rec.get("wx_weather_missing_flag"))
        rec["wind_speed_bucket"] = _wind_speed_bucket(wind_speed)

        rec["feature_wind_speed_mps"] = _to_float(rec.get("wx_wind_speed_mps"))
        rec["feature_wind_gust_mps"] = _to_float(rec.get("wx_wind_gust_mps"))
        rec["feature_wind_dir_deg"] = _to_float(rec.get("wx_wind_dir_deg"))
        rec["feature_temp_c"] = _to_float(rec.get("wx_temperature_c"))
        rec["feature_precip_mm"] = _to_float(rec.get("wx_precip_mm"))

        rec["feature_layout_id"] = _to_int(rec.get("layout_id"))
        rec["feature_course_id"] = _to_int(rec.get("course_id"))

        rec["row_hash_sha256"] = _json_hash(
            {
                "grain": "round",
                "tourn_id": rec.get("tourn_id"),
                "round_number": rec.get("round_number"),
                "player_key": rec.get("player_key"),
                "target_strokes_over_par": rec.get("target_strokes_over_par"),
                "feature_wind_speed_mps": rec.get("feature_wind_speed_mps"),
                "feature_wind_gust_mps": rec.get("feature_wind_gust_mps"),
                "feature_wind_dir_deg": rec.get("feature_wind_dir_deg"),
                "feature_precip_mm": rec.get("feature_precip_mm"),
                "source_content_sha256": rec.get("source_content_sha256"),
                "model_inputs_version": rec.get("model_inputs_version"),
            }
        )

        out.append(rec)

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
            "strokes_over_par": r.get("strokes_over_par"),
            "actual_strokes": r.get("actual_strokes"),
            "hole_score": r.get("hole_score"),
            "hole_par": r.get("hole_par"),
            "wx_wind_speed_mps": r.get("wx_wind_speed_mps"),
            "wx_wind_gust_mps": r.get("wx_wind_gust_mps"),
            "wx_wind_dir_deg": r.get("wx_wind_dir_deg"),
            "wx_precip_mm": r.get("wx_precip_mm"),
            "wx_weather_missing_flag": r.get("wx_weather_missing_flag"),
            "source_content_sha256": r.get("source_content_sha256"),
        }

    def _project_round(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "tourn_id": r.get("tourn_id"),
            "round_number": r.get("round_number"),
            "player_key": r.get("player_key"),
            "strokes_over_par": r.get("strokes_over_par"),
            "actual_strokes": r.get("actual_strokes"),
            "round_score": r.get("round_score"),
            "layout_par": r.get("layout_par"),
            "wx_wind_speed_mps": r.get("wx_wind_speed_mps"),
            "wx_wind_gust_mps": r.get("wx_wind_gust_mps"),
            "wx_wind_dir_deg": r.get("wx_wind_dir_deg"),
            "wx_precip_mm": r.get("wx_precip_mm"),
            "wx_weather_missing_flag": r.get("wx_weather_missing_flag"),
            "source_content_sha256": r.get("source_content_sha256"),
        }

    hole_proj = sorted((_project_hole(r) for r in hole_rows), key=lambda x: json.dumps(x, sort_keys=True))
    round_proj = sorted((_project_round(r) for r in (round_rows or [])), key=lambda x: json.dumps(x, sort_keys=True))

    payload = {
        "model_inputs_policy_version": model_inputs_policy_version,
        "hole_rows": hole_proj,
        "round_rows": round_proj,
    }
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()