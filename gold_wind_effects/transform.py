from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from gold_wind_effects.models import GOLD_POLICY_VERSION


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
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


def _row_hash(payload: dict[str, Any]) -> str:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_round_features(
    round_rows: list[dict[str, Any]],
    *,
    run_id: str,
    processed_at_utc: str | None = None,
) -> list[dict[str, Any]]:
    ts = processed_at_utc or _utc_now_iso()
    out: list[dict[str, Any]] = []

    for row in round_rows:
        rec = dict(row)
        wx_speed = _to_float(rec.get("wx_wind_speed_mps"))
        actual = _to_int(rec.get("round_score"))
        strokes_over_par = _to_int(rec.get("round_to_par"))
        if strokes_over_par is None:
            round_par = _to_int(rec.get("layout_par"))
            if actual is not None and round_par is not None:
                strokes_over_par = actual - round_par

        rec["gold_grain"] = "round"
        rec["gold_model_version"] = GOLD_POLICY_VERSION
        rec["gold_run_id"] = run_id
        rec["gold_processed_at_utc"] = ts
        rec["actual_strokes"] = actual
        rec["strokes_over_par"] = strokes_over_par
        rec["weather_available_flag"] = not bool(rec.get("wx_weather_missing_flag"))
        rec["wind_speed_bucket"] = _wind_speed_bucket(wx_speed)

        rec["row_hash_sha256"] = _row_hash(
            {
                "grain": "round",
                "tourn_id": rec.get("tourn_id"),
                "round_number": rec.get("round_number"),
                "player_key": rec.get("player_key"),
                "actual_strokes": rec.get("actual_strokes"),
                "strokes_over_par": rec.get("strokes_over_par"),
                "wx_observation_hour_utc": rec.get("wx_observation_hour_utc"),
                "wx_wind_speed_mps": rec.get("wx_wind_speed_mps"),
                "wx_wind_gust_mps": rec.get("wx_wind_gust_mps"),
                "wx_wind_dir_deg": rec.get("wx_wind_dir_deg"),
                "wx_precip_mm": rec.get("wx_precip_mm"),
                "source_content_sha256": rec.get("source_content_sha256"),
                "gold_model_version": rec.get("gold_model_version"),
            }
        )
        out.append(rec)
    return out


def build_hole_features(
    hole_rows: list[dict[str, Any]],
    *,
    run_id: str,
    processed_at_utc: str | None = None,
) -> list[dict[str, Any]]:
    ts = processed_at_utc or _utc_now_iso()
    out: list[dict[str, Any]] = []

    for row in hole_rows:
        rec = dict(row)
        wx_speed = _to_float(rec.get("wx_wind_speed_mps"))
        actual = _to_int(rec.get("hole_score"))
        strokes_over_par = _to_int(rec.get("hole_to_par"))
        if strokes_over_par is None:
            hole_par = _to_int(rec.get("hole_par"))
            if actual is not None and hole_par is not None:
                strokes_over_par = actual - hole_par

        rec["gold_grain"] = "hole"
        rec["gold_model_version"] = GOLD_POLICY_VERSION
        rec["gold_run_id"] = run_id
        rec["gold_processed_at_utc"] = ts
        rec["actual_strokes"] = actual
        rec["strokes_over_par"] = strokes_over_par
        rec["weather_available_flag"] = not bool(rec.get("wx_weather_missing_flag"))
        rec["wind_speed_bucket"] = _wind_speed_bucket(wx_speed)

        rec["row_hash_sha256"] = _row_hash(
            {
                "grain": "hole",
                "tourn_id": rec.get("tourn_id"),
                "round_number": rec.get("round_number"),
                "hole_number": rec.get("hole_number"),
                "player_key": rec.get("player_key"),
                "actual_strokes": rec.get("actual_strokes"),
                "strokes_over_par": rec.get("strokes_over_par"),
                "wx_observation_hour_utc": rec.get("wx_observation_hour_utc"),
                "wx_wind_speed_mps": rec.get("wx_wind_speed_mps"),
                "wx_wind_gust_mps": rec.get("wx_wind_gust_mps"),
                "wx_wind_dir_deg": rec.get("wx_wind_dir_deg"),
                "wx_precip_mm": rec.get("wx_precip_mm"),
                "source_content_sha256": rec.get("source_content_sha256"),
                "gold_model_version": rec.get("gold_model_version"),
            }
        )
        out.append(rec)
    return out


def compute_gold_event_fingerprint(
    *,
    round_rows: list[dict[str, Any]],
    hole_rows: list[dict[str, Any]],
    gold_policy_version: str = GOLD_POLICY_VERSION,
) -> str:
    def _project_round(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "tourn_id": r.get("tourn_id"),
            "round_number": r.get("round_number"),
            "player_key": r.get("player_key"),
            "round_score": r.get("round_score"),
            "round_to_par": r.get("round_to_par"),
            "wx_observation_hour_utc": r.get("wx_observation_hour_utc"),
            "wx_wind_speed_mps": r.get("wx_wind_speed_mps"),
            "wx_wind_gust_mps": r.get("wx_wind_gust_mps"),
            "wx_wind_dir_deg": r.get("wx_wind_dir_deg"),
            "wx_precip_mm": r.get("wx_precip_mm"),
            "source_content_sha256": r.get("source_content_sha256"),
        }

    def _project_hole(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "tourn_id": r.get("tourn_id"),
            "round_number": r.get("round_number"),
            "hole_number": r.get("hole_number"),
            "player_key": r.get("player_key"),
            "hole_score": r.get("hole_score"),
            "hole_to_par": r.get("hole_to_par"),
            "wx_observation_hour_utc": r.get("wx_observation_hour_utc"),
            "wx_wind_speed_mps": r.get("wx_wind_speed_mps"),
            "wx_wind_gust_mps": r.get("wx_wind_gust_mps"),
            "wx_wind_dir_deg": r.get("wx_wind_dir_deg"),
            "wx_precip_mm": r.get("wx_precip_mm"),
            "source_content_sha256": r.get("source_content_sha256"),
        }

    round_proj = sorted((_project_round(r) for r in round_rows), key=lambda x: json.dumps(x, sort_keys=True))
    hole_proj = sorted((_project_hole(r) for r in hole_rows), key=lambda x: json.dumps(x, sort_keys=True))

    payload = {
        "gold_policy_version": gold_policy_version,
        "round_rows": round_proj,
        "hole_rows": hole_proj,
    }
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()