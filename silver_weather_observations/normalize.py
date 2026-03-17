from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from silver_weather_observations.models import BronzeWeatherRoundSource


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _normalize_iso_utc(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None

    # Open-Meteo often returns local-like timestamps without offset.
    # We normalize to UTC ISO for deterministic downstream joins.
    if text.endswith("Z"):
        return text

    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _observation_hour_utc(observation_ts_utc: str | None) -> str | None:
    if not observation_ts_utc:
        return None
    try:
        dt = datetime.fromisoformat(observation_ts_utc.replace("Z", "+00:00"))
    except ValueError:
        return None
    dt = dt.replace(minute=0, second=0, microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def _hash_text(text: str) -> str:
    import hashlib

    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_weather_obs_pk(
    *,
    event_id: int,
    round_number: int,
    provider: str,
    source_id: str,
    observation_hour_utc: str,
) -> str:
    payload = f"{int(event_id)}|{int(round_number)}|{provider}|{source_id}|{observation_hour_utc}"
    return _hash_text(payload)


def _event_year_from_metadata_or_ts(event_metadata: dict[str, Any], observation_ts_utc: str | None) -> int:
    start_date = str(event_metadata.get("start_date", "")).strip()
    if len(start_date) >= 4 and start_date[:4].isdigit():
        return int(start_date[:4])

    if observation_ts_utc and len(observation_ts_utc) >= 4 and observation_ts_utc[:4].isdigit():
        return int(observation_ts_utc[:4])

    return 0


def _hourly_at(hourly: dict[str, Any], key: str, idx: int) -> Any:
    values = hourly.get(key)
    if not isinstance(values, list):
        return None
    if idx < 0 or idx >= len(values):
        return None
    return values[idx]


def normalize_event_records(
    *,
    event_metadata: dict[str, Any],
    round_sources: list[BronzeWeatherRoundSource],
    event_source_fingerprint: str,
    run_id: str,
    silver_processed_at_utc: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    event_id = int(event_metadata.get("event_id", 0))
    event_lat = _safe_float(event_metadata.get("latitude") or event_metadata.get("lat"))
    event_lon = _safe_float(event_metadata.get("longitude") or event_metadata.get("lon"))

    city = str(event_metadata.get("city", "")).strip()
    state = str(event_metadata.get("state", "")).strip()
    country = str(event_metadata.get("country", "")).strip()

    for src in round_sources:
        payload = src.payload if isinstance(src.payload, dict) else {}
        hourly = payload.get("hourly")
        if not isinstance(hourly, dict):
            continue

        times = hourly.get("time")
        if not isinstance(times, list):
            continue

        payload_lat = _safe_float(payload.get("latitude"))
        payload_lon = _safe_float(payload.get("longitude"))

        for idx, raw_time in enumerate(times):
            observation_ts_utc = _normalize_iso_utc(raw_time)
            observation_hour = _observation_hour_utc(observation_ts_utc)
            if not observation_hour:
                continue

            event_year = _event_year_from_metadata_or_ts(event_metadata, observation_ts_utc)

            wind_speed_mps = _safe_float(_hourly_at(hourly, "wind_speed_10m", idx))
            wind_gust_mps = _safe_float(_hourly_at(hourly, "wind_gusts_10m", idx))
            wind_dir_deg = _safe_float(_hourly_at(hourly, "wind_direction_10m", idx))
            temperature_c = _safe_float(_hourly_at(hourly, "temperature_2m", idx))
            pressure_hpa = _safe_float(_hourly_at(hourly, "pressure_msl", idx))
            relative_humidity_pct = _safe_float(_hourly_at(hourly, "relative_humidity_2m", idx))
            precip_mm = _safe_float(_hourly_at(hourly, "precipitation", idx))

            pk = build_weather_obs_pk(
                event_id=event_id or src.event_id,
                round_number=src.round_number,
                provider=src.provider,
                source_id=src.source_id,
                observation_hour_utc=observation_hour,
            )

            rows.append(
                {
                    "weather_obs_pk": pk,
                    "event_id": int(event_id or src.event_id),
                    "event_year": int(event_year),
                    "round_number": int(src.round_number),
                    "provider": src.provider,
                    "source_id": src.source_id,
                    "observation_ts_utc": observation_ts_utc,
                    "observation_hour_utc": observation_hour,
                    "wind_speed_mps": wind_speed_mps,
                    "wind_gust_mps": wind_gust_mps,
                    "wind_dir_deg": wind_dir_deg,
                    "temperature_c": temperature_c,
                    "pressure_hpa": pressure_hpa,
                    "relative_humidity_pct": relative_humidity_pct,
                    "precip_mm": precip_mm,
                    "daylight_flag": None,  # can be populated in a later step if daylight row-level tags are persisted
                    "event_latitude": event_lat,
                    "event_longitude": event_lon,
                    "obs_latitude": payload_lat,
                    "obs_longitude": payload_lon,
                    "city": city,
                    "state": state,
                    "country": country,
                    "event_source_fingerprint": event_source_fingerprint,
                    "source_json_key": src.source_json_key,
                    "source_meta_key": src.source_meta_key or "",
                    "source_content_sha256": src.source_content_sha256,
                    "source_fetched_at_utc": src.source_fetched_at_utc,
                    "request_fingerprint": src.request_fingerprint,
                    "tee_time_source_fingerprint": src.tee_time_source_fingerprint,
                    "silver_run_id": run_id,
                    "silver_processed_at_utc": silver_processed_at_utc,
                }
            )

    rows.sort(
        key=lambda r: (
            r["event_id"],
            r["round_number"],
            r["provider"],
            r["source_id"],
            r["observation_hour_utc"],
            r["source_fetched_at_utc"],
            r["source_json_key"],
        )
    )
    return rows