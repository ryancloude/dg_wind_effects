from __future__ import annotations

import json
from typing import Any, Dict, Tuple

import boto3

from ingest_weather_observations.models import WeatherObservationTask
from ingest_weather_observations.utils import sanitize_iso_ts_for_s3, utc_now_iso


def build_weather_keys(task: WeatherObservationTask, fetched_at_iso: str) -> Tuple[str, str]:
    fetch_date = fetched_at_iso[:10]
    fetch_ts = sanitize_iso_ts_for_s3(fetched_at_iso)

    prefix = (
        "bronze/weather/observations/"
        f"provider={task.provider}/"
        f"event_id={task.event_id}/"
        f"round={task.window.round_number}/"
        f"source_id={task.source_id}/"
        f"fetch_date={fetch_date}/"
    )
    base = f"fetch_ts={fetch_ts}"
    return prefix + base + ".json", prefix + base + ".meta.json"


def put_weather_raw(
    *,
    bucket: str,
    task: WeatherObservationTask,
    source_url: str,
    request_params: dict[str, str],
    request_fingerprint: str,
    payload: dict[str, Any],
    daylight_hour_count: int,
    content_sha256: str,
    http_status: int,
    run_id: str,
    tee_time_source_fingerprint: str,
    s3_client=None,
) -> Dict[str, Any]:
    s3 = s3_client or boto3.client("s3")
    fetched_at = utc_now_iso()
    json_key, meta_key = build_weather_keys(task, fetched_at)

    payload_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    meta = {
        "event_id": task.event_id,
        "event_name": task.event_name,
        "round_number": task.window.round_number,
        "provider": task.provider,
        "source_id": task.source_id,
        "source_url": source_url,
        "request_params": request_params,
        "request_fingerprint": request_fingerprint,
        "tee_time_source_fingerprint": tee_time_source_fingerprint,
        "window_start_date": task.window.start_date.isoformat(),
        "window_end_date": task.window.end_date.isoformat(),
        "round_date": task.window.round_date.isoformat(),
        "event_latitude": task.point.latitude,
        "event_longitude": task.point.longitude,
        "city": task.city,
        "state": task.state,
        "country": task.country,
        "fetched_at": fetched_at,
        "http_status": int(http_status),
        "content_sha256": content_sha256,
        "content_length": len(payload_bytes),
        "daylight_hour_count": int(daylight_hour_count),
        "run_id": run_id,
        "s3_json_key": json_key,
    }

    s3.put_object(
        Bucket=bucket,
        Key=json_key,
        Body=payload_bytes,
        ContentType="application/json",
    )
    s3.put_object(
        Bucket=bucket,
        Key=meta_key,
        Body=json.dumps(meta, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )

    return {
        "event_id": task.event_id,
        "round_number": task.window.round_number,
        "provider": task.provider,
        "source_id": task.source_id,
        "fetched_at": fetched_at,
        "s3_json_key": json_key,
        "s3_meta_key": meta_key,
    }