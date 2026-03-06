from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

import boto3

from ingest_pdga_live_results.dynamo_reader import LiveResultsTask


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_live_results_keys(task: LiveResultsTask, fetched_at_iso: str) -> Tuple[str, str]:
    fetch_date = fetched_at_iso[:10]
    prefix = (
        "bronze/pdga/live_results/"
        f"event_id={task.event_id}/"
        f"division={task.division}/"
        f"round={task.round_number}/"
        f"fetch_date={fetch_date}/"
    )
    base = f"fetch_ts={fetched_at_iso}"
    return prefix + base + ".json", prefix + base + ".meta.json"


def put_live_results_raw(
    *,
    bucket: str,
    task: LiveResultsTask,
    source_url: str,
    payload: dict[str, Any] | list[Any],
    http_status: int,
    content_sha256: str,
    run_id: str,
    s3_client=None,
) -> Dict[str, Any]:
    s3 = s3_client or boto3.client("s3")

    fetched_at = utc_now_iso()
    json_key, meta_key = build_live_results_keys(task, fetched_at)

    payload_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    meta = {
        "event_id": int(task.event_id),
        "division": task.division,
        "round_number": int(task.round_number),
        "source_url": source_url,
        "fetched_at": fetched_at,
        "http_status": int(http_status),
        "content_sha256": content_sha256,
        "content_length": len(payload_bytes),
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
        "event_id": int(task.event_id),
        "division": task.division,
        "round_number": int(task.round_number),
        "fetched_at": fetched_at,
        "s3_json_key": json_key,
        "s3_meta_key": meta_key,
    }