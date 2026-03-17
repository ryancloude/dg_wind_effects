from __future__ import annotations

import json
from io import BytesIO
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


def _to_parquet_bytes(rows: list[dict[str, Any]]) -> bytes:
    table = pa.Table.from_pylist(rows) if rows else pa.table({})
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def build_round_output_key(*, event_year: int, event_id: int) -> str:
    return (
        "silver/pdga/live_results_enriched/player_rounds_weather/"
        f"event_year={int(event_year)}/"
        f"tourn_id={int(event_id)}/"
        "player_rounds_weather.parquet"
    )


def build_hole_output_key(*, event_year: int, event_id: int) -> str:
    return (
        "silver/pdga/live_results_enriched/player_holes_weather/"
        f"event_year={int(event_year)}/"
        f"tourn_id={int(event_id)}/"
        "player_holes_weather.parquet"
    )


def build_quarantine_key(*, event_year: int, event_id: int, run_id: str) -> str:
    return (
        "silver/pdga/live_results_enriched/quarantine/"
        f"event_year={int(event_year)}/"
        f"tourn_id={int(event_id)}/"
        f"run_id={run_id}/"
        "dq_errors.json"
    )


def overwrite_event_tables(
    *,
    bucket: str,
    event_year: int,
    event_id: int,
    round_rows: list[dict[str, Any]],
    hole_rows: list[dict[str, Any]],
    s3_client=None,
) -> dict[str, str]:
    s3 = s3_client or boto3.client("s3")

    round_key = build_round_output_key(event_year=event_year, event_id=event_id)
    hole_key = build_hole_output_key(event_year=event_year, event_id=event_id)

    s3.put_object(
        Bucket=bucket,
        Key=round_key,
        Body=_to_parquet_bytes(round_rows),
        ContentType="application/octet-stream",
    )
    s3.put_object(
        Bucket=bucket,
        Key=hole_key,
        Body=_to_parquet_bytes(hole_rows),
        ContentType="application/octet-stream",
    )

    return {"round_key": round_key, "hole_key": hole_key}


def put_quarantine_report(
    *,
    bucket: str,
    event_year: int,
    event_id: int,
    run_id: str,
    errors: list[dict[str, Any]],
    s3_client=None,
) -> str:
    s3 = s3_client or boto3.client("s3")
    key = build_quarantine_key(event_year=event_year, event_id=event_id, run_id=run_id)

    payload = {
        "event_id": int(event_id),
        "event_year": int(event_year),
        "run_id": run_id,
        "error_count": len(errors),
        "errors": errors,
    }

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    return key