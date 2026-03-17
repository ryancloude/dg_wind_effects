from __future__ import annotations

import json
from io import BytesIO
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


def _table_from_rows(rows: list[dict[str, Any]]) -> pa.Table:
    if not rows:
        # Build an empty table with no columns if needed.
        return pa.table({})
    return pa.Table.from_pylist(rows)


def _write_parquet_bytes(rows: list[dict[str, Any]]) -> bytes:
    table = _table_from_rows(rows)
    buffer = BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()


def build_observations_key(*, event_year: int, event_id: int) -> str:
    return (
        "silver/weather/observations_hourly/"
        f"event_year={int(event_year)}/"
        f"event_id={int(event_id)}/"
        "observations_hourly.parquet"
    )


def build_quarantine_key(*, event_year: int, event_id: int, run_id: str) -> str:
    return (
        "silver/weather/quarantine/observations_hourly/"
        f"event_year={int(event_year)}/"
        f"event_id={int(event_id)}/"
        f"run_id={run_id}/"
        "dq_errors.json"
    )


def overwrite_event_table(
    *,
    bucket: str,
    event_year: int,
    event_id: int,
    run_id: str,
    observation_rows: list[dict[str, Any]],
    s3_client=None,
) -> dict[str, str]:
    del run_id  # reserved for future multi-object lineage extensions
    s3 = s3_client or boto3.client("s3")

    key = build_observations_key(event_year=event_year, event_id=event_id)
    payload = _write_parquet_bytes(observation_rows)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        ContentType="application/octet-stream",
    )

    return {"observations_key": key}


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

    body = json.dumps(
        {
            "event_id": int(event_id),
            "event_year": int(event_year),
            "run_id": run_id,
            "error_count": len(errors),
            "errors": errors,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    return key