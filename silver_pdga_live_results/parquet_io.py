from __future__ import annotations

import json
from typing import Any

import boto3
from botocore.exceptions import ClientError

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - validated in runtime env
    pa = None
    pq = None


def _ensure_pyarrow() -> None:
    if pa is None or pq is None:
        raise RuntimeError("pyarrow is required for Silver Parquet writes. Install dependency: pyarrow>=17.0")


def _write_rows_to_parquet_bytes(rows: list[dict[str, Any]]) -> bytes:
    _ensure_pyarrow()
    if not rows:
        raise ValueError("cannot write empty row list to parquet")
    table = pa.Table.from_pylist(rows)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue().to_pybytes()


def _safe_delete_object(*, s3, bucket: str, key: str) -> None:
    try:
        s3.delete_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound"):
            return
        raise


def overwrite_event_tables(
    *,
    bucket: str,
    event_year: int,
    event_id: int,
    run_id: str,
    round_rows: list[dict[str, Any]],
    hole_rows: list[dict[str, Any]],
    s3_client=None,
) -> dict[str, str]:
    s3 = s3_client or boto3.client("s3")

    round_final_key = (
        f"silver/pdga/live_results/player_rounds/event_year={int(event_year)}/"
        f"tourn_id={int(event_id)}/player_rounds.parquet"
    )
    hole_final_key = (
        f"silver/pdga/live_results/player_holes/event_year={int(event_year)}/"
        f"tourn_id={int(event_id)}/player_holes.parquet"
    )

    tmp_prefix = f"silver/pdga/live_results/_tmp/run_id={run_id}/event_id={int(event_id)}/"
    round_tmp_key = tmp_prefix + "player_rounds.parquet"
    hole_tmp_key = tmp_prefix + "player_holes.parquet"

    s3.put_object(
        Bucket=bucket,
        Key=round_tmp_key,
        Body=_write_rows_to_parquet_bytes(round_rows),
        ContentType="application/octet-stream",
    )
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": round_tmp_key}, Key=round_final_key)
    _safe_delete_object(s3=s3, bucket=bucket, key=round_tmp_key)

    if hole_rows:
        s3.put_object(
            Bucket=bucket,
            Key=hole_tmp_key,
            Body=_write_rows_to_parquet_bytes(hole_rows),
            ContentType="application/octet-stream",
        )
        s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": hole_tmp_key}, Key=hole_final_key)
        _safe_delete_object(s3=s3, bucket=bucket, key=hole_tmp_key)
        hole_key_out = hole_final_key
    else:
        # Event has no hole detail: ensure no stale hole parquet remains.
        _safe_delete_object(s3=s3, bucket=bucket, key=hole_final_key)
        hole_key_out = ""

    return {
        "round_key": round_final_key,
        "hole_key": hole_key_out,
    }


def put_quarantine_report(
    *,
    bucket: str,
    event_id: int,
    run_id: str,
    errors: list[str],
    s3_client=None,
) -> str:
    s3 = s3_client or boto3.client("s3")
    key = (
        f"silver/pdga/live_results/quarantine/event_id={int(event_id)}/"
        f"run_id={run_id}/dq_errors.json"
    )
    body = json.dumps({"event_id": int(event_id), "run_id": run_id, "errors": errors}, ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    return key