from __future__ import annotations

from io import BytesIO
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from score_round_wind_model.models import SCORED_ROUNDS_PREFIX


def _to_parquet_bytes(rows: list[dict[str, Any]]) -> bytes:
    table = pa.Table.from_pylist(rows) if rows else pa.table({})
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def build_scored_round_output_key(*, event_year: int, event_id: int) -> str:
    return (
        f"{SCORED_ROUNDS_PREFIX}"
        f"event_year={int(event_year)}/"
        f"tourn_id={int(event_id)}/"
        "scored_rounds.parquet"
    )


def overwrite_event_scored_rounds(
    *,
    bucket: str,
    event_year: int,
    event_id: int,
    rows: list[dict[str, Any]],
    s3_client=None,
) -> str:
    s3 = s3_client or boto3.client("s3")
    key = build_scored_round_output_key(event_year=event_year, event_id=event_id)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=_to_parquet_bytes(rows),
        ContentType="application/octet-stream",
    )
    return key