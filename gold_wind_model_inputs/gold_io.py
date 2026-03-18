from __future__ import annotations

from io import BytesIO
from typing import Any

import boto3
import pyarrow.parquet as pq


def load_parquet_rows(*, bucket: str, key: str, s3_client=None) -> list[dict[str, Any]]:
    s3 = s3_client or boto3.client("s3")
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    table = pq.read_table(BytesIO(body))
    return table.to_pylist()


def load_event_input_tables(
    *,
    bucket: str,
    hole_s3_key: str,
    round_s3_key: str | None = None,
    s3_client=None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    hole_rows = load_parquet_rows(bucket=bucket, key=hole_s3_key, s3_client=s3_client)
    round_rows: list[dict[str, Any]] = []
    if round_s3_key:
        round_rows = load_parquet_rows(bucket=bucket, key=round_s3_key, s3_client=s3_client)
    return hole_rows, round_rows