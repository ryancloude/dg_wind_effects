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


def load_hole_feature_rows(
    *,
    bucket: str,
    hole_s3_key: str,
    s3_client=None,
) -> list[dict[str, Any]]:
    return load_parquet_rows(bucket=bucket, key=hole_s3_key, s3_client=s3_client)
