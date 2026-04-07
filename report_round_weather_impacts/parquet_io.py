from __future__ import annotations

from io import BytesIO

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from report_round_weather_impacts.models import INTERMEDIATE_BASE_PREFIX, PUBLISHED_BASE_PREFIX


def _s3_client(s3_client=None):
    return s3_client or boto3.client("s3")


def _df_to_bytes(df: pd.DataFrame) -> bytes:
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def build_intermediate_key(*, table_name: str, event_year: int, event_id: int) -> str:
    return f"{INTERMEDIATE_BASE_PREFIX}{table_name}/event_year={int(event_year)}/tourn_id={int(event_id)}/part.parquet"


def build_published_key(*, table_name: str) -> str:
    return f"{PUBLISHED_BASE_PREFIX}{table_name}/report.parquet"


def write_intermediate_table(
    *,
    bucket: str,
    table_name: str,
    event_year: int,
    event_id: int,
    df: pd.DataFrame,
    s3_client=None,
) -> str:
    key = build_intermediate_key(table_name=table_name, event_year=event_year, event_id=event_id)
    s3 = _s3_client(s3_client)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=_df_to_bytes(df),
        ContentType="application/octet-stream",
    )
    return key


def write_published_table(
    *,
    bucket: str,
    table_name: str,
    df: pd.DataFrame,
    s3_client=None,
) -> str:
    key = build_published_key(table_name=table_name)
    s3 = _s3_client(s3_client)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=_df_to_bytes(df),
        ContentType="application/octet-stream",
    )
    return key
