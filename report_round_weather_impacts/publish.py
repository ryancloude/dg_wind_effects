from __future__ import annotations

from io import BytesIO

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from report_round_weather_impacts.models import INTERMEDIATE_BASE_PREFIX, REPORT_TABLES, SUM_METRIC_COLS


def _s3_client(s3_client=None):
    return s3_client or boto3.client("s3")


def _read_parquet_df(*, bucket: str, key: str, s3_client=None) -> pd.DataFrame:
    s3 = _s3_client(s3_client)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    table = pq.read_table(BytesIO(body))
    return table.to_pandas()


def list_intermediate_keys(*, bucket: str, table_name: str, s3_client=None) -> list[str]:
    s3 = _s3_client(s3_client)
    prefix = f"{INTERMEDIATE_BASE_PREFIX}{table_name}/"
    out: list[str] = []
    continuation_token: str | None = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = str(obj.get("Key", ""))
            if key.endswith(".parquet"):
                out.append(key)
        if not resp.get("IsTruncated"):
            break
        continuation_token = resp.get("NextContinuationToken")

    out.sort()
    return out


def _published_group_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c not in set(SUM_METRIC_COLS) and c not in {"event_year", "tourn_id"}]


def _finalize_metrics(df: pd.DataFrame) -> pd.DataFrame:
    denom = df["rounds_scored"].where(df["rounds_scored"] != 0, 1)
    df["avg_observed_wind_mph"] = df["sum_observed_wind_mph"] / denom
    df["avg_observed_temp_f"] = df["sum_observed_temp_f"] / denom
    df["avg_actual_round_strokes"] = df["sum_actual_round_strokes"] / denom
    df["avg_predicted_round_strokes"] = df["sum_predicted_round_strokes"] / denom
    df["avg_predicted_round_strokes_wind_reference"] = df["sum_predicted_round_strokes_wind_reference"] / denom
    df["avg_estimated_wind_impact_strokes"] = df["sum_estimated_wind_impact_strokes"] / denom
    df["avg_estimated_temperature_impact_strokes"] = df["sum_estimated_temperature_impact_strokes"] / denom
    df["avg_estimated_total_weather_impact_strokes"] = df["sum_estimated_total_weather_impact_strokes"] / denom
    return df


def build_published_table(*, bucket: str, table_name: str, s3_client=None) -> pd.DataFrame:
    keys = list_intermediate_keys(bucket=bucket, table_name=table_name, s3_client=s3_client)
    if not keys:
        return pd.DataFrame()

    dfs = [_read_parquet_df(bucket=bucket, key=key, s3_client=s3_client) for key in keys]
    combined = pd.concat(dfs, ignore_index=True)

    group_cols = _published_group_cols(combined)
    agg_map = {col: "sum" for col in combined.columns if col not in set(group_cols)}
    published = combined.groupby(group_cols, dropna=False).agg(agg_map).reset_index()
    published = _finalize_metrics(published)
    return published
