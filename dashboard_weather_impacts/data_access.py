from __future__ import annotations

from io import BytesIO
from typing import Any

import boto3
import pandas as pd
import pyarrow.parquet as pq
import streamlit as st

from dashboard_weather_impacts.config import DashboardConfig


PAGE_TABLES: dict[str, list[str]] = {
    "Overview": [
        "weather_overview",
        "weather_impact_distribution",
        "weather_wind_impact_points",
    ],
    "Geography": [
        "weather_by_state",
    ],
    "Where It Matters": [
        "weather_by_division",
        "weather_by_rating_band",
        "weather_by_course_layout",
        "weather_by_wind_bucket",
        "weather_by_temperature_band",
    ],
    "Event Explorer": [
        "weather_by_event",
        "weather_by_event_round",
    ],
}


def _s3_client(config: DashboardConfig):
    if config.aws_region:
        return boto3.client("s3", region_name=config.aws_region)
    return boto3.client("s3")


def _read_parquet_from_s3(*, bucket: str, key: str, s3_client) -> pd.DataFrame:
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    table = pq.read_table(BytesIO(body))
    return table.to_pandas()


def _is_ctas_data_key(key: str) -> bool:
    lower = key.lower()

    if lower.endswith("/"):
        return False

    if lower.endswith(".csv") or lower.endswith(".json") or lower.endswith(".metadata"):
        return False
    if lower.endswith("manifest") or lower.endswith("-manifest.csv"):
        return False

    if lower.endswith(".parquet"):
        return True

    leaf = lower.rsplit("/", 1)[-1]
    return "." not in leaf


def _list_ctas_data_keys_under_prefix(*, bucket: str, prefix: str, s3_client) -> list[str]:
    out: list[str] = []
    continuation_token: str | None = None

    while True:
        kwargs: dict[str, Any] = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": 1000,
        }
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = str(obj.get("Key", ""))
            if _is_ctas_data_key(key):
                out.append(key)

        if not resp.get("IsTruncated"):
            break

        continuation_token = resp.get("NextContinuationToken")

    out.sort()
    return out


@st.cache_data(show_spinner=False)
def load_published_report_table(config: DashboardConfig, table_name: str) -> pd.DataFrame:
    s3 = _s3_client(config)
    prefix = f"{config.reports_published_prefix}{table_name}/"
    data_keys = _list_ctas_data_keys_under_prefix(bucket=config.s3_bucket, prefix=prefix, s3_client=s3)

    if not data_keys:
        return pd.DataFrame()

    frames = [
        _read_parquet_from_s3(bucket=config.s3_bucket, key=key, s3_client=s3)
        for key in data_keys
    ]
    return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]


def report_tables_for_page(page: str) -> list[str]:
    return PAGE_TABLES.get(page, [])


@st.cache_data(show_spinner=False)
def load_page_datasets(config: DashboardConfig, page: str) -> dict[str, pd.DataFrame]:
    table_names = report_tables_for_page(page)
    return {name: load_published_report_table(config, name) for name in table_names}


@st.cache_data(show_spinner=False)
def list_scored_event_keys(config: DashboardConfig) -> list[str]:
    s3 = _s3_client(config)
    prefix = config.scored_rounds_prefix
    out: list[str] = []
    continuation_token: str | None = None

    while True:
        kwargs: dict[str, Any] = {
            "Bucket": config.s3_bucket,
            "Prefix": prefix,
            "MaxKeys": 1000,
        }
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


def _scored_key_for_event(config: DashboardConfig, event_year: int, event_id: int) -> str:
    return (
        f"{config.scored_rounds_prefix}"
        f"event_year={int(event_year)}/"
        f"tourn_id={int(event_id)}/"
        "scored_rounds.parquet"
    )


@st.cache_data(show_spinner=False)
def load_scored_round_detail(config: DashboardConfig, event_year: int, event_id: int) -> pd.DataFrame:
    s3 = _s3_client(config)
    key = _scored_key_for_event(config, event_year, event_id)
    return _read_parquet_from_s3(bucket=config.s3_bucket, key=key, s3_client=s3)
