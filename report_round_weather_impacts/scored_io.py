from __future__ import annotations

import re
from io import BytesIO
from typing import Any

import boto3
import pandas as pd
import pyarrow.parquet as pq

from report_round_weather_impacts.models import SCORED_ROUNDS_PREFIX


_TOURN_ID_PATTERN = re.compile(r"tourn_id=(\d+)")


def _s3_client(s3_client=None):
    return s3_client or boto3.client("s3")


def _object_descriptor(obj: dict[str, Any]) -> dict[str, Any]:
    return {
        "key": str(obj.get("Key", "")),
        "etag": str(obj.get("ETag", "")).strip('"'),
        "size": int(obj.get("Size", 0) or 0),
        "last_modified": obj.get("LastModified").isoformat() if obj.get("LastModified") is not None else "",
    }


def list_scored_event_objects(
    *,
    bucket: str,
    prefix: str = SCORED_ROUNDS_PREFIX,
    event_ids: list[int] | None = None,
    s3_client=None,
) -> list[dict[str, Any]]:
    s3 = _s3_client(s3_client)
    allowed = {int(x) for x in event_ids} if event_ids else None

    out: list[dict[str, Any]] = []
    continuation_token: str | None = None

    while True:
        kwargs: dict[str, Any] = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": 1000,
        }
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        resp = s3.list_objects_v2(**kwargs)

        for obj in resp.get("Contents", []):
            key = str(obj.get("Key", ""))
            if not key.endswith(".parquet"):
                continue

            if allowed is not None:
                match = _TOURN_ID_PATTERN.search(key)
                if not match:
                    continue
                event_id = int(match.group(1))
                if event_id not in allowed:
                    continue

            out.append(_object_descriptor(obj))

        if not resp.get("IsTruncated"):
            break

        continuation_token = resp.get("NextContinuationToken")

    out.sort(key=lambda x: x["key"])
    return out


def load_scored_event_dataframe(
    *,
    bucket: str,
    key: str,
    s3_client=None,
) -> pd.DataFrame:
    s3 = _s3_client(s3_client)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    table = pq.read_table(BytesIO(body))
    return table.to_pandas()
