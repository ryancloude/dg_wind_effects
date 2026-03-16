from __future__ import annotations

from io import BytesIO
from typing import Any

import boto3
import pyarrow.parquet as pq

from ingest_weather_observations.utils import sha256_obj


def load_player_round_rows(
    *,
    bucket: str,
    key: str,
    s3_client=None,
) -> list[dict[str, Any]]:
    s3 = s3_client or boto3.client("s3")
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    table = pq.read_table(BytesIO(body))
    return table.to_pylist()


def compute_tee_time_source_fingerprint(
    rows: list[dict[str, Any]],
    *,
    round_key: str = "round_number",
    tee_time_key: str = "tee_time_join_ts",
    player_key: str = "player_key",
) -> str:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                "round_number": int(row.get(round_key)) if row.get(round_key) is not None else None,
                "tee_time_join_ts": row.get(tee_time_key),
                "player_key": row.get(player_key),
            }
        )
    normalized.sort(
        key=lambda r: (
            "" if r["round_number"] is None else str(r["round_number"]),
            "" if r["tee_time_join_ts"] is None else str(r["tee_time_join_ts"]),
            "" if r["player_key"] is None else str(r["player_key"]),
        )
    )
    return sha256_obj(normalized)