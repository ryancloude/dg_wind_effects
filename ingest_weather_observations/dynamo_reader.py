from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import boto3
from boto3.dynamodb.conditions import Key

SILVER_CHECKPOINT_PK = "PIPELINE#SILVER_LIVE_RESULTS"


@dataclass(frozen=True)
class WeatherEventCandidate:
    event_id: int
    event_metadata: dict[str, Any]
    silver_checkpoint: dict[str, Any]

    @property
    def round_s3_key(self) -> str:
        return str(self.silver_checkpoint.get("round_s3_key", "")).strip()

    @property
    def silver_checkpoint_updated_at(self) -> str:
        return str(self.silver_checkpoint.get("updated_at", "")).strip()


def _ddb_resource(aws_region: Optional[str]):
    return boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")


def _ddb_client(aws_region: Optional[str]):
    return boto3.client("dynamodb", region_name=aws_region) if aws_region else boto3.client("dynamodb")


def _get_item(table, *, pk: str, sk: str) -> dict[str, Any] | None:
    resp = table.get_item(Key={"pk": pk, "sk": sk}, ConsistentRead=False)
    return resp.get("Item")


def _load_success_silver_checkpoints(table) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    last_key = None

    while True:
        kwargs: dict[str, Any] = {
            "KeyConditionExpression": Key("pk").eq(SILVER_CHECKPOINT_PK),
            "ConsistentRead": False,
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        resp = table.query(**kwargs)
        for item in resp.get("Items", []):
            if str(item.get("status", "")).strip().lower() != "success":
                continue
            if not str(item.get("round_s3_key", "")).strip():
                continue
            out.append(item)

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    return out


def load_weather_event_candidates(
    *,
    table_name: str,
    aws_region: Optional[str],
    event_ids: list[int] | None = None,
) -> list[WeatherEventCandidate]:
    table = _ddb_resource(aws_region).Table(table_name)

    checkpoints: list[dict[str, Any]] = []
    if event_ids:
        for event_id in sorted({int(x) for x in event_ids}):
            checkpoint = _get_item(table, pk=SILVER_CHECKPOINT_PK, sk=f"EVENT#{event_id}")
            if not checkpoint:
                continue
            if str(checkpoint.get("status", "")).strip().lower() != "success":
                continue
            if not str(checkpoint.get("round_s3_key", "")).strip():
                continue
            checkpoints.append(checkpoint)
    else:
        checkpoints = _load_success_silver_checkpoints(table)

    out: list[WeatherEventCandidate] = []
    for checkpoint in checkpoints:
        event_id_raw = checkpoint.get("event_id")
        if event_id_raw is None:
            continue

        event_id = int(event_id_raw)
        metadata = _get_item(table, pk=f"EVENT#{event_id}", sk="METADATA")
        if not metadata:
            continue

        out.append(
            WeatherEventCandidate(
                event_id=event_id,
                event_metadata=metadata,
                silver_checkpoint=checkpoint,
            )
        )

    out.sort(key=lambda c: c.event_id)
    return out


def get_event_weather_summary(
    *,
    table_name: str,
    event_id: int,
    aws_region: Optional[str],
) -> dict[str, Any] | None:
    table = _ddb_resource(aws_region).Table(table_name)
    return _get_item(table, pk=f"EVENT#{int(event_id)}", sk="WEATHER_OBS#SUMMARY")


def get_event_weather_summaries(
    *,
    table_name: str,
    event_ids: list[int],
    aws_region: Optional[str],
) -> dict[int, dict[str, Any]]:
    """
    Batch-load WEATHER_OBS#SUMMARY items keyed by event_id.
    """
    client = _ddb_client(aws_region)
    wanted_ids = sorted({int(x) for x in event_ids})
    out: dict[int, dict[str, Any]] = {}

    def _chunk(values: list[int], size: int) -> list[list[int]]:
        return [values[i:i + size] for i in range(0, len(values), size)]

    for chunk_ids in _chunk(wanted_ids, 100):
        request_items = {
            table_name: {
                "Keys": [
                    {
                        "pk": {"S": f"EVENT#{event_id}"},
                        "sk": {"S": "WEATHER_OBS#SUMMARY"},
                    }
                    for event_id in chunk_ids
                ]
            }
        }

        while request_items:
            resp = client.batch_get_item(RequestItems=request_items)

            for item in resp.get("Responses", {}).get(table_name, []):
                pk = item.get("pk", {}).get("S", "")
                event_id_text = pk.replace("EVENT#", "", 1)
                if not event_id_text:
                    continue

                event_id = int(event_id_text)
                out[event_id] = {
                    "pk": pk,
                    "sk": item.get("sk", {}).get("S", ""),
                    "event_id": int(item.get("event_id", {}).get("N", event_id)),
                    "pipeline": item.get("pipeline", {}).get("S", ""),
                    "last_run_id": item.get("last_run_id", {}).get("S", ""),
                    "updated_at": item.get("updated_at", {}).get("S", ""),
                    "last_silver_checkpoint_updated_at": item.get("last_silver_checkpoint_updated_at", {}).get("S", ""),
                    "status": item.get("status", {}).get("S", ""),
                    "error_type": item.get("error_type", {}).get("S", ""),
                    "error_message": item.get("error_message", {}).get("S", ""),
                }

            request_items = resp.get("UnprocessedKeys", {})

    return out


def get_cached_geocode(
    *,
    table_name: str,
    query_fingerprint: str,
    aws_region: Optional[str],
) -> dict[str, Any] | None:
    table = _ddb_resource(aws_region).Table(table_name)
    return _get_item(table, pk=f"GEO#QUERY#{query_fingerprint}", sk="WEATHER_GEO#CACHE")
