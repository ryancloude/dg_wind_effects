from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

import boto3
from boto3.dynamodb.conditions import Attr, Key

SILVER_WEATHER_CHECKPOINT_PK = "PIPELINE#SILVER_WEATHER_OBSERVATIONS"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ddb_resource(aws_region: Optional[str]):
    return boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")


def load_weather_event_summaries(
    *,
    table_name: str,
    aws_region: Optional[str],
    event_ids: list[int] | None = None,
) -> list[dict[str, Any]]:
    table = _ddb_resource(aws_region).Table(table_name)
    out: list[dict[str, Any]] = []

    if event_ids:
        for event_id in sorted({int(x) for x in event_ids}):
            resp = table.get_item(
                Key={"pk": f"EVENT#{event_id}", "sk": "WEATHER_OBS#SUMMARY"},
                ConsistentRead=False,
            )
            item = resp.get("Item")
            if item:
                out.append(item)
        return out

    last_key = None
    while True:
        kwargs: dict[str, Any] = {
            "FilterExpression": Attr("sk").eq("WEATHER_OBS#SUMMARY"),
            "ProjectionExpression": "pk, sk, event_id, updated_at, last_silver_checkpoint_updated_at",
            "ConsistentRead": False,
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        resp = table.scan(**kwargs)
        out.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    out.sort(key=lambda x: int(x.get("event_id", 0)))
    return out


def get_event_metadata(
    *,
    table_name: str,
    event_id: int,
    aws_region: Optional[str],
) -> dict[str, Any] | None:
    table = _ddb_resource(aws_region).Table(table_name)
    resp = table.get_item(
        Key={"pk": f"EVENT#{int(event_id)}", "sk": "METADATA"},
        ConsistentRead=False,
    )
    return resp.get("Item")


def load_weather_state_items(
    *,
    table_name: str,
    event_id: int,
    aws_region: Optional[str],
) -> list[dict[str, Any]]:
    table = _ddb_resource(aws_region).Table(table_name)

    items: list[dict[str, Any]] = []
    last_key = None
    while True:
        kwargs: dict[str, Any] = {
            "KeyConditionExpression": Key("pk").eq(f"EVENT#{int(event_id)}") & Key("sk").begins_with("WEATHER_OBS#ROUND#"),
            "ConsistentRead": False,
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    return items


def load_silver_weather_event_checkpoints(
    *,
    table_name: str,
    aws_region: Optional[str],
) -> dict[int, dict[str, Any]]:
    table = _ddb_resource(aws_region).Table(table_name)

    checkpoints: dict[int, dict[str, Any]] = {}
    last_key = None

    while True:
        kwargs: dict[str, Any] = {
            "KeyConditionExpression": Key("pk").eq(SILVER_WEATHER_CHECKPOINT_PK),
            "ConsistentRead": False,
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        resp = table.query(**kwargs)
        for item in resp.get("Items", []):
            event_id_raw = item.get("event_id")
            if event_id_raw is None:
                sk = str(item.get("sk", ""))
                if sk.startswith("EVENT#"):
                    try:
                        event_id_raw = int(sk.replace("EVENT#", "", 1))
                    except ValueError:
                        event_id_raw = None

            if event_id_raw is None:
                continue

            checkpoints[int(event_id_raw)] = item

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    return checkpoints


def get_silver_weather_event_checkpoint(
    *,
    table_name: str,
    event_id: int,
    aws_region: Optional[str],
) -> dict[str, Any] | None:
    table = _ddb_resource(aws_region).Table(table_name)
    resp = table.get_item(
        Key={"pk": SILVER_WEATHER_CHECKPOINT_PK, "sk": f"EVENT#{int(event_id)}"},
        ConsistentRead=False,
    )
    return resp.get("Item")


def put_silver_weather_event_checkpoint(
    *,
    table_name: str,
    event_id: int,
    run_id: str,
    status: str,
    event_source_fingerprint: str,
    aws_region: Optional[str],
    extra_attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    table = _ddb_resource(aws_region).Table(table_name)

    item = {
        "pk": SILVER_WEATHER_CHECKPOINT_PK,
        "sk": f"EVENT#{int(event_id)}",
        "event_id": int(event_id),
        "pipeline": "silver_weather_observations",
        "status": status,
        "event_source_fingerprint": event_source_fingerprint,
        "last_run_id": run_id,
        "updated_at": utc_now_iso(),
    }
    if extra_attributes:
        item.update(extra_attributes)

    table.put_item(Item=item)
    return item


def put_silver_weather_run_summary(
    *,
    table_name: str,
    run_id: str,
    stats: dict[str, Any],
    aws_region: Optional[str],
) -> dict[str, Any]:
    table = _ddb_resource(aws_region).Table(table_name)

    item = {
        "pk": f"RUN#{run_id}",
        "sk": "SILVER_WEATHER_OBSERVATIONS#SUMMARY",
        "run_id": run_id,
        "created_at": utc_now_iso(),
        **stats,
    }
    table.put_item(Item=item)
    return item