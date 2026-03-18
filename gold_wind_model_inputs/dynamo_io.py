from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import boto3
from boto3.dynamodb.conditions import Key

from gold_wind_model_inputs.models import GOLD_MODEL_INPUTS_CHECKPOINT_PK, PIPELINE_NAME


@dataclass(frozen=True)
class ModelInputsEventCandidate:
    event_id: int
    event_year: int
    hole_s3_key: str
    round_s3_key: str | None = None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ddb_resource(aws_region: Optional[str]):
    return boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")


def _query_pipeline_items(*, table, pk: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    last_key = None
    while True:
        kwargs: dict[str, Any] = {
            "KeyConditionExpression": Key("pk").eq(pk),
            "ConsistentRead": False,
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        resp = table.query(**kwargs)
        out.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return out


def _get_item(table, *, pk: str, sk: str) -> dict[str, Any] | None:
    resp = table.get_item(Key={"pk": pk, "sk": sk}, ConsistentRead=False)
    return resp.get("Item")


def load_model_inputs_event_candidates(
    *,
    table_name: str,
    aws_region: Optional[str],
    event_ids: list[int] | None = None,
) -> list[ModelInputsEventCandidate]:
    table = _ddb_resource(aws_region).Table(table_name)
    gold_items = _query_pipeline_items(table=table, pk="PIPELINE#GOLD_WIND_EFFECTS")

    success_items: dict[int, dict[str, Any]] = {}
    for item in gold_items:
        if str(item.get("status", "")).strip().lower() != "success":
            continue

        event_id = item.get("event_id")
        if event_id is None:
            continue

        hole_key = str(item.get("hole_s3_key", "")).strip()
        if not hole_key:
            continue

        success_items[int(event_id)] = item

    candidate_ids = sorted(success_items.keys())
    if event_ids:
        allowed = {int(x) for x in event_ids}
        candidate_ids = [eid for eid in candidate_ids if eid in allowed]

    out: list[ModelInputsEventCandidate] = []
    for event_id in candidate_ids:
        src = success_items[event_id]

        meta = _get_item(table, pk=f"EVENT#{event_id}", sk="METADATA") or {}
        event_year = 0
        start_date = str(meta.get("start_date", "")).strip()
        if len(start_date) >= 4 and start_date[:4].isdigit():
            event_year = int(start_date[:4])
        if event_year == 0:
            event_year = int(src.get("event_year", 0) or 0)

        round_key = str(src.get("round_s3_key", "")).strip() or None

        out.append(
            ModelInputsEventCandidate(
                event_id=event_id,
                event_year=event_year,
                hole_s3_key=str(src.get("hole_s3_key", "")).strip(),
                round_s3_key=round_key,
            )
        )

    return out


def load_model_inputs_event_checkpoints(
    *,
    table_name: str,
    aws_region: Optional[str],
) -> dict[int, dict[str, Any]]:
    table = _ddb_resource(aws_region).Table(table_name)
    items = _query_pipeline_items(table=table, pk=GOLD_MODEL_INPUTS_CHECKPOINT_PK)

    out: dict[int, dict[str, Any]] = {}
    for item in items:
        event_id = item.get("event_id")
        if event_id is None:
            sk = str(item.get("sk", ""))
            if sk.startswith("EVENT#"):
                try:
                    event_id = int(sk.replace("EVENT#", "", 1))
                except ValueError:
                    event_id = None
        if event_id is None:
            continue
        out[int(event_id)] = item
    return out


def get_model_inputs_event_checkpoint(
    *,
    table_name: str,
    event_id: int,
    aws_region: Optional[str],
) -> dict[str, Any] | None:
    table = _ddb_resource(aws_region).Table(table_name)
    return _get_item(
        table,
        pk=GOLD_MODEL_INPUTS_CHECKPOINT_PK,
        sk=f"EVENT#{int(event_id)}",
    )


def put_model_inputs_event_checkpoint(
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
        "pk": GOLD_MODEL_INPUTS_CHECKPOINT_PK,
        "sk": f"EVENT#{int(event_id)}",
        "event_id": int(event_id),
        "pipeline": PIPELINE_NAME,
        "status": status,
        "event_source_fingerprint": event_source_fingerprint,
        "last_run_id": run_id,
        "updated_at": utc_now_iso(),
    }
    if extra_attributes:
        item.update(extra_attributes)

    table.put_item(Item=item)
    return item


def put_model_inputs_run_summary(
    *,
    table_name: str,
    run_id: str,
    stats: dict[str, Any],
    aws_region: Optional[str],
) -> dict[str, Any]:
    table = _ddb_resource(aws_region).Table(table_name)

    item = {
        "pk": f"RUN#{run_id}",
        "sk": "GOLD_WIND_MODEL_INPUTS#SUMMARY",
        "run_id": run_id,
        "created_at": utc_now_iso(),
        **stats,
    }
    table.put_item(Item=item)
    return item