from __future__ import annotations

import math
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

import boto3

from score_round_wind_model.models import PIPELINE_NAME, SCORE_CHECKPOINT_PK


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ddb_resource(aws_region: Optional[str]):
    return boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")


def _to_dynamodb_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return Decimal(str(value))
    if hasattr(value, "item") and callable(value.item):
        try:
            return _to_dynamodb_safe(value.item())
        except Exception:
            pass
    if isinstance(value, dict):
        return {str(k): _to_dynamodb_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_dynamodb_safe(v) for v in value]
    return value


def get_score_checkpoint(
    *,
    table_name: str,
    event_id: int,
    training_request_fingerprint: str,
    aws_region: Optional[str],
) -> dict[str, Any] | None:
    table = _ddb_resource(aws_region).Table(table_name)
    resp = table.get_item(
        Key={
            "pk": SCORE_CHECKPOINT_PK,
            "sk": f"EVENT#{int(event_id)}#MODEL#{training_request_fingerprint}",
        },
        ConsistentRead=False,
    )
    return resp.get("Item")


def put_score_checkpoint(
    *,
    table_name: str,
    event_id: int,
    training_request_fingerprint: str,
    run_id: str,
    status: str,
    aws_region: Optional[str],
    extra_attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    table = _ddb_resource(aws_region).Table(table_name)

    checkpoint_item = {
        "pk": SCORE_CHECKPOINT_PK,
        "sk": f"EVENT#{int(event_id)}#MODEL#{training_request_fingerprint}",
        "pipeline": PIPELINE_NAME,
        "event_id": int(event_id),
        "training_request_fingerprint": training_request_fingerprint,
        "status": status,
        "last_run_id": run_id,
        "updated_at": utc_now_iso(),
    }
    if extra_attributes:
        checkpoint_item.update(extra_attributes)

    safe_checkpoint_item = _to_dynamodb_safe(checkpoint_item)
    table.put_item(Item=safe_checkpoint_item)
    return safe_checkpoint_item


def put_score_run_summary(
    *,
    table_name: str,
    run_id: str,
    stats: dict[str, Any],
    aws_region: Optional[str],
) -> dict[str, Any]:
    table = _ddb_resource(aws_region).Table(table_name)

    summary_item = {
        "pk": f"RUN#{run_id}",
        "sk": "SCORE_ROUND_WIND_MODEL#SUMMARY",
        "run_id": run_id,
        "created_at": utc_now_iso(),
        **stats,
    }

    safe_summary_item = _to_dynamodb_safe(summary_item)
    table.put_item(Item=safe_summary_item)
    return safe_summary_item