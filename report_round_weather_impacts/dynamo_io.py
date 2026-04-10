from __future__ import annotations

import math
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3

from report_round_weather_impacts.models import PIPELINE_NAME, REPORT_CHECKPOINT_PK


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ddb_resource(aws_region: str | None):
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


def get_report_table_checkpoint(
    *,
    table_name: str,
    report_table: str,
    report_policy_version: str,
    aws_region: str | None,
) -> dict[str, Any] | None:
    table = _ddb_resource(aws_region).Table(table_name)
    resp = table.get_item(
        Key={
            "pk": REPORT_CHECKPOINT_PK,
            "sk": f"TABLE#{report_table}#REPORT_POLICY#{report_policy_version}",
        },
        ConsistentRead=False,
    )
    return resp.get("Item")


def put_report_table_checkpoint(
    *,
    table_name: str,
    report_table: str,
    report_policy_version: str,
    run_id: str,
    status: str,
    aws_region: str | None,
    extra_attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    table = _ddb_resource(aws_region).Table(table_name)
    item = {
        "pk": REPORT_CHECKPOINT_PK,
        "sk": f"TABLE#{report_table}#REPORT_POLICY#{report_policy_version}",
        "pipeline": PIPELINE_NAME,
        "report_table": report_table,
        "report_policy_version": report_policy_version,
        "status": status,
        "last_run_id": run_id,
        "updated_at": utc_now_iso(),
    }
    if extra_attributes:
        item.update(extra_attributes)
    safe_item = _to_dynamodb_safe(item)
    table.put_item(Item=safe_item)
    return safe_item


def put_report_run_summary(
    *,
    table_name: str,
    run_id: str,
    stats: dict[str, Any],
    aws_region: str | None,
) -> dict[str, Any]:
    table = _ddb_resource(aws_region).Table(table_name)
    item = {
        "pk": f"RUN#{run_id}",
        "sk": "REPORT_ROUND_WEATHER_IMPACTS#SUMMARY",
        "run_id": run_id,
        "created_at": utc_now_iso(),
        **stats,
    }
    safe_item = _to_dynamodb_safe(item)
    table.put_item(Item=safe_item)
    return safe_item
