from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import boto3
from botocore.exceptions import ClientError


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _pipeline_pk(pipeline_name: str) -> str:
    normalized = pipeline_name.strip().upper()
    if not normalized:
        raise ValueError("pipeline_name must not be empty")
    return f"SILVER#{normalized}"


def _checkpoint_sk() -> str:
    return "CHECKPOINT#GLOBAL"


def _unit_state_sk(unit_key: str) -> str:
    return f"STATE#UNIT#{unit_key}"


def _run_summary_sk(run_id: str) -> str:
    return f"RUN#{run_id}"


def build_round_unit_key(event_id: int, division: str, round_number: int) -> str:
    division_norm = division.strip().upper()
    if not division_norm:
        raise ValueError("division must not be empty")
    return f"EVENT#{int(event_id)}#DIV#{division_norm}#ROUND#{int(round_number)}"


def _coerce_non_negative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, Decimal):
        try:
            if value != value.to_integral_value():
                return None
            parsed = int(value)
            return parsed if parsed >= 0 else None
        except (InvalidOperation, ValueError):
            return None
    return None


@dataclass(frozen=True)
class GlobalCheckpoint:
    last_processed_fetch_ts: str | None = None
    last_processed_s3_key: str | None = None
    last_run_id: str | None = None
    updated_at: str | None = None

    def has_cursor(self) -> bool:
        return bool(self.last_processed_fetch_ts and self.last_processed_s3_key)


@dataclass(frozen=True)
class UnitState:
    unit_key: str
    last_applied_sha256: str
    last_applied_fetch_ts: str
    last_applied_s3_key: str
    last_applied_row_count: int
    last_run_id: str | None
    updated_at: str | None


def get_global_checkpoint(
    *,
    table_name: str,
    pipeline_name: str,
    aws_region: str | None = None,
) -> GlobalCheckpoint:
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    resp = table.get_item(
        Key={"pk": _pipeline_pk(pipeline_name), "sk": _checkpoint_sk()},
        ConsistentRead=False,
    )
    item = resp.get("Item")
    if not item:
        return GlobalCheckpoint()

    return GlobalCheckpoint(
        last_processed_fetch_ts=item.get("last_processed_fetch_ts"),
        last_processed_s3_key=item.get("last_processed_s3_key"),
        last_run_id=item.get("last_run_id"),
        updated_at=item.get("updated_at"),
    )


def put_global_checkpoint(
    *,
    table_name: str,
    pipeline_name: str,
    last_processed_fetch_ts: str,
    last_processed_s3_key: str,
    run_id: str,
    aws_region: str | None = None,
) -> bool:
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    now = utc_now_iso()
    try:
        table.update_item(
            Key={"pk": _pipeline_pk(pipeline_name), "sk": _checkpoint_sk()},
            UpdateExpression="""
            SET
                last_processed_fetch_ts = :new_fetch_ts,
                last_processed_s3_key = :new_s3_key,
                last_run_id = :run_id,
                updated_at = :updated_at
            """,
            ExpressionAttributeValues={
                ":new_fetch_ts": last_processed_fetch_ts,
                ":new_s3_key": last_processed_s3_key,
                ":run_id": run_id,
                ":updated_at": now,
            },
            ConditionExpression="""
                attribute_not_exists(last_processed_fetch_ts)
                OR :new_fetch_ts > last_processed_fetch_ts
                OR (
                    :new_fetch_ts = last_processed_fetch_ts
                    AND (
                        attribute_not_exists(last_processed_s3_key)
                        OR :new_s3_key >= last_processed_s3_key
                    )
                )
            """,
            ReturnValues="ALL_NEW",
        )
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code == "ConditionalCheckFailedException":
            return False
        raise


def get_round_unit_state(
    *,
    table_name: str,
    pipeline_name: str,
    unit_key: str,
    aws_region: str | None = None,
) -> UnitState | None:
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    resp = table.get_item(
        Key={"pk": _pipeline_pk(pipeline_name), "sk": _unit_state_sk(unit_key)},
        ConsistentRead=False,
    )
    item = resp.get("Item")
    if not item:
        return None

    row_count = _coerce_non_negative_int(item.get("last_applied_row_count")) or 0
    return UnitState(
        unit_key=unit_key,
        last_applied_sha256=str(item.get("last_applied_sha256", "")),
        last_applied_fetch_ts=str(item.get("last_applied_fetch_ts", "")),
        last_applied_s3_key=str(item.get("last_applied_s3_key", "")),
        last_applied_row_count=row_count,
        last_run_id=item.get("last_run_id"),
        updated_at=item.get("updated_at"),
    )


def put_round_unit_state(
    *,
    table_name: str,
    pipeline_name: str,
    unit_key: str,
    last_applied_sha256: str,
    last_applied_fetch_ts: str,
    last_applied_s3_key: str,
    last_applied_row_count: int,
    run_id: str,
    aws_region: str | None = None,
) -> bool:
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    now = utc_now_iso()
    try:
        table.update_item(
            Key={"pk": _pipeline_pk(pipeline_name), "sk": _unit_state_sk(unit_key)},
            UpdateExpression="""
            SET
                last_applied_sha256 = :sha256,
                last_applied_fetch_ts = :new_fetch_ts,
                last_applied_s3_key = :new_s3_key,
                last_applied_row_count = :row_count,
                last_run_id = :run_id,
                updated_at = :updated_at,
                first_applied_at = if_not_exists(first_applied_at, :updated_at)
            """,
            ExpressionAttributeValues={
                ":sha256": last_applied_sha256,
                ":new_fetch_ts": last_applied_fetch_ts,
                ":new_s3_key": last_applied_s3_key,
                ":row_count": int(last_applied_row_count),
                ":run_id": run_id,
                ":updated_at": now,
            },
            ConditionExpression="""
                attribute_not_exists(last_applied_fetch_ts)
                OR :new_fetch_ts > last_applied_fetch_ts
                OR (
                    :new_fetch_ts = last_applied_fetch_ts
                    AND (
                        attribute_not_exists(last_applied_s3_key)
                        OR :new_s3_key >= last_applied_s3_key
                    )
                )
            """,
            ReturnValues="ALL_NEW",
        )
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code == "ConditionalCheckFailedException":
            return False
        raise


def put_run_summary(
    *,
    table_name: str,
    pipeline_name: str,
    run_id: str,
    status: str,
    stats: dict[str, int],
    aws_region: str | None = None,
) -> dict[str, Any]:
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    now = utc_now_iso()
    item = {
        "pk": _pipeline_pk(pipeline_name),
        "sk": _run_summary_sk(run_id),
        "run_id": run_id,
        "status": status,
        "created_at": now,
        **stats,
    }
    table.put_item(Item=item)
    return item