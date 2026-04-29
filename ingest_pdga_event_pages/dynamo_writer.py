from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def upsert_event_metadata(
    *,
    table_name: str,
    parsed: Dict[str, Any],
    s3_ptrs: Dict[str, Any],
    aws_region: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Upsert one item per event:
      pk = EVENT#<event_id>
      sk = METADATA
    """
    event_id = int(parsed["event_id"])

    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    pk = f"EVENT#{event_id}"
    sk = "METADATA"
    now = utc_now_iso()

    division_rounds = {key: int(value) for key, value in (parsed.get("division_rounds") or {}).items()}

    update_expr = """
    SET
        event_id = :event_id,
        source_url = :source_url,
        #name = :name,
        raw_date_str = :raw_date_str,
        start_date = :start_date,
        end_date = :end_date,
        status_text = :status_text,
        division_rounds = :division_rounds,
        location_raw = :location_raw,
        city = :city,
        #state = :state,
        country = :country,
        content_sha256 = :content_sha256,
        parse_warnings = :parse_warnings,
        parser_version = :parser_version,
        latest_s3_html_key = :latest_s3_html_key,
        latest_s3_meta_key = :latest_s3_meta_key,
        last_fetched_at = :last_fetched_at,
        first_seen_at = if_not_exists(first_seen_at, :first_seen_at),
        idempotency_sha256 = :idempotency_sha256,
        raw_html_sha256 = :raw_html_sha256,
        is_unscheduled_placeholder = :is_unscheduled_placeholder,
        last_fetch_status = :last_fetch_status,
        last_fetch_failed_at = :last_fetch_failed_at,
        last_fetch_failure_count = :last_fetch_failure_count,
        last_fetch_failure_reason = :last_fetch_failure_reason
    """

    expr_attr_names = {
        "#name": "name",
        "#state": "state",
    }

    expr_attr_values = {
        ":event_id": event_id,
        ":source_url": parsed.get("source_url", ""),
        ":name": parsed.get("name", ""),
        ":raw_date_str": parsed.get("raw_date_str", ""),
        ":start_date": parsed.get("start_date", ""),
        ":end_date": parsed.get("end_date", ""),
        ":status_text": parsed.get("status_text", ""),
        ":division_rounds": division_rounds,
        ":location_raw": parsed.get("location_raw", ""),
        ":city": parsed.get("city", ""),
        ":state": parsed.get("state", ""),
        ":country": parsed.get("country", ""),
        ":content_sha256": parsed.get("content_sha256", ""),
        ":parse_warnings": parsed.get("parse_warnings", []),
        ":parser_version": parsed.get("parser_version", ""),
        ":latest_s3_html_key": s3_ptrs.get("s3_html_key", ""),
        ":latest_s3_meta_key": s3_ptrs.get("s3_meta_key", ""),
        ":last_fetched_at": s3_ptrs.get("fetched_at", now),
        ":first_seen_at": now,
        ":idempotency_sha256": parsed.get("idempotency_sha256", ""),
        ":raw_html_sha256": parsed.get("raw_html_sha256", ""),
        ":is_unscheduled_placeholder": bool(parsed.get("is_unscheduled_placeholder", False)),
        ":last_fetch_status": "success",
        ":last_fetch_failed_at": "",
        ":last_fetch_failure_count": 0,
        ":last_fetch_failure_reason": "",
    }

    resp = table.update_item(
        Key={"pk": pk, "sk": sk},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=expr_attr_names,
        ExpressionAttributeValues=expr_attr_values,
        ReturnValues="ALL_NEW",
    )

    return resp.get("Attributes", {})


def touch_event_fetch_success(
    *,
    table_name: str,
    event_id: int,
    fetched_at: str | None = None,
    aws_region: Optional[str] = None,
) -> bool:
    """
    Record a successful fetch for an existing metadata item without changing
    the latest raw S3 object pointers. This is used for unchanged pages so
    incremental staleness filters still work.
    """
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    pk = f"EVENT#{int(event_id)}"
    sk = "METADATA"
    fetched_at_value = fetched_at or utc_now_iso()

    try:
        table.update_item(
            Key={"pk": pk, "sk": sk},
            ConditionExpression="attribute_exists(pk) AND attribute_exists(sk)",
            UpdateExpression="""
            SET
                last_fetched_at = :last_fetched_at,
                last_fetch_status = :last_fetch_status,
                last_fetch_failed_at = :last_fetch_failed_at,
                last_fetch_failure_count = :last_fetch_failure_count,
                last_fetch_failure_reason = :last_fetch_failure_reason
            """,
            ExpressionAttributeValues={
                ":last_fetched_at": fetched_at_value,
                ":last_fetch_status": "success",
                ":last_fetch_failed_at": "",
                ":last_fetch_failure_count": 0,
                ":last_fetch_failure_reason": "",
            },
        )
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


def record_event_fetch_failure(
    *,
    table_name: str,
    event_id: int,
    error_message: str,
    aws_region: Optional[str] = None,
) -> bool:
    """
    Record failure metadata only for an existing metadata item. This avoids
    creating sparse placeholder METADATA rows for unknown forward-scan IDs.
    """
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    pk = f"EVENT#{int(event_id)}"
    sk = "METADATA"
    now = utc_now_iso()

    try:
        table.update_item(
            Key={"pk": pk, "sk": sk},
            ConditionExpression="attribute_exists(pk) AND attribute_exists(sk)",
            UpdateExpression="""
            SET
                last_fetch_status = :last_fetch_status,
                last_fetch_failed_at = :last_fetch_failed_at,
                last_fetch_failure_reason = :last_fetch_failure_reason
            ADD
                last_fetch_failure_count :failure_increment
            """,
            ExpressionAttributeValues={
                ":last_fetch_status": "failed",
                ":last_fetch_failed_at": now,
                ":last_fetch_failure_reason": error_message[:1000],
                ":failure_increment": 1,
            },
        )
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise
