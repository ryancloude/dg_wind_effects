from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

import boto3


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _state_sk(division: str, round_number: int) -> str:
    return f"LIVE_RESULTS#DIV#{division}#ROUND#{int(round_number)}"


def get_existing_live_results_sha256(
    *,
    table_name: str,
    event_id: int,
    division: str,
    round_number: int,
    aws_region: Optional[str] = None,
) -> Optional[str]:
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    resp = table.get_item(
        Key={"pk": f"EVENT#{int(event_id)}", "sk": _state_sk(division, round_number)},
        ConsistentRead=False,
    )
    item = resp.get("Item")
    if not item:
        return None
    return item.get("content_sha256")


def upsert_live_results_state(
    *,
    table_name: str,
    event_id: int,
    division: str,
    round_number: int,
    source_url: str,
    status_text: str,
    content_sha256: str | None,
    s3_ptrs: Dict[str, Any],
    run_id: str,
    aws_region: Optional[str] = None,
) -> Dict[str, Any]:
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    now = utc_now_iso()

    resp = table.update_item(
        Key={"pk": f"EVENT#{int(event_id)}", "sk": _state_sk(division, round_number)},
        UpdateExpression="""
        SET
            event_id = :event_id,
            division = :division,
            round_number = :round_number,
            source_url = :source_url,
            fetch_status = :fetch_status,
            content_sha256 = :content_sha256,
            latest_s3_json_key = :latest_s3_json_key,
            latest_s3_meta_key = :latest_s3_meta_key,
            last_fetched_at = :last_fetched_at,
            last_run_id = :last_run_id,
            first_seen_at = if_not_exists(first_seen_at, :first_seen_at)
        """,
        ExpressionAttributeValues={
            ":event_id": int(event_id),
            ":division": division,
            ":round_number": int(round_number),
            ":source_url": source_url,
            ":fetch_status": status_text,
            ":content_sha256": content_sha256 or "",
            ":latest_s3_json_key": s3_ptrs.get("s3_json_key", ""),
            ":latest_s3_meta_key": s3_ptrs.get("s3_meta_key", ""),
            ":last_fetched_at": s3_ptrs.get("fetched_at", now),
            ":last_run_id": run_id,
            ":first_seen_at": now,
        },
        ReturnValues="ALL_NEW",
    )
    return resp.get("Attributes", {})


def put_live_results_run_summary(
    *,
    table_name: str,
    run_id: str,
    stats: Dict[str, int],
    aws_region: Optional[str] = None,
) -> Dict[str, Any]:
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    now = utc_now_iso()
    item = {
        "pk": f"RUN#{run_id}",
        "sk": "LIVE_RESULTS#SUMMARY",
        "run_id": run_id,
        "created_at": now,
        **stats,
    }
    table.put_item(Item=item)
    return item

def mark_event_live_results_ingested(
    *,
    table_name: str,
    event_id: int,
    run_id: str,
    aws_region: Optional[str] = None,
) -> Dict[str, Any]:
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    now = utc_now_iso()
    resp = table.update_item(
        Key={"pk": f"EVENT#{int(event_id)}", "sk": "METADATA"},
        UpdateExpression="""
        SET
            live_results_ingested = :true_value,
            live_results_ingested_at = :ingested_at,
            live_results_ingested_run_id = :run_id
        """,
        ExpressionAttributeValues={
            ":true_value": True,
            ":ingested_at": now,
            ":run_id": run_id,
        },
        ReturnValues="ALL_NEW",
    )
    return resp.get("Attributes", {})