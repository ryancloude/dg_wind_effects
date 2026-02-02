# ingest_pdga_event_pages/dynamo_writer.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

import boto3


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
    Upsert ONE item per event:
      pk = EVENT#<event_id>
      sk = METADATA

    Stores:
      - discovery fields from parsed (name, dates, status, division_rounds, content_sha256, parser_version)
      - pointers to the latest raw HTML/meta in S3
      - timestamps (first_seen_at, last_fetched_at)
    """
    event_id = int(parsed["event_id"])

    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    pk = f"EVENT#{event_id}"
    sk = "METADATA"

    now = utc_now_iso()

    # Normalize division_rounds values to int (Dynamo stores numbers, boto3 handles ints fine)
    division_rounds = {k: int(v) for k, v in (parsed.get("division_rounds") or {}).items()}

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
    content_sha256 = :content_sha256,
    parse_warnings = :parse_warnings,
    parser_version = :parser_version,
    latest_s3_html_key = :latest_s3_html_key,
    latest_s3_meta_key = :latest_s3_meta_key,
    last_fetched_at = :last_fetched_at,
    first_seen_at = if_not_exists(first_seen_at, :first_seen_at),
    idempotency_sha256 = :idempotency_sha256,
    raw_html_sha256 = :raw_html_sha256
    """

    expr_attr_names = {
        "#name": "name",  # "name" is sometimes reserved-ish; this avoids surprises
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
        ":content_sha256": parsed.get("content_sha256", ""),
        ":parse_warnings": parsed.get("parse_warnings", []),
        ":parser_version": parsed.get("parser_version", ""),
        ":latest_s3_html_key": s3_ptrs.get("s3_html_key", ""),
        ":latest_s3_meta_key": s3_ptrs.get("s3_meta_key", ""),
        ":last_fetched_at": s3_ptrs.get("fetched_at", now),
        ":first_seen_at": now,
        ":idempotency_sha256": parsed.get("idempotency_sha256", ""),
        ":raw_html_sha256": parsed.get("raw_html_sha256", ""),
    }

    resp = table.update_item(
        Key={"pk": pk, "sk": sk},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=expr_attr_names,
        ExpressionAttributeValues=expr_attr_values,
        ReturnValues="ALL_NEW",
    )

    return resp.get("Attributes", {})