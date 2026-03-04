from __future__ import annotations

from datetime import date, timedelta
from typing import Iterator, Optional

import boto3
from boto3.dynamodb.conditions import Key


def get_existing_content_sha256(*, table_name: str, event_id: int, aws_region: Optional[str] = None) -> Optional[str]:
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    pk = f"EVENT#{int(event_id)}"
    sk = "METADATA"

    resp = table.get_item(Key={"pk": pk, "sk": sk}, ConsistentRead=False)
    item = resp.get("Item")
    if not item:
        return None
    return item.get("idempotency_sha256")


def iter_rescrape_event_ids_via_gsi(
    *,
    table_name: str,
    gsi_name: str,
    status_texts: list[str],
    start_date: str,
    end_before_date: str,
    aws_region: Optional[str] = None,
) -> Iterator[int]:
    """
    Query candidates via GSI:
      PK: status_text
      SK: end_date (YYYY-MM-DD)
    Filter target window: start_date <= end_date < end_before_date
    """
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    wanted_statuses = [s.strip() for s in status_texts if s.strip()]
    if not wanted_statuses:
        return

    end_exclusive = date.fromisoformat(end_before_date)
    end_inclusive = (end_exclusive - timedelta(days=1)).isoformat()
    if start_date > end_inclusive:
        return

    for status in wanted_statuses:
        last_evaluated_key = None
        while True:
            query_kwargs = {
                "IndexName": gsi_name,
                "KeyConditionExpression": Key("status_text").eq(status) & Key("end_date").between(start_date, end_inclusive),
                "ProjectionExpression": "event_id, #pk, #sk, status_text, end_date",
                "ExpressionAttributeNames": {"#pk": "pk", "#sk": "sk"},
            }
            if last_evaluated_key:
                query_kwargs["ExclusiveStartKey"] = last_evaluated_key

            resp = table.query(**query_kwargs)

            for item in resp.get("Items", []):
                if item.get("sk") != "METADATA":
                    continue
                event_id = item.get("event_id")
                if event_id is not None:
                    yield int(event_id)

            last_evaluated_key = resp.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break


def get_max_event_id(*, table_name: str, aws_region: Optional[str] = None) -> Optional[int]:
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    max_event_id: Optional[int] = None
    last_evaluated_key = None

    while True:
        scan_kwargs = {"ProjectionExpression": "event_id, sk"}
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        resp = table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            if item.get("sk") != "METADATA":
                continue
            event_id = item.get("event_id")
            if event_id is None:
                continue
            event_id = int(event_id)
            if max_event_id is None or event_id > max_event_id:
                max_event_id = event_id

        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    return max_event_id
