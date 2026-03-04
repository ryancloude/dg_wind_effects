from __future__ import annotations

from typing import Iterator, Optional

import boto3


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


def iter_rescrape_event_ids(
    *,
    table_name: str,
    status_texts: list[str],
    start_date: str,
    end_before_date: str,
    aws_region: Optional[str] = None,
) -> Iterator[int]:
    """
    Yield event IDs from DynamoDB metadata items where:
    - sk == METADATA
    - status_text is in the requested set
    - start_date <= end_date < end_before_date

    Dates must be ISO strings in YYYY-MM-DD format so string comparison is valid.
    """
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    wanted_statuses = {value.strip() for value in status_texts if value.strip()}
    if not wanted_statuses:
        return

    last_evaluated_key = None

    while True:
        scan_kwargs = {
            "ProjectionExpression": "event_id, sk, status_text, end_date",
        }
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        resp = table.scan(**scan_kwargs)

        for item in resp.get("Items", []):
            if item.get("sk") != "METADATA":
                continue

            status_text = (item.get("status_text") or "").strip()
            end_date = (item.get("end_date") or "").strip()
            event_id = item.get("event_id")

            if status_text not in wanted_statuses:
                continue
            if not end_date:
                continue
            if not (start_date <= end_date < end_before_date):
                continue
            if event_id is None:
                continue

            yield int(event_id)

        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break


def get_max_event_id(*, table_name: str, aws_region: Optional[str] = None) -> Optional[int]:
    """
    Return the largest event_id currently present in DynamoDB metadata items.
    """
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    max_event_id: Optional[int] = None
    last_evaluated_key = None

    while True:
        scan_kwargs = {
            "ProjectionExpression": "event_id, sk",
        }
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