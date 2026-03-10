from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Optional

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from silver_pdga_live_results.models import FINAL_EVENT_STATUSES


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ddb_resource(aws_region: Optional[str]):
    return boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")


def _get_metadata_item_by_event_id(table, event_id: int) -> dict[str, Any] | None:
    resp = table.get_item(Key={"pk": f"EVENT#{int(event_id)}", "sk": "METADATA"}, ConsistentRead=False)
    return resp.get("Item")


def _iter_final_events_via_gsi(
    *,
    table,
    gsi_name: str,
    end_before_date: str,
) -> list[dict[str, Any]]:
    """
    Query existing status/end_date GSI for finalized statuses and return METADATA-like projected items.
    Falls back to no filter on live_results_ingested if not projected in GSI.
    """
    out: list[dict[str, Any]] = []
    seen_event_ids: set[int] = set()

    for status in FINAL_EVENT_STATUSES:
        last_key = None
        while True:
            kwargs: dict[str, Any] = {
                "IndexName": gsi_name,
                "KeyConditionExpression": Key("status_text").eq(status) & Key("end_date").lt(end_before_date),
                "ProjectionExpression": "event_id, #pk, #sk, status_text, end_date, live_results_ingested",
                "ExpressionAttributeNames": {"#pk": "pk", "#sk": "sk"},
                "FilterExpression": Attr("live_results_ingested").eq(True),
                "ConsistentRead": False,
            }
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key

            try:
                resp = table.query(**kwargs)
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                # Fallback if live_results_ingested is not projected in GSI
                if code == "ValidationException" and "live_results_ingested" in str(exc):
                    kwargs.pop("FilterExpression", None)
                    resp = table.query(**kwargs)
                else:
                    raise

            for item in resp.get("Items", []):
                if item.get("sk") != "METADATA":
                    continue
                event_id = item.get("event_id")
                if event_id is None:
                    continue
                event_id_int = int(event_id)
                if event_id_int in seen_event_ids:
                    continue
                seen_event_ids.add(event_id_int)
                out.append(item)

            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break

    return out


def _scan_final_events_fallback(*, table) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    last_key = None

    while True:
        kwargs: dict[str, Any] = {
            "FilterExpression": Attr("sk").eq("METADATA"),
            "ProjectionExpression": (
                "event_id, #pk, #sk, status_text, end_date, start_date, "
                "live_results_ingested, division_rounds, #name, "
                "location_raw, raw_location, city, #state, country"
            ),
            "ExpressionAttributeNames": {"#pk": "pk", "#sk": "sk", "#name": "name", "#state": "state"},
            "ConsistentRead": False,
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        resp = table.scan(**kwargs)
        for item in resp.get("Items", []):
            status = str(item.get("status_text", "")).strip()
            if status not in FINAL_EVENT_STATUSES:
                continue
            if not bool(item.get("live_results_ingested", False)):
                continue
            if item.get("event_id") is None:
                continue
            out.append(item)

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    # de-dup by event_id
    dedup: dict[int, dict[str, Any]] = {}
    for item in out:
        dedup[int(item["event_id"])] = item
    return [dedup[k] for k in sorted(dedup.keys())]


def load_candidate_event_metadata(
    *,
    table_name: str,
    aws_region: Optional[str],
    status_end_date_gsi_name: str,
    event_ids: list[int] | None = None,
) -> list[dict[str, Any]]:
    """
    Load finalized + live_results_ingested events.

    If event_ids are provided: direct METADATA gets.
    Else: query status/end_date GSI and then hydrate full METADATA rows.
    """
    table = _ddb_resource(aws_region).Table(table_name)

    if event_ids:
        out: list[dict[str, Any]] = []
        for event_id in sorted({int(x) for x in event_ids}):
            item = _get_metadata_item_by_event_id(table, event_id)
            if not item:
                continue

            status = str(item.get("status_text", "")).strip()
            if status not in FINAL_EVENT_STATUSES:
                continue
            if not bool(item.get("live_results_ingested", False)):
                continue
            out.append(item)
        return out

    # Preferred: GSI path
    try:
        projected = _iter_final_events_via_gsi(
            table=table,
            gsi_name=status_end_date_gsi_name,
            end_before_date=date.today().isoformat(),
        )

        # Hydrate full METADATA records (so we have name/location/division_rounds/etc)
        hydrated: list[dict[str, Any]] = []
        for item in projected:
            event_id = int(item["event_id"])
            full = _get_metadata_item_by_event_id(table, event_id)
            if not full:
                continue
            status = str(full.get("status_text", "")).strip()
            if status not in FINAL_EVENT_STATUSES:
                continue
            if not bool(full.get("live_results_ingested", False)):
                continue
            hydrated.append(full)

        hydrated.sort(key=lambda x: int(x.get("event_id", 0)))
        return hydrated

    except ClientError:
        # Fallback: full table scan
        return _scan_final_events_fallback(table=table)


def load_live_results_state_items(
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
            "KeyConditionExpression": Key("pk").eq(f"EVENT#{int(event_id)}") & Key("sk").begins_with("LIVE_RESULTS#DIV#"),
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


def get_silver_event_checkpoint(
    *,
    table_name: str,
    event_id: int,
    aws_region: Optional[str],
) -> dict[str, Any] | None:
    table = _ddb_resource(aws_region).Table(table_name)
    resp = table.get_item(
        Key={"pk": "PIPELINE#SILVER_LIVE_RESULTS", "sk": f"EVENT#{int(event_id)}"},
        ConsistentRead=False,
    )
    return resp.get("Item")


def put_silver_event_checkpoint(
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
        "pk": "PIPELINE#SILVER_LIVE_RESULTS",
        "sk": f"EVENT#{int(event_id)}",
        "event_id": int(event_id),
        "pipeline": "silver_live_results",
        "status": status,
        "event_source_fingerprint": event_source_fingerprint,
        "last_run_id": run_id,
        "updated_at": utc_now_iso(),
    }
    if extra_attributes:
        item.update(extra_attributes)

    table.put_item(Item=item)
    return item


def put_silver_run_summary(
    *,
    table_name: str,
    run_id: str,
    stats: dict[str, Any],
    aws_region: Optional[str],
) -> dict[str, Any]:
    table = _ddb_resource(aws_region).Table(table_name)

    item = {
        "pk": f"RUN#{run_id}",
        "sk": "SILVER_LIVE_RESULTS#SUMMARY",
        "run_id": run_id,
        "created_at": utc_now_iso(),
        **stats,
    }
    table.put_item(Item=item)
    return item