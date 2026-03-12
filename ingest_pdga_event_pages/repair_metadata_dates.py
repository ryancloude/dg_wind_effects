from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from typing import Any, Iterable

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from ingest_pdga_event_pages.event_page_parser import idempotency_sha256, parse_date_range


REPAIR_WARNING = "date_repaired_from_raw_date_str_v1"
REPAIR_PARSER_VERSION = "event-page-v4-repair"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_args():
    p = argparse.ArgumentParser(
        description="Repair EVENT#*/METADATA date ranges where start_date > end_date using raw_date_str."
    )
    p.add_argument("--ddb-table", default=os.getenv("PDGA_DDB_TABLE"), help="DynamoDB table name")
    p.add_argument("--aws-region", default=os.getenv("AWS_REGION"), help="AWS region (optional)")
    p.add_argument("--event-ids", help="Optional comma-separated event IDs to restrict repair scope")
    p.add_argument("--limit", type=int, help="Optional max number of candidate rows to inspect")
    p.add_argument("--apply", action="store_true", help="Apply updates (default is dry-run)")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def parse_event_ids(raw: str | None) -> set[int] | None:
    if not raw:
        return None
    return {int(x.strip()) for x in raw.split(",") if x.strip()}


def _normalize_warning_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    return []


def _normalize_division_rounds(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, int] = {}
    for k, v in value.items():
        try:
            out[str(k)] = int(v)
        except (TypeError, ValueError):
            continue
    return out

def _build_idempotency_payload(item: dict[str, Any], new_start: str, new_end: str, new_warnings: list[str]) -> dict[str, Any]:
    return {
        "name": item.get("name", ""),
        "raw_date_str": item.get("raw_date_str", ""),
        "start_date": new_start,
        "end_date": new_end,
        "status_text": item.get("status_text", ""),
        "division_rounds": _normalize_division_rounds(item.get("division_rounds")),
        "is_unscheduled_placeholder": bool(item.get("is_unscheduled_placeholder", False)),
        "location_raw": item.get("location_raw", ""),
        "city": item.get("city", ""),
        "state": item.get("state", ""),
        "country": item.get("country", ""),
        "parse_warnings": new_warnings,
    }


def compute_repaired_dates(item: dict[str, Any]) -> tuple[str, str] | None:
    start_date = str(item.get("start_date", "") or "").strip()
    end_date = str(item.get("end_date", "") or "").strip()

    if not start_date or not end_date:
        return None
    if start_date <= end_date:
        return None

    raw_date_str = str(item.get("raw_date_str", "") or "").strip()
    if not raw_date_str:
        return None

    repaired_start, repaired_end = parse_date_range(raw_date_str)
    if repaired_start > repaired_end:
        return None
    return repaired_start, repaired_end


def iter_metadata_items(table, *, event_ids: set[int] | None, limit: int | None) -> Iterable[dict[str, Any]]:
    scanned = 0
    last_key = None

    while True:
        kwargs: dict[str, Any] = {
            "FilterExpression": Attr("sk").eq("METADATA"),
            "ProjectionExpression": (
                "pk, sk, event_id, raw_date_str, start_date, end_date, "
                "parse_warnings, parser_version, #name, status_text, division_rounds, "
                "is_unscheduled_placeholder, location_raw, city, #state, country"
            ),
            "ExpressionAttributeNames": {
                "#name": "name",
                "#state": "state",
            },
            "ConsistentRead": False,
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        resp = table.scan(**kwargs)
        items = resp.get("Items", [])

        for item in items:
            scanned += 1
            event_id = item.get("event_id")
            if event_id is None:
                continue
            event_id_int = int(event_id)

            if event_ids and event_id_int not in event_ids:
                continue

            yield item

            if limit is not None and scanned >= limit:
                return

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break


def apply_repair(table, item: dict[str, Any], repaired_start: str, repaired_end: str, now_iso: str, run_id: str) -> bool:
    old_start = str(item.get("start_date", "") or "")
    old_end = str(item.get("end_date", "") or "")
    warnings = _normalize_warning_list(item.get("parse_warnings"))
    if REPAIR_WARNING not in warnings:
        warnings = warnings + [REPAIR_WARNING]

    payload_for_hash = _build_idempotency_payload(item, repaired_start, repaired_end, warnings)
    new_idempotency = idempotency_sha256(payload_for_hash)

    update_expr = """
    SET
      start_date = :start_date,
      end_date = :end_date,
      parse_warnings = :parse_warnings,
      parser_version = :parser_version,
      idempotency_sha256 = :idempotency_sha256,
      repair_dates_applied_at = :repair_dates_applied_at,
      repair_dates_run_id = :repair_dates_run_id
    """

    expr_vals = {
        ":start_date": repaired_start,
        ":end_date": repaired_end,
        ":parse_warnings": warnings,
        ":parser_version": REPAIR_PARSER_VERSION,
        ":idempotency_sha256": new_idempotency,
        ":repair_dates_applied_at": now_iso,
        ":repair_dates_run_id": run_id,
    }

    try:
        table.update_item(
            Key={"pk": item["pk"], "sk": item["sk"]},
            UpdateExpression=update_expr,
            ConditionExpression=Attr("start_date").eq(old_start) & Attr("end_date").eq(old_end),
            ExpressionAttributeValues=expr_vals,
        )
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code == "ConditionalCheckFailedException":
            return False
        raise


def main() -> int:
    args = parse_args()
    if not args.ddb_table:
        raise SystemExit("Missing DynamoDB table. Set --ddb-table or PDGA_DDB_TABLE.")

    ddb = boto3.resource("dynamodb", region_name=args.aws_region) if args.aws_region else boto3.resource("dynamodb")
    table = ddb.Table(args.ddb_table)

    run_id = f"repair-dates-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    event_ids = parse_event_ids(args.event_ids)
    now_iso = utc_now_iso()

    scanned = 0
    candidates = 0
    reparable = 0
    applied = 0
    skipped_parse_fail = 0
    skipped_race = 0

    examples: list[dict[str, Any]] = []

    for item in iter_metadata_items(table, event_ids=event_ids, limit=args.limit):
        scanned += 1
        event_id = int(item["event_id"])
        current_start = str(item.get("start_date", "") or "")
        current_end = str(item.get("end_date", "") or "")

        if not current_start or not current_end:
            continue
        if current_start <= current_end:
            continue

        candidates += 1
        repaired = compute_repaired_dates(item)
        if repaired is None:
            skipped_parse_fail += 1
            continue

        reparable += 1
        new_start, new_end = repaired

        if len(examples) < 10:
            examples.append(
                {
                    "event_id": event_id,
                    "raw_date_str": item.get("raw_date_str", ""),
                    "old_start_date": current_start,
                    "old_end_date": current_end,
                    "new_start_date": new_start,
                    "new_end_date": new_end,
                }
            )

        if args.apply:
            ok = apply_repair(table, item, new_start, new_end, now_iso, run_id)
            if ok:
                applied += 1
            else:
                skipped_race += 1

    summary = {
        "run_id": run_id,
        "dry_run": not bool(args.apply),
        "scanned_metadata_items": scanned,
        "candidates_start_gt_end": candidates,
        "reparable": reparable,
        "applied": applied,
        "skipped_parse_fail": skipped_parse_fail,
        "skipped_conditional_race": skipped_race,
        "examples": examples,
    }

    print({"repair_metadata_dates_summary": summary})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())