from __future__ import annotations

import argparse
import gzip
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional

import boto3

from ingest_pdga_event_pages.config import load_config
from ingest_pdga_event_pages.event_page_parser import parse_event_page


logger = logging.getLogger("pdga_location_backfill")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="One-time backfill: extract tournament location from raw HTML in S3 and update DynamoDB metadata."
    )
    p.add_argument("--bucket", help="Override S3 bucket (default from config)")
    p.add_argument("--table", help="Override DynamoDB table (default from config)")
    p.add_argument("--aws-region", help="Override AWS region (default from config/env)")
    p.add_argument("--limit", type=int, help="Max metadata items to process")
    p.add_argument("--dry-run", action="store_true", help="Parse and compare only; do not write to DynamoDB")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def iter_event_metadata_items(table) -> Iterator[Dict[str, Any]]:
    last_evaluated_key = None
    while True:
        kwargs = {
            "ProjectionExpression": "pk, sk, event_id, latest_s3_html_key, location_raw, city, #state, country",
            "ExpressionAttributeNames": {"#state": "state"},
        }
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key

        resp = table.scan(**kwargs)
        for item in resp.get("Items", []):
            if item.get("sk") == "METADATA":
                yield item

        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break


def parse_event_id_from_item(item: Dict[str, Any]) -> Optional[int]:
    event_id = item.get("event_id")
    if event_id is not None:
        return int(event_id)

    pk = item.get("pk", "")
    if isinstance(pk, str) and pk.startswith("EVENT#"):
        try:
            return int(pk.split("#", 1)[1])
        except ValueError:
            return None
    return None


def read_html_from_s3(s3_client, *, bucket: str, key: str) -> str:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    payload = obj["Body"].read()

    content_encoding = (obj.get("ContentEncoding") or "").lower()
    if content_encoding == "gzip" or key.endswith(".gz"):
        payload = gzip.decompress(payload)

    return payload.decode("utf-8", errors="replace")


def update_location_fields(
    table,
    *,
    event_id: int,
    location_raw: str,
    city: str,
    state: str,
    country: str,
) -> None:
    table.update_item(
        Key={"pk": f"EVENT#{event_id}", "sk": "METADATA"},
        UpdateExpression="""
            SET
                location_raw = :location_raw,
                city = :city,
                #state = :state,
                country = :country,
                location_backfilled_at = :location_backfilled_at
        """,
        ExpressionAttributeNames={"#state": "state"},
        ExpressionAttributeValues={
            ":location_raw": location_raw,
            ":city": city,
            ":state": state,
            ":country": country,
            ":location_backfilled_at": utc_now_iso(),
        },
    )


def main() -> int:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    cfg = load_config()
    bucket = args.bucket or cfg.s3_bucket
    table_name = args.table or cfg.ddb_table
    region = args.aws_region or cfg.aws_region

    ddb = boto3.resource("dynamodb", region_name=region) if region else boto3.resource("dynamodb")
    s3 = boto3.client("s3", region_name=region) if region else boto3.client("s3")
    table = ddb.Table(table_name)

    counts = {
        "scanned": 0,
        "missing_html_key": 0,
        "location_missing_in_html": 0,
        "unchanged": 0,
        "updated": 0,
        "would_update": 0,
        "errors": 0,
    }

    for item in iter_event_metadata_items(table):
        if args.limit is not None and counts["scanned"] >= args.limit:
            break

        counts["scanned"] += 1

        event_id = parse_event_id_from_item(item)
        if event_id is None:
            counts["errors"] += 1
            logger.error("missing_or_invalid_event_id", extra={"pk": item.get("pk")})
            continue

        html_key = item.get("latest_s3_html_key", "")
        if not html_key:
            counts["missing_html_key"] += 1
            continue

        try:
            html = read_html_from_s3(s3, bucket=bucket, key=html_key)
            parsed = parse_event_page(event_id=event_id, html=html, source_url=item.get("source_url"))

            new_location_raw = parsed.get("location_raw", "")
            new_city = parsed.get("city", "")
            new_state = parsed.get("state", "")
            new_country = parsed.get("country", "")

            if not new_location_raw:
                counts["location_missing_in_html"] += 1
                continue

            old_tuple = (
                item.get("location_raw", ""),
                item.get("city", ""),
                item.get("state", ""),
                item.get("country", ""),
            )
            new_tuple = (new_location_raw, new_city, new_state, new_country)

            if old_tuple == new_tuple:
                counts["unchanged"] += 1
                continue

            if args.dry_run:
                counts["would_update"] += 1
                logger.info(
                    "location_backfill_would_update",
                    extra={"event_id": event_id, "old": old_tuple, "new": new_tuple},
                )
                continue

            update_location_fields(
                table,
                event_id=event_id,
                location_raw=new_location_raw,
                city=new_city,
                state=new_state,
                country=new_country,
            )
            counts["updated"] += 1
            logger.info("location_backfill_updated", extra={"event_id": event_id})

        except Exception as exc:
            counts["errors"] += 1
            logger.exception(
                "location_backfill_failed",
                extra={"event_id": event_id, "html_key": html_key, "error": str(exc)},
            )

    logger.info("location_backfill_summary", extra=counts)
    print({"location_backfill_summary": counts})
    return 0 if counts["errors"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())