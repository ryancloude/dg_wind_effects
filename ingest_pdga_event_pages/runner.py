from __future__ import annotations

import argparse
import logging

from ingest_pdga_event_pages.config import load_config
from ingest_pdga_event_pages.http_client import HttpConfig, build_session, get_event_page_html, polite_sleep
from ingest_pdga_event_pages.event_page_parser import parse_event_page
from ingest_pdga_event_pages.s3_writer import put_event_page_raw
from ingest_pdga_event_pages.dynamo_writer import upsert_event_metadata
from ingest_pdga_event_pages.dynamo_reader import get_existing_content_sha256


logger = logging.getLogger("pdga_ingest")


def parse_args():
    p = argparse.ArgumentParser(description="Ingest PDGA event pages (raw HTML to S3 + parse discovery fields)")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--ids", help="Comma-separated event ids, e.g. 90009,90010")
    group.add_argument("--range", help="Inclusive range like 90000-90050")

    # Optional overrides (env/.env is the default source of truth)
    p.add_argument("--bucket", help="Override S3 bucket (otherwise PDGA_S3_BUCKET from env/.env)")
    p.add_argument("--dry-run", action="store_true", help="Fetch + parse but do not write to S3")
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("--sleep-base", type=float, default=4.0)
    p.add_argument("--sleep-jitter", type=float, default=2.0)
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def iter_event_ids(args):
    ddb_attrs = {}
    if args.ids:
        return [int(x.strip()) for x in args.ids.split(",") if x.strip()]
    start_s, end_s = args.range.split("-", 1)
    start, end = int(start_s), int(end_s)
    return list(range(start, end + 1))


def main() -> int:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    # App-level config (S3 bucket, Dynamo table later, region, etc.)
    app_cfg = load_config()

    # Bucket resolution: CLI overrides env
    bucket = args.bucket or app_cfg.s3_bucket

    # HTTP config
    http_cfg = HttpConfig(
        timeout_s=args.timeout,
        base_sleep_s=args.sleep_base,
        jitter_s=args.sleep_jitter,
    )
    session = build_session(http_cfg)

    ok = 0
    failed = 0

    for event_id in iter_event_ids(args):
        url = f"https://www.pdga.com/tour/event/{event_id}"
        try:
            status_code, html = get_event_page_html(session, http_cfg, event_id)

            parsed = parse_event_page(event_id=event_id, html=html, source_url=url)

            existing_hash = get_existing_content_sha256(
            table_name=app_cfg.ddb_table,
            event_id=event_id,
            aws_region=app_cfg.aws_region,
            )

            if existing_hash and existing_hash == parsed["idempotency_sha256"]:
                logger.info("event_unchanged", extra={"event_id": event_id})
                polite_sleep(http_cfg)
                continue

            s3_ptrs = None
            if not args.dry_run:
                s3_ptrs = put_event_page_raw(
                    bucket=bucket,
                    event_id=event_id,
                    source_url=url,
                    html=html,
                    http_status=status_code,
                    content_sha256=parsed["content_sha256"],
                    parser_version=parsed["parser_version"],
                )

                ddb_attrs = upsert_event_metadata(
                table_name=app_cfg.ddb_table,
                parsed=parsed,
                s3_ptrs=s3_ptrs,
                aws_region=app_cfg.aws_region,
            )

            logger.info(
                "event_ok",
                extra={
                    "event_id": event_id,
                    "divisions": len(parsed["division_rounds"]),
                    "s3_html_key": (s3_ptrs or {}).get("s3_html_key"),
                    "ddb_pk": ddb_attrs.get("pk")
                },
            )

            print(
                {
                    "event_id": event_id,
                    "name": parsed["name"],
                    "division_rounds": parsed["division_rounds"],
                    "status_text": parsed["status_text"],
                    "s3_html_key": (s3_ptrs or {}).get("s3_html_key"),
                    "ddb_pk": ddb_attrs.get("pk")
                }
            )

            ok += 1

        except Exception as e:
            failed += 1
            logger.exception("event_failed", extra={"event_id": event_id, "error": str(e)})

        polite_sleep(http_cfg)

    logger.info("summary", extra={"ok": ok, "failed": failed})
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())