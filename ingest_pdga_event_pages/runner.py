from __future__ import annotations

import argparse
import itertools
import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable

from ingest_pdga_event_pages.config import load_config
from ingest_pdga_event_pages.dynamo_reader import get_existing_content_sha256
from ingest_pdga_event_pages.dynamo_writer import upsert_event_metadata
from ingest_pdga_event_pages.event_page_parser import parse_event_page
from ingest_pdga_event_pages.http_client import HttpConfig, build_session, get_event_page_html, polite_sleep
from ingest_pdga_event_pages.s3_writer import put_event_page_raw


logger = logging.getLogger("pdga_ingest")


@dataclass(frozen=True)
class ProcessResult:
    event_id: int
    parsed: Dict[str, Any]
    http_status: int
    s3_ptrs: Dict[str, Any]
    ddb_attrs: Dict[str, Any]
    unchanged: bool


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingest PDGA event pages (raw HTML to S3 + parse discovery fields)"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--ids", help="Comma-separated event ids, e.g. 90009,90010")
    group.add_argument("--range", help="Inclusive range like 90000-90050")
    group.add_argument(
        "--backfill-start-id",
        type=positive_int,
        help="Start sequential backfill at this event id and stop after a configurable placeholder streak.",
    )

    parser.add_argument("--bucket", help="Override S3 bucket (otherwise PDGA_S3_BUCKET from env/.env)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch + parse but do not write to S3/DynamoDB")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--sleep-base", type=float, default=4.0)
    parser.add_argument("--sleep-jitter", type=float, default=2.0)
    parser.add_argument("--log-level", default="INFO")

    parser.add_argument(
        "--backfill-stop-after-unscheduled",
        type=positive_int,
        default=5,
        help="In backfill mode, stop after this many unscheduled placeholder pages in a row.",
    )
    parser.add_argument(
        "--backfill-max-event-id",
        type=positive_int,
        help="Optional hard stop for backfill mode to prevent runaway scans if the placeholder heuristic fails.",
    )
    return parser.parse_args()


def iter_explicit_event_ids(args) -> Iterable[int]:
    if args.ids:
        return [int(value.strip()) for value in args.ids.split(",") if value.strip()]

    start_s, end_s = args.range.split("-", 1)
    start, end = int(start_s), int(end_s)
    if end < start:
        raise ValueError(f"range end must be >= start: {args.range}")
    return list(range(start, end + 1))


def update_unscheduled_streak(current_streak: int, is_unscheduled_placeholder: bool) -> int:
    if is_unscheduled_placeholder:
        return current_streak + 1
    return 0


def should_stop_backfill(unscheduled_streak: int, stop_after_unscheduled: int) -> bool:
    return unscheduled_streak >= stop_after_unscheduled


def process_event(
    *,
    event_id: int,
    bucket: str,
    dry_run: bool,
    app_cfg,
    session,
    http_cfg: HttpConfig,
) -> ProcessResult:
    url = f"https://www.pdga.com/tour/event/{event_id}"

    status_code, html = get_event_page_html(session, http_cfg, event_id)
    parsed = parse_event_page(event_id=event_id, html=html, source_url=url)

    unchanged = False
    s3_ptrs: Dict[str, Any] = {}
    ddb_attrs: Dict[str, Any] = {}

    if not dry_run:
        existing_hash = get_existing_content_sha256(
            table_name=app_cfg.ddb_table,
            event_id=event_id,
            aws_region=app_cfg.aws_region,
        )
        unchanged = bool(existing_hash and existing_hash == parsed["idempotency_sha256"])

        if not unchanged:
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

    return ProcessResult(
        event_id=event_id,
        parsed=parsed,
        http_status=status_code,
        s3_ptrs=s3_ptrs,
        ddb_attrs=ddb_attrs,
        unchanged=unchanged,
    )


def log_event_result(result: ProcessResult) -> None:
    parsed = result.parsed

    logger.info(
        "event_ok",
        extra={
            "event_id": result.event_id,
            "unchanged": result.unchanged,
            "is_unscheduled_placeholder": parsed["is_unscheduled_placeholder"],
            "divisions": len(parsed["division_rounds"]),
            "status_text": parsed["status_text"],
            "s3_html_key": result.s3_ptrs.get("s3_html_key"),
            "ddb_pk": result.ddb_attrs.get("pk"),
        },
    )

    print(
        {
            "event_id": result.event_id,
            "name": parsed["name"],
            "status_text": parsed["status_text"],
            "division_rounds": parsed["division_rounds"],
            "is_unscheduled_placeholder": parsed["is_unscheduled_placeholder"],
            "unchanged": result.unchanged,
            "s3_html_key": result.s3_ptrs.get("s3_html_key"),
            "ddb_pk": result.ddb_attrs.get("pk"),
        }
    )


def main() -> int:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    app_cfg = load_config()
    bucket = args.bucket or app_cfg.s3_bucket

    http_cfg = HttpConfig(
        timeout_s=args.timeout,
        base_sleep_s=args.sleep_base,
        jitter_s=args.sleep_jitter,
    )
    session = build_session(http_cfg)

    ok = 0
    failed = 0
    unscheduled_streak = 0

    if args.backfill_start_id is not None:
        event_iter: Iterable[int] = itertools.count(args.backfill_start_id)
    else:
        event_iter = iter_explicit_event_ids(args)

    for event_id in event_iter:
        if args.backfill_max_event_id is not None and event_id > args.backfill_max_event_id:
            logger.info(
                "backfill_stopped_at_max_event_id",
                extra={
                    "event_id": event_id,
                    "backfill_max_event_id": args.backfill_max_event_id,
                    "ok": ok,
                    "failed": failed,
                },
            )
            break

        try:
            result = process_event(
                event_id=event_id,
                bucket=bucket,
                dry_run=args.dry_run,
                app_cfg=app_cfg,
                session=session,
                http_cfg=http_cfg,
            )

            if result.unchanged:
                logger.info(
                    "event_unchanged",
                    extra={
                        "event_id": event_id,
                        "is_unscheduled_placeholder": result.parsed["is_unscheduled_placeholder"],
                    },
                )

            log_event_result(result)
            ok += 1

            if args.backfill_start_id is not None:
                unscheduled_streak = update_unscheduled_streak(
                    unscheduled_streak,
                    result.parsed["is_unscheduled_placeholder"],
                )

                logger.info(
                    "backfill_progress",
                    extra={
                        "event_id": event_id,
                        "unscheduled_streak": unscheduled_streak,
                        "stop_after_unscheduled": args.backfill_stop_after_unscheduled,
                    },
                )

                if should_stop_backfill(
                    unscheduled_streak,
                    args.backfill_stop_after_unscheduled,
                ):
                    logger.info(
                        "backfill_stop_condition_met",
                        extra={
                            "event_id": event_id,
                            "unscheduled_streak": unscheduled_streak,
                            "stop_after_unscheduled": args.backfill_stop_after_unscheduled,
                        },
                    )
                    break

        except Exception as exc:
            failed += 1
            unscheduled_streak = 0
            logger.exception("event_failed", extra={"event_id": event_id, "error": str(exc)})

        polite_sleep(http_cfg)

    logger.info("summary", extra={"ok": ok, "failed": failed})
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())