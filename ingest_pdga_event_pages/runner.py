from __future__ import annotations

import argparse
import itertools
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, Iterable

from ingest_pdga_event_pages.config import load_config
from ingest_pdga_event_pages.dynamo_reader import (
    get_existing_content_sha256,
    get_max_event_id,
    iter_rescrape_event_ids,
)
from ingest_pdga_event_pages.dynamo_writer import upsert_event_metadata
from ingest_pdga_event_pages.event_page_parser import parse_event_page
from ingest_pdga_event_pages.http_client import HttpConfig, build_session, get_event_page_html, polite_sleep
from ingest_pdga_event_pages.s3_writer import put_event_page_raw


logger = logging.getLogger("pdga_ingest")

DEFAULT_INCREMENTAL_WINDOW_DAYS = 183


@dataclass(frozen=True)
class ProcessResult:
    """Structured result for one processed event page."""

    event_id: int
    parsed: Dict[str, Any]
    http_status: int
    s3_ptrs: Dict[str, Any]
    ddb_attrs: Dict[str, Any]
    unchanged: bool


def positive_int(value: str) -> int:
    """Argparse validator for positive integer CLI inputs."""
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def parse_args():
    p = argparse.ArgumentParser(description="Ingest PDGA event pages (raw HTML to S3 + parse discovery fields)")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--ids", help="Comma-separated event ids, e.g. 90009,90010")
    group.add_argument("--range", help="Inclusive range like 90000-90050")
    group.add_argument(
        "--backfill-start-id",
        type=positive_int,
        help="Start sequential backfill at this event id and stop after a configurable placeholder streak.",
    )
    group.add_argument(
        "--incremental-statuses",
        help="Comma-separated status_text values to rescrape for recent completed events before scanning new higher IDs.",
    )

    p.add_argument("--bucket", help="Override S3 bucket (otherwise PDGA_S3_BUCKET from env/.env)")
    p.add_argument("--dry-run", action="store_true", help="Fetch + parse but do not write to S3 or DynamoDB")
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("--sleep-base", type=float, default=4.0)
    p.add_argument("--sleep-jitter", type=float, default=2.0)
    p.add_argument("--log-level", default="INFO")
    p.add_argument(
        "--backfill-stop-after-unscheduled",
        type=positive_int,
        default=5,
        help="In backfill or incremental forward-scan mode, stop after this many unscheduled placeholder pages in a row.",
    )
    p.add_argument(
        "--backfill-max-event-id",
        type=positive_int,
        help="Optional hard stop for backfill or incremental forward scan to prevent runaway scans.",
    )
    p.add_argument(
        "--incremental-window-days",
        type=positive_int,
        default=DEFAULT_INCREMENTAL_WINDOW_DAYS,
        help="Only rescrape events whose end_date falls within this many days before today.",
    )
    return p.parse_args()


def iter_explicit_event_ids(args) -> Iterable[int]:
    """Expand explicit CLI inputs into a concrete list of event IDs."""
    if args.ids:
        return [int(x.strip()) for x in args.ids.split(",") if x.strip()]

    start_s, end_s = args.range.split("-", 1)
    start, end = int(start_s), int(end_s)
    if end < start:
        raise ValueError(f"range end must be >= start: {args.range}")
    return list(range(start, end + 1))


def parse_status_list(raw_statuses: str) -> list[str]:
    values = [value.strip() for value in raw_statuses.split(",") if value.strip()]
    if not values:
        raise ValueError("--incremental-statuses requires at least one status_text value")
    return values


def update_unscheduled_streak(current_streak: int, is_unscheduled_placeholder: bool) -> int:
    """
    Maintain the consecutive-placeholder counter used by backfill and incremental forward scan.

    Placeholder pages increment the streak.
    Any scheduled event resets the streak to zero.
    """
    if is_unscheduled_placeholder:
        return current_streak + 1
    return 0


def should_stop_backfill(unscheduled_streak: int, stop_after_unscheduled: int) -> bool:
    """Return True when the configured consecutive-placeholder stop condition is met."""
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
    """
    Fetch, parse, and optionally persist a single PDGA event page.

    Dry-run mode intentionally avoids all DynamoDB and S3 interaction so the
    parser and control flow can be validated without AWS credentials.
    """
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

        # Skip writes when the parsed business payload has not changed.
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
    """Emit a structured log entry plus a compact stdout record for one event."""
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


def run_event_sequence(
    *,
    event_ids: Iterable[int],
    bucket: str,
    dry_run: bool,
    app_cfg,
    session,
    http_cfg: HttpConfig,
) -> tuple[int, int]:
    """
    Process a finite list or iterator of explicit event IDs.
    """
    ok = 0
    failed = 0

    for event_id in event_ids:
        try:
            result = process_event(
                event_id=event_id,
                bucket=bucket,
                dry_run=dry_run,
                app_cfg=app_cfg,
                session=session,
                http_cfg=http_cfg,
            )

            if result.unchanged:
                logger.info("event_unchanged", extra={"event_id": event_id})

            log_event_result(result)
            ok += 1

        except Exception as exc:
            failed += 1
            logger.exception("event_failed", extra={"event_id": event_id, "error": str(exc)})

        polite_sleep(http_cfg)

    return ok, failed


def run_forward_scan(
    *,
    start_event_id: int,
    stop_after_unscheduled: int,
    max_event_id: int | None,
    bucket: str,
    dry_run: bool,
    app_cfg,
    session,
    http_cfg: HttpConfig,
) -> tuple[int, int]:
    """
    Scan sequentially upward from a starting event ID until the placeholder stop rule is met.
    """
    ok = 0
    failed = 0
    unscheduled_streak = 0

    for event_id in itertools.count(start_event_id):
        if max_event_id is not None and event_id > max_event_id:
            logger.info(
                "forward_scan_stopped_at_max_event_id",
                extra={"event_id": event_id, "max_event_id": max_event_id, "ok": ok, "failed": failed},
            )
            break

        try:
            result = process_event(
                event_id=event_id,
                bucket=bucket,
                dry_run=dry_run,
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

            unscheduled_streak = update_unscheduled_streak(
                unscheduled_streak,
                result.parsed["is_unscheduled_placeholder"],
            )

            logger.info(
                "forward_scan_progress",
                extra={
                    "event_id": event_id,
                    "unscheduled_streak": unscheduled_streak,
                    "stop_after_unscheduled": stop_after_unscheduled,
                },
            )

            if should_stop_backfill(unscheduled_streak, stop_after_unscheduled):
                logger.info(
                    "forward_scan_stop_condition_met",
                    extra={
                        "event_id": event_id,
                        "unscheduled_streak": unscheduled_streak,
                        "stop_after_unscheduled": stop_after_unscheduled,
                    },
                )
                break

        except Exception as exc:
            failed += 1

            # Failed fetches/parses are not evidence of an unscheduled placeholder.
            unscheduled_streak = 0
            logger.exception("event_failed", extra={"event_id": event_id, "error": str(exc)})

        polite_sleep(http_cfg)

    return ok, failed


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

    total_ok = 0
    total_failed = 0

    if args.incremental_statuses:
        statuses = parse_status_list(args.incremental_statuses)
        today = date.today()
        window_start = today - timedelta(days=args.incremental_window_days)

        logger.info(
            "incremental_rescrape_started",
            extra={
                "statuses": statuses,
                "window_start": window_start.isoformat(),
                "end_before_date": today.isoformat(),
            },
        )

        candidate_ids = iter_rescrape_event_ids(
            table_name=app_cfg.ddb_table,
            status_texts=statuses,
            start_date=window_start.isoformat(),
            end_before_date=today.isoformat(),
            aws_region=app_cfg.aws_region,
        )

        ok, failed = run_event_sequence(
            event_ids=candidate_ids,
            bucket=bucket,
            dry_run=args.dry_run,
            app_cfg=app_cfg,
            session=session,
            http_cfg=http_cfg,
        )
        total_ok += ok
        total_failed += failed

        max_known_event_id = get_max_event_id(
            table_name=app_cfg.ddb_table,
            aws_region=app_cfg.aws_region,
        )
        if max_known_event_id is None:
            raise RuntimeError("Could not determine max known event_id for incremental forward scan")

        logger.info(
            "incremental_forward_scan_started",
            extra={"start_event_id": max_known_event_id + 1},
        )

        ok, failed = run_forward_scan(
            start_event_id=max_known_event_id + 1,
            stop_after_unscheduled=args.backfill_stop_after_unscheduled,
            max_event_id=args.backfill_max_event_id,
            bucket=bucket,
            dry_run=args.dry_run,
            app_cfg=app_cfg,
            session=session,
            http_cfg=http_cfg,
        )
        total_ok += ok
        total_failed += failed

    elif args.backfill_start_id is not None:
        ok, failed = run_forward_scan(
            start_event_id=args.backfill_start_id,
            stop_after_unscheduled=args.backfill_stop_after_unscheduled,
            max_event_id=args.backfill_max_event_id,
            bucket=bucket,
            dry_run=args.dry_run,
            app_cfg=app_cfg,
            session=session,
            http_cfg=http_cfg,
        )
        total_ok += ok
        total_failed += failed

    else:
        ok, failed = run_event_sequence(
            event_ids=iter_explicit_event_ids(args),
            bucket=bucket,
            dry_run=args.dry_run,
            app_cfg=app_cfg,
            session=session,
            http_cfg=http_cfg,
        )
        total_ok += ok
        total_failed += failed

    logger.info("summary", extra={"ok": total_ok, "failed": total_failed})
    return 0 if total_failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())