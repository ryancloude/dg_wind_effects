from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable

from ingest_pdga_event_pages.config import load_config
from ingest_pdga_event_pages.http_client import HttpConfig, build_session, polite_sleep
from ingest_pdga_live_results.dynamo_reader import LiveResultsTask, load_live_results_tasks
from ingest_pdga_live_results.dynamo_writer import (
    get_existing_live_results_sha256,
    put_live_results_run_summary,
    upsert_live_results_state,
)
from ingest_pdga_live_results.http_client import get_live_results_json
from ingest_pdga_live_results.response_handler import classify_response, compute_payload_sha256
from ingest_pdga_live_results.s3_writer import put_live_results_raw

logger = logging.getLogger("pdga_live_results_ingest")


@dataclass(frozen=True)
class ProcessResult:
    task: LiveResultsTask
    source_url: str
    status_code: int | None
    classification: str
    content_sha256: str | None
    changed: bool
    unchanged: bool
    s3_ptrs: Dict[str, Any]


@dataclass
class RunStats:
    attempted: int = 0
    success: int = 0
    not_found_404: int = 0
    empty: int = 0
    changed: int = 0
    unchanged: int = 0
    failed: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "attempted": self.attempted,
            "success": self.success,
            "not_found_404": self.not_found_404,
            "empty": self.empty,
            "changed": self.changed,
            "unchanged": self.unchanged,
            "failed": self.failed,
        }


def parse_args():
    p = argparse.ArgumentParser(description="Ingest PDGA live results API to Bronze S3 + state in DynamoDB")
    p.add_argument("--event-ids", help="Optional comma-separated event IDs, e.g. 86076,86077")
    p.add_argument("--bucket")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("--sleep-base", type=float, default=4.0)
    p.add_argument("--sleep-jitter", type=float, default=2.0)
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def parse_event_ids(raw: str | None) -> list[int] | None:
    if not raw:
        return None
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"live-results-{ts}"


def process_task(
    *,
    task: LiveResultsTask,
    bucket: str,
    dry_run: bool,
    app_cfg,
    session,
    http_cfg: HttpConfig,
    run_id: str,
) -> ProcessResult:
    source_url = ""
    status_code: int | None = None
    payload = None
    error: Exception | None = None

    try:
        status_code, payload, source_url = get_live_results_json(session, http_cfg, task)
    except Exception as exc:
        error = exc

    classification = classify_response(status_code=status_code, payload=payload, error=error)
    content_sha256: str | None = None
    changed = False
    unchanged = False
    s3_ptrs: Dict[str, Any] = {}

    if classification in ("success", "empty"):
        content_sha256 = compute_payload_sha256(payload if payload is not None else {})
        if not dry_run:
            existing_hash = get_existing_live_results_sha256(
                table_name=app_cfg.ddb_table,
                event_id=int(task.event_id),
                division=task.division,
                round_number=task.round_number,
                aws_region=app_cfg.aws_region,
            )
            unchanged = bool(existing_hash and existing_hash == content_sha256)
            changed = not unchanged

            if changed:
                s3_ptrs = put_live_results_raw(
                    bucket=bucket,
                    task=task,
                    source_url=source_url,
                    payload=payload if payload is not None else {},
                    http_status=int(status_code or 200),
                    content_sha256=content_sha256,
                    run_id=run_id,
                )

            upsert_live_results_state(
                table_name=app_cfg.ddb_table,
                event_id=int(task.event_id),
                division=task.division,
                round_number=task.round_number,
                source_url=source_url,
                status_text=classification,
                content_sha256=content_sha256,
                s3_ptrs=s3_ptrs,
                run_id=run_id,
                aws_region=app_cfg.aws_region,
            )

    if error is not None:
        raise error

    return ProcessResult(
        task=task,
        source_url=source_url,
        status_code=status_code,
        classification=classification,
        content_sha256=content_sha256,
        changed=changed,
        unchanged=unchanged,
        s3_ptrs=s3_ptrs,
    )


def run_task_sequence(
    *,
    tasks: Iterable[LiveResultsTask],
    bucket: str,
    dry_run: bool,
    app_cfg,
    session,
    http_cfg: HttpConfig,
    run_id: str,
) -> RunStats:
    stats = RunStats()

    for task in tasks:
        stats.attempted += 1

        try:
            result = process_task(
                task=task,
                bucket=bucket,
                dry_run=dry_run,
                app_cfg=app_cfg,
                session=session,
                http_cfg=http_cfg,
                run_id=run_id,
            )

            if result.classification == "success":
                stats.success += 1
            elif result.classification == "empty":
                stats.empty += 1
            elif result.classification == "not_found":
                stats.not_found_404 += 1

            if result.changed:
                stats.changed += 1
            if result.unchanged:
                stats.unchanged += 1

            logger.info(
                "live_results_task_ok",
                extra={
                    "event_id": task.event_id,
                    "division": task.division,
                    "round_number": task.round_number,
                    "classification": result.classification,
                    "changed": result.changed,
                    "unchanged": result.unchanged,
                    "s3_json_key": result.s3_ptrs.get("s3_json_key"),
                },
            )

        except Exception as exc:
            stats.failed += 1
            logger.exception(
                "live_results_task_failed",
                extra={
                    "event_id": task.event_id,
                    "division": task.division,
                    "round_number": task.round_number,
                    "error": str(exc),
                },
            )

        polite_sleep(http_cfg)

    return stats


def main() -> int:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    app_cfg = load_config()
    bucket = args.bucket or app_cfg.s3_bucket
    http_cfg = HttpConfig(timeout_s=args.timeout, base_sleep_s=args.sleep_base, jitter_s=args.sleep_jitter)
    session = build_session(http_cfg)

    event_ids = parse_event_ids(args.event_ids)
    tasks = load_live_results_tasks(
        table_name=app_cfg.ddb_table,
        event_ids=event_ids,
        aws_region=app_cfg.aws_region,
    )

    run_id = make_run_id()
    stats = run_task_sequence(
        tasks=tasks,
        bucket=bucket,
        dry_run=args.dry_run,
        app_cfg=app_cfg,
        session=session,
        http_cfg=http_cfg,
        run_id=run_id,
    )

    if not args.dry_run:
        put_live_results_run_summary(
            table_name=app_cfg.ddb_table,
            run_id=run_id,
            stats=stats.to_dict(),
            aws_region=app_cfg.aws_region,
        )

    logger.info("live_results_summary", extra={"run_id": run_id, **stats.to_dict()})
    print({"run_id": run_id, "summary": stats.to_dict()})
    return 0 if stats.failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())