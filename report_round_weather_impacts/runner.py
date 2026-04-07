from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from report_round_weather_impacts.aggregations import build_event_contributions
from report_round_weather_impacts.config import load_config
from report_round_weather_impacts.dimensions import prepare_reporting_dataframe
from report_round_weather_impacts.dynamo_io import (
    get_report_checkpoint,
    put_report_checkpoint,
    put_report_run_summary,
)
from report_round_weather_impacts.models import REPORT_POLICY_VERSION, REPORT_TABLES
from report_round_weather_impacts.parquet_io import write_intermediate_table, write_published_table
from report_round_weather_impacts.publish import build_published_table
from report_round_weather_impacts.scored_io import list_scored_event_objects, load_scored_event_dataframe

logger = logging.getLogger("report_round_weather_impacts")


@dataclass
class RunStats:
    attempted_events: int = 0
    processed_events: int = 0
    skipped_unchanged_events: int = 0
    failed_events: int = 0
    published_tables: int = 0
    rows_input: int = 0
    rows_retained: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "attempted_events": self.attempted_events,
            "processed_events": self.processed_events,
            "skipped_unchanged_events": self.skipped_unchanged_events,
            "failed_events": self.failed_events,
            "published_tables": self.published_tables,
            "rows_input": self.rows_input,
            "rows_retained": self.rows_retained,
        }


def make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"report-round-weather-impacts-{ts}"


def parse_args():
    p = argparse.ArgumentParser(description="Build dashboard reporting tables from scored round outputs.")
    p.add_argument("--event-ids", help="Optional comma-separated event IDs")
    p.add_argument("--bucket", help="Override S3 bucket")
    p.add_argument("--ddb-table", help="Override DynamoDB table")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force-events", action="store_true")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def parse_event_ids(raw: str | None) -> list[int] | None:
    if not raw:
        return None
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def _log_phase_timing(*, run_id: str, phase: str, started_at: float, extra: dict | None = None) -> None:
    elapsed_s = round(time.perf_counter() - started_at, 3)
    payload = {"run_id": run_id, "phase": phase, "elapsed_s": elapsed_s}
    if extra:
        payload.update(extra)
    logger.info("report_round_weather_impacts_phase_complete", extra=payload)
    print({"report_round_weather_impacts_phase_complete": payload})


def _event_id_from_key(key: str) -> int:
    marker = "tourn_id="
    start = key.index(marker) + len(marker)
    end = key.index("/", start)
    return int(key[start:end])


def _fingerprint_event_object(event_object: dict[str, Any]) -> str:
    payload = {
        "key": str(event_object.get("key", "")),
        "etag": str(event_object.get("etag", "")),
        "size": int(event_object.get("size", 0) or 0),
        "last_modified": str(event_object.get("last_modified", "")),
    }
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def main() -> int:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    cfg = load_config()
    bucket = args.bucket or cfg.s3_bucket
    ddb_table = args.ddb_table or cfg.ddb_table
    event_ids = parse_event_ids(args.event_ids)
    run_id = make_run_id()
    stats = RunStats()

    try:
        t0 = time.perf_counter()
        event_objects = list_scored_event_objects(bucket=bucket, event_ids=event_ids)
        _log_phase_timing(
            run_id=run_id,
            phase="list_scored_event_objects",
            started_at=t0,
            extra={"event_object_count": len(event_objects)},
        )

        for event_object in event_objects:
            stats.attempted_events += 1
            event_id = _event_id_from_key(event_object["key"])
            scored_input_fingerprint = _fingerprint_event_object(event_object)

            try:
                checkpoint = get_report_checkpoint(
                    table_name=ddb_table,
                    event_id=event_id,
                    report_policy_version=REPORT_POLICY_VERSION,
                    aws_region=cfg.aws_region,
                )

                if (
                    not args.force_events
                    and checkpoint
                    and str(checkpoint.get("status", "")).strip().lower() == "success"
                    and str(checkpoint.get("scored_input_fingerprint", "")) == scored_input_fingerprint
                ):
                    stats.skipped_unchanged_events += 1
                    continue

                t_load = time.perf_counter()
                raw_df = load_scored_event_dataframe(bucket=bucket, key=event_object["key"])
                stats.rows_input += int(len(raw_df))
                prepared_df = prepare_reporting_dataframe(raw_df)
                stats.rows_retained += int(len(prepared_df))
                _log_phase_timing(
                    run_id=run_id,
                    phase="prepare_reporting_dataframe",
                    started_at=t_load,
                    extra={"event_id": event_id, "rows_input": len(raw_df), "rows_retained": len(prepared_df)},
                )

                event_year = int(prepared_df["event_year"].iloc[0])
                contributions = build_event_contributions(prepared_df)

                if not args.dry_run:
                    t_write = time.perf_counter()
                    for table_name, table_df in contributions.items():
                        write_intermediate_table(
                            bucket=bucket,
                            table_name=table_name,
                            event_year=event_year,
                            event_id=event_id,
                            df=table_df,
                        )
                    _log_phase_timing(
                        run_id=run_id,
                        phase="write_intermediate_tables",
                        started_at=t_write,
                        extra={"event_id": event_id, "table_count": len(contributions)},
                    )

                    put_report_checkpoint(
                        table_name=ddb_table,
                        event_id=event_id,
                        report_policy_version=REPORT_POLICY_VERSION,
                        run_id=run_id,
                        status="success",
                        aws_region=cfg.aws_region,
                        extra_attributes={
                            "event_year": event_year,
                            "source_scored_key": event_object["key"],
                            "scored_input_fingerprint": scored_input_fingerprint,
                            "rows_input": int(len(raw_df)),
                            "rows_retained": int(len(prepared_df)),
                        },
                    )

                stats.processed_events += 1

            except Exception as exc:
                stats.failed_events += 1
                logger.exception(
                    "report_round_weather_impacts_event_failed",
                    extra={"run_id": run_id, "event_id": event_id, "error": str(exc)},
                )
                if not args.dry_run:
                    try:
                        put_report_checkpoint(
                            table_name=ddb_table,
                            event_id=event_id,
                            report_policy_version=REPORT_POLICY_VERSION,
                            run_id=run_id,
                            status="failed",
                            aws_region=cfg.aws_region,
                            extra_attributes={
                                "source_scored_key": event_object["key"],
                                "scored_input_fingerprint": scored_input_fingerprint,
                                "error_message": str(exc),
                            },
                        )
                    except Exception:
                        logger.exception(
                            "report_round_weather_impacts_checkpoint_write_failed",
                            extra={"run_id": run_id, "event_id": event_id},
                        )

        if not args.dry_run:
            for table_name in REPORT_TABLES:
                t_pub = time.perf_counter()
                published_df = build_published_table(bucket=bucket, table_name=table_name)
                if not published_df.empty:
                    write_published_table(bucket=bucket, table_name=table_name, df=published_df)
                stats.published_tables += 1
                _log_phase_timing(
                    run_id=run_id,
                    phase="publish_table",
                    started_at=t_pub,
                    extra={"table_name": table_name, "rows": int(len(published_df))},
                )

            put_report_run_summary(
                table_name=ddb_table,
                run_id=run_id,
                stats=stats.to_dict(),
                aws_region=cfg.aws_region,
            )

        summary = {"run_id": run_id, **stats.to_dict()}
        logger.info("report_round_weather_impacts_summary", extra=summary)
        print({"report_round_weather_impacts_summary": summary})
        return 0 if stats.failed_events == 0 else 2

    except Exception as exc:
        logger.exception("report_round_weather_impacts_failed", extra={"run_id": run_id, "error": str(exc)})
        print({"report_round_weather_impacts_summary": {"run_id": run_id, "error_message": str(exc)}})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
