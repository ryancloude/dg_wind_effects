from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from score_round_wind_model.athena_io import register_scored_round_partition
from score_round_wind_model.config import load_config
from score_round_wind_model.dynamo_io import (
    get_score_checkpoint,
    put_score_checkpoint,
    put_score_run_summary,
)
from score_round_wind_model.gold_io import list_model_input_round_objects, load_event_dataframe
from score_round_wind_model.model_io import load_model_bundle
from score_round_wind_model.parquet_io import (
    build_scored_round_partition_location,
    overwrite_event_scored_rounds,
)
from score_round_wind_model.scoring import compute_scoring_request_fingerprint, score_round_rows

logger = logging.getLogger("score_round_wind_model")


@dataclass
class RunStats:
    attempted_events: int = 0
    processed_events: int = 0
    skipped_unchanged_events: int = 0
    failed_events: int = 0
    rows_scored: int = 0
    partitions_registered: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "attempted_events": self.attempted_events,
            "processed_events": self.processed_events,
            "skipped_unchanged_events": self.skipped_unchanged_events,
            "failed_events": self.failed_events,
            "rows_scored": self.rows_scored,
            "partitions_registered": self.partitions_registered,
        }

    def failure_rate(self) -> float:
        if self.attempted_events <= 0:
            return 0.0
        return self.failed_events / self.attempted_events


def make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"score-round-wind-model-{ts}"


def probability(value: str) -> float:
    parsed = float(value)
    if not 0.0 <= parsed <= 1.0:
        raise argparse.ArgumentTypeError("value must be between 0.0 and 1.0")
    return parsed


def parse_args():
    p = argparse.ArgumentParser(description="Score round model-input rows with the trained CatBoost wind model.")
    p.add_argument("--training-request-fingerprint", required=True, help="Explicit training request fingerprint to score with")
    p.add_argument("--event-ids", help="Optional comma-separated event IDs")
    p.add_argument("--bucket", help="Override S3 bucket")
    p.add_argument("--ddb-table", help="Override DDB table")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force-events", action="store_true")
    p.add_argument(
        "--include-failed-events",
        action="store_true",
        help="Include events with score checkpoints in failed status.",
    )
    p.add_argument(
        "--max-failure-rate",
        type=probability,
        default=0.5,
        help="Exit non-zero only when failed events are at or above this fraction of attempted events.",
    )
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
    logger.info("score_round_wind_model_phase_complete", extra=payload)
    print({"score_round_wind_model_phase_complete": payload})


def _event_id_from_key(key: str) -> int:
    marker = "tourn_id="
    start = key.index(marker) + len(marker)
    end = key.index("/", start)
    return int(key[start:end])


def _should_skip_event(
    *,
    checkpoint: dict | None,
    scoring_request_fingerprint: str,
    force_events: bool,
    include_failed: bool,
) -> tuple[bool, str]:
    if force_events or not checkpoint:
        return False, ""

    status = str(checkpoint.get("status", "")).strip().lower()
    if status == "success":
        checkpoint_fp = str(checkpoint.get("scoring_request_fingerprint", "")).strip()
        if checkpoint_fp == scoring_request_fingerprint:
            return True, "unchanged_success"
        return False, ""

    if status == "failed" and not include_failed:
        return True, "previous_failed"

    return False, ""


def _should_exit_nonzero(*, stats: RunStats, max_failure_rate: float) -> bool:
    return stats.failure_rate() >= max_failure_rate


def main() -> int:
    args = parse_args()
    include_failed_events = bool(getattr(args, "include_failed_events", False))
    max_failure_rate = float(getattr(args, "max_failure_rate", 0.5))

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
        model_bundle = load_model_bundle(
            bucket=bucket,
            training_request_fingerprint=args.training_request_fingerprint,
        )
        _log_phase_timing(
            run_id=run_id,
            phase="load_model_bundle",
            started_at=t0,
            extra={"artifact_prefix": model_bundle["artifact_prefix"]},
        )

        t1 = time.perf_counter()
        event_objects = list_model_input_round_objects(
            bucket=bucket,
            event_ids=event_ids,
        )
        _log_phase_timing(
            run_id=run_id,
            phase="list_model_input_round_objects",
            started_at=t1,
            extra={"event_object_count": len(event_objects)},
        )

        logger.info(
            "score_round_wind_model_run_plan",
            extra={
                "run_id": run_id,
                "training_request_fingerprint": args.training_request_fingerprint,
                "candidate_event_count": len(event_objects),
                "include_failed_events": include_failed_events,
                "dry_run": bool(args.dry_run),
                "force_events": bool(args.force_events),
                "max_failure_rate": max_failure_rate,
            },
        )
        print(
            {
                "score_round_wind_model_run_plan": {
                    "run_id": run_id,
                    "training_request_fingerprint": args.training_request_fingerprint,
                    "candidate_event_count": len(event_objects),
                    "include_failed_events": include_failed_events,
                    "dry_run": bool(args.dry_run),
                    "force_events": bool(args.force_events),
                    "max_failure_rate": max_failure_rate,
                }
            }
        )

        for event_object in event_objects:
            event_id = _event_id_from_key(event_object["key"])

            try:
                scoring_request_fingerprint = compute_scoring_request_fingerprint(
                    event_object=event_object,
                    training_request_fingerprint=args.training_request_fingerprint,
                )

                checkpoint = get_score_checkpoint(
                    table_name=ddb_table,
                    event_id=event_id,
                    training_request_fingerprint=args.training_request_fingerprint,
                    aws_region=cfg.aws_region,
                )

                should_skip, skip_reason = _should_skip_event(
                    checkpoint=checkpoint,
                    scoring_request_fingerprint=scoring_request_fingerprint,
                    force_events=bool(args.force_events),
                    include_failed=include_failed_events,
                )

                if should_skip and skip_reason == "unchanged_success":
                    stats.skipped_unchanged_events += 1
                    logger.info(
                        "score_round_wind_model_event_skipped_unchanged",
                        extra={
                            "run_id": run_id,
                            "event_id": event_id,
                            "training_request_fingerprint": args.training_request_fingerprint,
                        },
                    )
                    continue

                if should_skip and skip_reason == "previous_failed":
                    logger.info(
                        "score_round_wind_model_event_skipped_failed",
                        extra={
                            "run_id": run_id,
                            "event_id": event_id,
                            "training_request_fingerprint": args.training_request_fingerprint,
                        },
                    )
                    continue

                stats.attempted_events += 1

                t_event_load = time.perf_counter()
                df = load_event_dataframe(
                    bucket=bucket,
                    key=event_object["key"],
                )
                _log_phase_timing(
                    run_id=run_id,
                    phase="load_event_dataframe",
                    started_at=t_event_load,
                    extra={"event_id": event_id, "rows": len(df)},
                )

                scored_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

                t_score = time.perf_counter()
                result = score_round_rows(
                    df=df,
                    model=model_bundle["model"],
                    training_manifest=model_bundle["training_manifest"],
                    feature_columns=model_bundle["feature_columns"],
                    categorical_feature_columns=model_bundle["categorical_feature_columns"],
                    training_request_fingerprint=args.training_request_fingerprint,
                    scoring_run_id=run_id,
                    scored_at_utc=scored_at_utc,
                    scoring_request_fingerprint=scoring_request_fingerprint,
                    model_artifact_prefix=model_bundle["artifact_prefix"],
                )
                _log_phase_timing(
                    run_id=run_id,
                    phase="score_round_rows",
                    started_at=t_score,
                    extra={"event_id": event_id, "rows_scored": len(result.scored_df)},
                )

                scored_rows = result.scored_df.to_dict(orient="records")

                scored_key = ""
                partition_location = ""
                athena_partition_result: dict | None = None

                if not args.dry_run:
                    event_year = int(result.scored_df["event_year"].iloc[0])

                    t_write = time.perf_counter()
                    scored_key = overwrite_event_scored_rounds(
                        bucket=bucket,
                        event_year=event_year,
                        event_id=event_id,
                        rows=scored_rows,
                    )
                    _log_phase_timing(
                        run_id=run_id,
                        phase="overwrite_event_scored_rounds",
                        started_at=t_write,
                        extra={"event_id": event_id, "scored_key": scored_key},
                    )

                    partition_location = build_scored_round_partition_location(
                        bucket=bucket,
                        event_year=event_year,
                        event_id=event_id,
                    )

                    t_partition = time.perf_counter()
                    athena_partition_result = register_scored_round_partition(
                        database=cfg.athena_database,
                        table_name=cfg.athena_source_scored_table,
                        workgroup=cfg.athena_workgroup,
                        output_location=cfg.athena_results_s3_uri,
                        aws_region=cfg.aws_region,
                        event_year=event_year,
                        event_id=event_id,
                        partition_location=partition_location,
                    )
                    stats.partitions_registered += 1
                    _log_phase_timing(
                        run_id=run_id,
                        phase="register_scored_round_partition",
                        started_at=t_partition,
                        extra={
                            "event_id": event_id,
                            "event_year": event_year,
                            "partition_location": partition_location,
                            "query_execution_id": athena_partition_result["query_execution_id"],
                        },
                    )

                    put_score_checkpoint(
                        table_name=ddb_table,
                        event_id=event_id,
                        training_request_fingerprint=args.training_request_fingerprint,
                        run_id=run_id,
                        status="success",
                        aws_region=cfg.aws_region,
                        extra_attributes={
                            "event_year": event_year,
                            "rows_scored": int(len(result.scored_df)),
                            "scored_rounds_key": scored_key,
                            "model_name": str(result.scoring_manifest["model_name"]),
                            "model_version": str(result.scoring_manifest["model_version"]),
                            "model_artifact_prefix": model_bundle["artifact_prefix"],
                            "scoring_request_fingerprint": scoring_request_fingerprint,
                            "athena_partition_location": partition_location,
                            "athena_partition_query_execution_id": athena_partition_result["query_execution_id"],
                        },
                    )

                stats.processed_events += 1
                stats.rows_scored += int(len(result.scored_df))

                logger.info(
                    "score_round_wind_model_event_processed",
                    extra={
                        "run_id": run_id,
                        "event_id": event_id,
                        "rows_scored": len(result.scored_df),
                        "scored_rounds_key": scored_key,
                        "partition_location": partition_location,
                    },
                )

            except Exception as exc:
                stats.failed_events += 1
                logger.exception(
                    "score_round_wind_model_event_failed",
                    extra={"run_id": run_id, "event_id": event_id, "error": str(exc)},
                )

                if not args.dry_run:
                    try:
                        put_score_checkpoint(
                            table_name=ddb_table,
                            event_id=event_id,
                            training_request_fingerprint=args.training_request_fingerprint,
                            run_id=run_id,
                            status="failed",
                            aws_region=cfg.aws_region,
                            extra_attributes={"error_message": str(exc)},
                        )
                    except Exception:
                        logger.exception(
                            "score_round_wind_model_checkpoint_write_failed",
                            extra={"run_id": run_id, "event_id": event_id},
                        )

        exit_nonzero = _should_exit_nonzero(stats=stats, max_failure_rate=max_failure_rate)
        summary = {
            "run_id": run_id,
            "training_request_fingerprint": args.training_request_fingerprint,
            **stats.to_dict(),
            "failure_rate": round(stats.failure_rate(), 4),
            "max_failure_rate": max_failure_rate,
            "exit_nonzero": exit_nonzero,
        }
        logger.info("score_round_wind_model_summary", extra=summary)
        print({"score_round_wind_model_summary": summary})

        if not args.dry_run:
            put_score_run_summary(
                table_name=ddb_table,
                run_id=run_id,
                stats=summary,
                aws_region=cfg.aws_region,
            )

        return 2 if exit_nonzero else 0

    except Exception as exc:
        logger.exception("score_round_wind_model_failed", extra={"run_id": run_id, "error": str(exc)})
        print({"score_round_wind_model_summary": {"run_id": run_id, "error_message": str(exc)}})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
