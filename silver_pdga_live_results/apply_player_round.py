from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence

from silver_pdga_live_results.bronze_reader import load_payload_for_pointer
from silver_pdga_live_results.candidate_reader import LiveResultsStatePointer
from silver_pdga_live_results.load_state import (
    build_round_unit_key,
    get_round_unit_state,
    put_global_checkpoint,
    put_round_unit_state,
    put_run_summary,
)
from silver_pdga_live_results.player_round_transform import transform_player_round_rows
from silver_pdga_live_results.player_round_writer import put_player_round_current

logger = logging.getLogger("silver_live_results_apply")


def make_apply_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"silver-live-results-apply-{ts}"


@dataclass
class ApplyStats:
    selected_units: int = 0
    attempted_units: int = 0
    applied_units: int = 0
    skipped_unchanged_units: int = 0
    skipped_stale_state_units: int = 0
    failed_units: int = 0
    rows_written: int = 0
    checkpoint_advanced: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "selected_units": self.selected_units,
            "attempted_units": self.attempted_units,
            "applied_units": self.applied_units,
            "skipped_unchanged_units": self.skipped_unchanged_units,
            "skipped_stale_state_units": self.skipped_stale_state_units,
            "failed_units": self.failed_units,
            "rows_written": self.rows_written,
            "checkpoint_advanced": self.checkpoint_advanced,
        }


def _select_apply_pointers(
    *,
    pointers: Sequence[LiveResultsStatePointer],
    max_units: int | None = None,
) -> list[LiveResultsStatePointer]:
    if max_units is not None and max_units <= 0:
        raise ValueError("max_units must be positive when provided")

    selected = list(pointers if max_units is None else pointers[:max_units])
    selected.sort(
        key=lambda p: (
            p.last_fetched_at,
            p.latest_s3_json_key,
            p.event_id,
            p.division,
            p.round_number,
        )
    )
    return selected


def apply_player_round_units(
    *,
    table_name: str,
    pipeline_name: str,
    bucket: str,
    silver_prefix: str,
    pointers: Sequence[LiveResultsStatePointer],
    run_id: str | None = None,
    max_units: int | None = None,
    aws_region: str | None = None,
) -> dict[str, Any]:
    apply_run_id = run_id or make_apply_run_id()
    selected = _select_apply_pointers(pointers=pointers, max_units=max_units)

    stats = ApplyStats(selected_units=len(selected))
    unit_results: list[dict[str, Any]] = []

    for pointer in selected:
        stats.attempted_units += 1
        unit_key = build_round_unit_key(pointer.event_id, pointer.division, pointer.round_number)

        try:
            existing_state = get_round_unit_state(
                table_name=table_name,
                pipeline_name=pipeline_name,
                unit_key=unit_key,
                aws_region=aws_region,
            )

            if existing_state and existing_state.last_applied_sha256 == pointer.content_sha256:
                stats.skipped_unchanged_units += 1
                unit_results.append(
                    {
                        "event_id": pointer.event_id,
                        "division": pointer.division,
                        "round_number": pointer.round_number,
                        "unit_key": unit_key,
                        "status": "skipped_unchanged",
                        "source_fetch_ts": pointer.last_fetched_at,
                        "source_s3_json_key": pointer.latest_s3_json_key,
                    }
                )
                continue

            bronze_payload = load_payload_for_pointer(
                bucket=bucket,
                pointer=pointer,
                aws_region=aws_region,
            )

            rows, transform_stats = transform_player_round_rows(
                pointer=pointer,
                payload=bronze_payload.payload,
                run_id=apply_run_id,
            )

            write_ptrs = put_player_round_current(
                bucket=bucket,
                silver_prefix=silver_prefix,
                pointer=pointer,
                rows=rows,
                run_id=apply_run_id,
            )
            stats.rows_written += len(rows)

            state_updated = put_round_unit_state(
                table_name=table_name,
                pipeline_name=pipeline_name,
                unit_key=unit_key,
                last_applied_sha256=pointer.content_sha256,
                last_applied_fetch_ts=pointer.last_fetched_at,
                last_applied_s3_key=write_ptrs["s3_rows_key"],
                last_applied_row_count=len(rows),
                run_id=apply_run_id,
                aws_region=aws_region,
            )

            if state_updated:
                stats.applied_units += 1
                status = "applied"
            else:
                stats.skipped_stale_state_units += 1
                status = "skipped_stale_state"

            unit_results.append(
                {
                    "event_id": pointer.event_id,
                    "division": pointer.division,
                    "round_number": pointer.round_number,
                    "unit_key": unit_key,
                    "status": status,
                    "source_fetch_ts": pointer.last_fetched_at,
                    "source_s3_json_key": pointer.latest_s3_json_key,
                    "silver_s3_rows_key": write_ptrs["s3_rows_key"],
                    "rows_out": write_ptrs["row_count"],
                    "scores_in_payload": transform_stats["total_scores"],
                    "skipped_non_object_scores": transform_stats["skipped_non_object_scores"],
                    "skipped_missing_result_id": transform_stats["skipped_missing_result_id"],
                }
            )

        except Exception as exc:
            stats.failed_units += 1
            logger.exception(
                "silver_apply_unit_failed",
                extra={
                    "event_id": pointer.event_id,
                    "division": pointer.division,
                    "round_number": pointer.round_number,
                    "error": str(exc),
                },
            )
            unit_results.append(
                {
                    "event_id": pointer.event_id,
                    "division": pointer.division,
                    "round_number": pointer.round_number,
                    "unit_key": unit_key,
                    "status": "failed",
                    "source_fetch_ts": pointer.last_fetched_at,
                    "source_s3_json_key": pointer.latest_s3_json_key,
                    "error": str(exc),
                }
            )

    checkpoint_target = None
    if selected:
        latest = selected[-1]
        checkpoint_target = {
            "last_processed_fetch_ts": latest.last_fetched_at,
            "last_processed_s3_key": latest.latest_s3_json_key,
        }

    if checkpoint_target and stats.failed_units == 0:
        advanced = put_global_checkpoint(
            table_name=table_name,
            pipeline_name=pipeline_name,
            last_processed_fetch_ts=checkpoint_target["last_processed_fetch_ts"],
            last_processed_s3_key=checkpoint_target["last_processed_s3_key"],
            run_id=apply_run_id,
            aws_region=aws_region,
        )
        stats.checkpoint_advanced = 1 if advanced else 0

    status = "success" if stats.failed_units == 0 else "failed"
    put_run_summary(
        table_name=table_name,
        pipeline_name=pipeline_name,
        run_id=apply_run_id,
        status=status,
        stats=stats.to_dict(),
        aws_region=aws_region,
    )

    return {
        "run_id": apply_run_id,
        "status": status,
        "summary": stats.to_dict(),
        "checkpoint_target": checkpoint_target,
        "units": unit_results,
    }