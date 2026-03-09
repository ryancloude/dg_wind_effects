from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence

from silver_pdga_live_results.bronze_reader import load_payload_for_pointer
from silver_pdga_live_results.candidate_reader import LiveResultsStatePointer
from silver_pdga_live_results.layout_hole_transform import (
    compute_layout_group_hash,
    group_rows_by_layout,
    transform_layout_hole_rows,
)
from silver_pdga_live_results.layout_hole_writer import put_layout_hole_current
from silver_pdga_live_results.load_state import (
    get_round_unit_state,
    put_global_checkpoint,
    put_round_unit_state,
    put_run_summary,
)

logger = logging.getLogger("silver_live_results_layout_apply")


def make_apply_layout_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"silver-layout-hole-apply-{ts}"


def build_layout_unit_key(layout_id: int) -> str:
    if int(layout_id) <= 0:
        raise ValueError("layout_id must be positive")
    return f"LAYOUT#{int(layout_id)}"


@dataclass
class LayoutApplyStats:
    selected_round_units: int = 0
    attempted_round_units: int = 0
    transformed_round_units: int = 0
    selected_layout_units: int = 0
    applied_layout_units: int = 0
    skipped_unchanged_layout_units: int = 0
    skipped_stale_state_layout_units: int = 0
    failed_round_units: int = 0
    rows_written: int = 0
    checkpoint_advanced: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "selected_round_units": self.selected_round_units,
            "attempted_round_units": self.attempted_round_units,
            "transformed_round_units": self.transformed_round_units,
            "selected_layout_units": self.selected_layout_units,
            "applied_layout_units": self.applied_layout_units,
            "skipped_unchanged_layout_units": self.skipped_unchanged_layout_units,
            "skipped_stale_state_layout_units": self.skipped_stale_state_layout_units,
            "failed_round_units": self.failed_round_units,
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


def apply_layout_hole_units(
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
    apply_run_id = run_id or make_apply_layout_run_id()
    selected = _select_apply_pointers(pointers=pointers, max_units=max_units)

    stats = LayoutApplyStats(selected_round_units=len(selected))
    unit_results: list[dict[str, Any]] = []

    for pointer in selected:
        stats.attempted_round_units += 1

        try:
            bronze_payload = load_payload_for_pointer(
                bucket=bucket,
                pointer=pointer,
                aws_region=aws_region,
            )
            rows, transform_stats = transform_layout_hole_rows(
                pointer=pointer,
                payload=bronze_payload.payload,
                run_id=apply_run_id,
            )
            grouped = group_rows_by_layout(rows)

            stats.transformed_round_units += 1
            stats.selected_layout_units += len(grouped)

            for layout_id, layout_rows in grouped.items():
                unit_key = build_layout_unit_key(layout_id)
                layout_hash = compute_layout_group_hash(layout_rows)

                existing_state = get_round_unit_state(
                    table_name=table_name,
                    pipeline_name=pipeline_name,
                    unit_key=unit_key,
                    aws_region=aws_region,
                )
                if existing_state and existing_state.last_applied_sha256 == layout_hash:
                    stats.skipped_unchanged_layout_units += 1
                    unit_results.append(
                        {
                            "layout_id": layout_id,
                            "unit_key": unit_key,
                            "status": "skipped_unchanged",
                            "source_fetch_ts": pointer.last_fetched_at,
                            "source_s3_json_key": pointer.latest_s3_json_key,
                        }
                    )
                    continue

                write_ptrs = put_layout_hole_current(
                    bucket=bucket,
                    silver_prefix=silver_prefix,
                    layout_id=layout_id,
                    source_fetch_ts=pointer.last_fetched_at,
                    source_content_sha256=pointer.content_sha256,
                    source_event_id=pointer.event_id,
                    source_division_code=pointer.division,
                    source_round_number=pointer.round_number,
                    source_url=pointer.source_url,
                    rows=layout_rows,
                    run_id=apply_run_id,
                )
                stats.rows_written += len(layout_rows)

                state_updated = put_round_unit_state(
                    table_name=table_name,
                    pipeline_name=pipeline_name,
                    unit_key=unit_key,
                    last_applied_sha256=layout_hash,
                    last_applied_fetch_ts=pointer.last_fetched_at,
                    last_applied_s3_key=write_ptrs["s3_rows_key"],
                    last_applied_row_count=len(layout_rows),
                    run_id=apply_run_id,
                    aws_region=aws_region,
                )

                if state_updated:
                    stats.applied_layout_units += 1
                    status = "applied"
                else:
                    stats.skipped_stale_state_layout_units += 1
                    status = "skipped_stale_state"

                unit_results.append(
                    {
                        "layout_id": layout_id,
                        "unit_key": unit_key,
                        "status": status,
                        "source_fetch_ts": pointer.last_fetched_at,
                        "source_s3_json_key": pointer.latest_s3_json_key,
                        "silver_s3_rows_key": write_ptrs["s3_rows_key"],
                        "rows_out": write_ptrs["row_count"],
                        "scores_in_payload": transform_stats["total_layouts"] * transform_stats["total_holes"],
                    }
                )

        except Exception as exc:
            stats.failed_round_units += 1
            logger.exception(
                "silver_layout_apply_round_failed",
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
                    "status": "failed_round",
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

    if checkpoint_target and stats.failed_round_units == 0:
        advanced = put_global_checkpoint(
            table_name=table_name,
            pipeline_name=pipeline_name,
            last_processed_fetch_ts=checkpoint_target["last_processed_fetch_ts"],
            last_processed_s3_key=checkpoint_target["last_processed_s3_key"],
            run_id=apply_run_id,
            aws_region=aws_region,
        )
        stats.checkpoint_advanced = 1 if advanced else 0

    status = "success" if stats.failed_round_units == 0 else "failed"
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