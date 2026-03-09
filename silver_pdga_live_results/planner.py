from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from silver_pdga_live_results.candidate_reader import (
    LiveResultsStatePointer,
    collect_live_results_state_pointers,
    latest_pointer_per_round,
)
from silver_pdga_live_results.load_state import get_global_checkpoint


@dataclass(frozen=True)
class IncrementalPlan:
    pipeline_name: str
    checkpoint_fetch_ts: str | None
    checkpoint_s3_key: str | None
    raw_candidate_count: int
    deduped_unit_count: int
    max_candidate_fetch_ts: str | None
    max_candidate_s3_key: str | None
    candidate_pointers: tuple[LiveResultsStatePointer, ...]

    def to_dict(self, preview_units: int = 20) -> dict[str, Any]:
        preview_count = max(preview_units, 0)
        preview = [
            {
                "event_id": p.event_id,
                "division": p.division,
                "round_number": p.round_number,
                "last_fetched_at": p.last_fetched_at,
                "latest_s3_json_key": p.latest_s3_json_key,
                "content_sha256": p.content_sha256,
            }
            for p in self.candidate_pointers[:preview_count]
        ]

        return {
            "pipeline_name": self.pipeline_name,
            "checkpoint_fetch_ts": self.checkpoint_fetch_ts,
            "checkpoint_s3_key": self.checkpoint_s3_key,
            "raw_candidate_count": self.raw_candidate_count,
            "deduped_unit_count": self.deduped_unit_count,
            "max_candidate_fetch_ts": self.max_candidate_fetch_ts,
            "max_candidate_s3_key": self.max_candidate_s3_key,
            "preview_units": preview,
        }


def build_incremental_plan(
    *,
    table_name: str,
    pipeline_name: str,
    allowed_statuses: set[str] | None = None,
    candidate_limit: int | None = None,
    aws_region: str | None = None,
) -> IncrementalPlan:
    checkpoint = get_global_checkpoint(
        table_name=table_name,
        pipeline_name=pipeline_name,
        aws_region=aws_region,
    )

    raw_candidates = collect_live_results_state_pointers(
        table_name=table_name,
        allowed_statuses=allowed_statuses,
        cursor_fetch_ts=checkpoint.last_processed_fetch_ts,
        cursor_s3_key=checkpoint.last_processed_s3_key,
        limit=candidate_limit,
        aws_region=aws_region,
    )

    deduped = latest_pointer_per_round(raw_candidates)

    max_fetch_ts = None
    max_s3_key = None
    if deduped:
        latest = max(deduped, key=lambda p: p.cursor_tuple())
        max_fetch_ts = latest.last_fetched_at
        max_s3_key = latest.latest_s3_json_key

    return IncrementalPlan(
        pipeline_name=pipeline_name,
        checkpoint_fetch_ts=checkpoint.last_processed_fetch_ts,
        checkpoint_s3_key=checkpoint.last_processed_s3_key,
        raw_candidate_count=len(raw_candidates),
        deduped_unit_count=len(deduped),
        max_candidate_fetch_ts=max_fetch_ts,
        max_candidate_s3_key=max_s3_key,
        candidate_pointers=tuple(deduped),
    )