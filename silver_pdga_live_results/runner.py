from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

from silver_pdga_live_results.apply_layout_hole import apply_layout_hole_units
from silver_pdga_live_results.apply_player_round import apply_player_round_units, make_apply_run_id
from silver_pdga_live_results.bronze_reader import load_payload_for_pointer
from silver_pdga_live_results.candidate_reader import DEFAULT_ALLOWED_FETCH_STATUSES
from silver_pdga_live_results.config import load_silver_config
from silver_pdga_live_results.planner import build_incremental_plan
from silver_pdga_live_results.player_round_transform import transform_player_round_rows

logger = logging.getLogger("silver_live_results")


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def parse_args():
    parser = argparse.ArgumentParser(
        description="Plan incremental Silver live-results unit loads from Dynamo state rows."
    )
    parser.add_argument("--pipeline-name", help="Override SILVER pipeline name.")
    parser.add_argument("--table-name", help="Override DynamoDB table.")
    parser.add_argument("--aws-region", help="Override AWS region.")
    parser.add_argument(
        "--allowed-statuses",
        help="Comma-separated fetch_status values to include. Defaults to success,empty.",
    )
    parser.add_argument("--candidate-limit", type=positive_int, help="Optional cap before per-round dedupe.")
    parser.add_argument("--preview-units", type=int, default=20, help="How many deduped units to print.")
    parser.add_argument(
        "--preview-transform",
        action="store_true",
        help="Load Bronze JSON and run player-round transform for selected units (no writes).",
    )
    parser.add_argument(
        "--preview-transform-units",
        type=positive_int,
        default=3,
        help="How many deduped units to transform when --preview-transform is set.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply transformed player-round units to Silver S3 and update load state/checkpoint.",
    )
    parser.add_argument(
        "--apply-units",
        type=positive_int,
        default=100,
        help="Maximum deduped units to apply when --apply is set.",
    )
    parser.add_argument(
        "--silver-prefix",
        help="Override S3 prefix for Silver player-round current rows.",
    )
    parser.add_argument(
        "--apply-layout-holes",
        action="store_true",
        help="Apply transformed layout-hole units to Silver S3 and update layout load state/checkpoint.",
    )
    parser.add_argument(
        "--apply-layout-units",
        type=positive_int,
        default=100,
        help="Maximum deduped round units to evaluate for layout-hole apply.",
    )
    parser.add_argument(
        "--layout-prefix",
        help="Override S3 prefix for Silver layout-hole current rows.",
    )
    parser.add_argument(
        "--layout-pipeline-name",
        help="Override pipeline name for layout-hole state/checkpoint. Defaults to <pipeline_name>_LAYOUT_HOLE.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def parse_statuses(raw: str | None) -> set[str]:
    if raw is None:
        return set(DEFAULT_ALLOWED_FETCH_STATUSES)

    statuses = {value.strip().lower() for value in raw.split(",") if value.strip()}
    if not statuses:
        raise ValueError("--allowed-statuses requires at least one status value")
    return statuses


def make_preview_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"silver-live-results-preview-{ts}"


def main() -> int:
    args = parse_args()

    if args.preview_transform and (args.apply or args.apply_layout_holes):
        raise ValueError("Use preview-only mode or apply mode, not both")

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    cfg = load_silver_config()
    pipeline_name = (args.pipeline_name or cfg.pipeline_name).strip().upper()
    table_name = args.table_name or cfg.ddb_table
    aws_region = args.aws_region if args.aws_region is not None else cfg.aws_region

    allowed_statuses = parse_statuses(args.allowed_statuses)

    plan = build_incremental_plan(
        table_name=table_name,
        pipeline_name=pipeline_name,
        allowed_statuses=allowed_statuses,
        candidate_limit=args.candidate_limit,
        aws_region=aws_region,
    )

    output: dict[str, object] = {"incremental_plan": plan.to_dict(preview_units=args.preview_units)}

    if args.preview_transform:
        run_id = make_preview_run_id()
        selected = list(plan.candidate_pointers[: max(args.preview_transform_units, 1)])

        preview_totals = {
            "run_id": run_id,
            "units_selected": len(selected),
            "units_transformed": 0,
            "units_failed": 0,
            "rows_out": 0,
            "skipped_non_object_scores": 0,
            "skipped_missing_result_id": 0,
        }
        preview_units: list[dict[str, object]] = []

        for pointer in selected:
            try:
                bronze_payload = load_payload_for_pointer(
                    bucket=cfg.s3_bucket,
                    pointer=pointer,
                    aws_region=aws_region,
                )
                rows, stats = transform_player_round_rows(
                    pointer=pointer,
                    payload=bronze_payload.payload,
                    run_id=run_id,
                )

                preview_totals["units_transformed"] += 1
                preview_totals["rows_out"] += stats["output_rows"]
                preview_totals["skipped_non_object_scores"] += stats["skipped_non_object_scores"]
                preview_totals["skipped_missing_result_id"] += stats["skipped_missing_result_id"]

                preview_units.append(
                    {
                        "event_id": pointer.event_id,
                        "division": pointer.division,
                        "round_number": pointer.round_number,
                        "source_fetch_ts": pointer.last_fetched_at,
                        "source_s3_json_key": pointer.latest_s3_json_key,
                        "scores_in_payload": stats["total_scores"],
                        "rows_out": stats["output_rows"],
                        "skipped_non_object_scores": stats["skipped_non_object_scores"],
                        "skipped_missing_result_id": stats["skipped_missing_result_id"],
                    }
                )
            except Exception as exc:
                preview_totals["units_failed"] += 1
                preview_units.append(
                    {
                        "event_id": pointer.event_id,
                        "division": pointer.division,
                        "round_number": pointer.round_number,
                        "source_fetch_ts": pointer.last_fetched_at,
                        "source_s3_json_key": pointer.latest_s3_json_key,
                        "error": str(exc),
                    }
                )

        output["transform_preview"] = {
            **preview_totals,
            "units": preview_units,
        }

    if args.apply:
        silver_prefix = args.silver_prefix or cfg.silver_player_round_prefix
        apply_result = apply_player_round_units(
            table_name=table_name,
            pipeline_name=pipeline_name,
            bucket=cfg.s3_bucket,
            silver_prefix=silver_prefix,
            pointers=plan.candidate_pointers,
            run_id=make_apply_run_id(),
            max_units=args.apply_units,
            aws_region=aws_region,
        )

        units = apply_result.get("units", [])
        preview_count = max(args.preview_units, 0)
        output["apply_result"] = {
            "run_id": apply_result["run_id"],
            "status": apply_result["status"],
            "summary": apply_result["summary"],
            "checkpoint_target": apply_result["checkpoint_target"],
            "units_total": len(units),
            "units_preview": units[:preview_count],
        }

    if args.apply_layout_holes:
        layout_pipeline_name = (
            (args.layout_pipeline_name or f"{pipeline_name}_LAYOUT_HOLE").strip().upper()
        )
        layout_prefix = args.layout_prefix or "silver/pdga/live_results/layout_hole_current"

        apply_layout_result = apply_layout_hole_units(
            table_name=table_name,
            pipeline_name=layout_pipeline_name,
            bucket=cfg.s3_bucket,
            silver_prefix=layout_prefix,
            pointers=plan.candidate_pointers,
            max_units=args.apply_layout_units,
            aws_region=aws_region,
        )

        units = apply_layout_result.get("units", [])
        preview_count = max(args.preview_units, 0)
        output["apply_layout_result"] = {
            "run_id": apply_layout_result["run_id"],
            "status": apply_layout_result["status"],
            "summary": apply_layout_result["summary"],
            "checkpoint_target": apply_layout_result["checkpoint_target"],
            "units_total": len(units),
            "units_preview": units[:preview_count],
        }

    logger.info("silver_incremental_plan", extra=output["incremental_plan"])
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())