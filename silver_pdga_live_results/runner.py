from __future__ import annotations

import argparse
import logging

from silver_pdga_live_results.candidate_reader import DEFAULT_ALLOWED_FETCH_STATUSES
from silver_pdga_live_results.config import load_silver_config
from silver_pdga_live_results.planner import build_incremental_plan

logger = logging.getLogger("silver_live_results")


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
    parser.add_argument("--candidate-limit", type=int, help="Optional cap before per-round dedupe.")
    parser.add_argument("--preview-units", type=int, default=20, help="How many deduped units to print.")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def parse_statuses(raw: str | None) -> set[str]:
    if raw is None:
        return set(DEFAULT_ALLOWED_FETCH_STATUSES)

    statuses = {value.strip().lower() for value in raw.split(",") if value.strip()}
    if not statuses:
        raise ValueError("--allowed-statuses requires at least one status value")
    return statuses


def main() -> int:
    args = parse_args()

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

    output = {"incremental_plan": plan.to_dict(preview_units=args.preview_units)}
    logger.info("silver_incremental_plan", extra=output["incremental_plan"])
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())