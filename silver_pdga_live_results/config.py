from __future__ import annotations

import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


@dataclass(frozen=True)
class SilverConfig:
    s3_bucket: str
    ddb_table: str
    aws_region: str | None = None
    pipeline_name: str = "LIVE_RESULTS_SILVER"


def load_silver_config() -> SilverConfig:
    return SilverConfig(
        s3_bucket=os.environ["PDGA_S3_BUCKET"],
        ddb_table=os.environ["PDGA_DDB_TABLE"],
        aws_region=os.getenv("AWS_REGION"),
        pipeline_name=os.getenv("SILVER_LIVE_RESULTS_PIPELINE_NAME", "LIVE_RESULTS_SILVER"),
    )