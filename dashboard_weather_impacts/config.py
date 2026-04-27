from __future__ import annotations

import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


@dataclass(frozen=True)
class DashboardConfig:
    s3_bucket: str
    aws_region: str | None
    reports_published_prefix: str = "gold/pdga/wind_effects/reports/published/"
    scored_rounds_prefix: str = "gold/pdga/wind_effects/scored_rounds/"


def load_dashboard_config() -> DashboardConfig:
    return DashboardConfig(
        s3_bucket=os.environ["PDGA_S3_BUCKET"],
        aws_region=os.getenv("AWS_REGION"),
    )
