import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


@dataclass(frozen=True)
class Config:
    s3_bucket: str
    ddb_table: str
    aws_region: str | None = None
    ddb_status_end_date_gsi: str = "gsi_status_end_date"


def load_config() -> Config:
    return Config(
        s3_bucket=os.environ["PDGA_S3_BUCKET"],
        ddb_table=os.environ["PDGA_DDB_TABLE"],
        aws_region=os.getenv("AWS_REGION"),
        ddb_status_end_date_gsi=os.getenv("PDGA_DDB_STATUS_END_DATE_GSI", "gsi_status_end_date"),
    )