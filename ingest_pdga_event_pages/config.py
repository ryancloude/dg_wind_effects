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

def load_config() -> Config:
    return Config(
        s3_bucket=os.environ["PDGA_S3_BUCKET"],
        ddb_table=os.environ["PDGA_DDB_TABLE"],
        aws_region=os.getenv("AWS_REGION"),
    )