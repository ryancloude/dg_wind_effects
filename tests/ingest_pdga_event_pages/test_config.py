import os

import pytest

from ingest_pdga_event_pages.config import load_config


def test_load_config_reads_required_env_vars(monkeypatch):
    monkeypatch.setenv("PDGA_S3_BUCKET", "test-bucket")
    monkeypatch.setenv("PDGA_DDB_TABLE", "test-table")
    monkeypatch.setenv("AWS_REGION", "us-east-1")

    cfg = load_config()

    assert cfg.s3_bucket == "test-bucket"
    assert cfg.ddb_table == "test-table"
    assert cfg.aws_region == "us-east-1"


def test_load_config_allows_missing_aws_region(monkeypatch):
    monkeypatch.setenv("PDGA_S3_BUCKET", "test-bucket")
    monkeypatch.setenv("PDGA_DDB_TABLE", "test-table")
    monkeypatch.delenv("AWS_REGION", raising=False)

    cfg = load_config()

    assert cfg.s3_bucket == "test-bucket"
    assert cfg.ddb_table == "test-table"
    assert cfg.aws_region is None


def test_load_config_raises_when_bucket_missing(monkeypatch):
    monkeypatch.delenv("PDGA_S3_BUCKET", raising=False)
    monkeypatch.setenv("PDGA_DDB_TABLE", "test-table")
    monkeypatch.setenv("AWS_REGION", "us-east-1")

    with pytest.raises(KeyError):
        load_config()


def test_load_config_raises_when_table_missing(monkeypatch):
    monkeypatch.setenv("PDGA_S3_BUCKET", "test-bucket")
    monkeypatch.delenv("PDGA_DDB_TABLE", raising=False)
    monkeypatch.setenv("AWS_REGION", "us-east-1")

    with pytest.raises(KeyError):
        load_config()