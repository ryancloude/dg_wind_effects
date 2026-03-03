 DG Wind Effects - PDGA Bronze Ingest

Bronze-layer ingestion for PDGA event pages:
- Fetch event page HTML
- Parse discovery fields such as dates, status, and division round counts
- Store raw HTML in S3 and metadata in DynamoDB
- Support idempotent re-runs using a stable content hash
- Support historical backfill by scanning sequential PDGA event IDs

## Current Scope

This package is the Bronze ingest layer for PDGA tournament event pages.

Today it focuses on:
- raw HTML capture to S3
- lightweight metadata extraction
- idempotent writes
- historical backfill support for event-page discovery

It does not yet build Silver/Gold datasets. The purpose of this layer is to preserve replayable raw data and extract enough metadata to support downstream normalization and analytics.

## Project Structure

```text
dg_wind_effects/
  ingest_pdga_event_pages/
    config.py
    dynamo_reader.py
    dynamo_writer.py
    event_page_parser.py
    http_client.py
    runner.py
    s3_writer.py
  tests/
    fixtures/
    test_config.py
    test_event_page_parser.py
    test_runner.py
    test_s3_writer.py
  README.md
  pyproject.toml
  Dockerfile