```md
# DG Wind Effects - PDGA Bronze Ingest

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
- incremental refreshes for recent events likely to change

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
    test_dynamo_reader.py
    test_dynamo_writer.py
    test_event_page_parser.py
    test_runner.py
    test_s3_writer.py
  README.md
  pyproject.toml
  Dockerfile
```

## Environment Variables

The app reads configuration from environment variables, including values loaded from a local `.env` file.

Required:
- `PDGA_S3_BUCKET`
- `PDGA_DDB_TABLE`

Optional:
- `AWS_REGION`

Example `.env`:

```dotenv
PDGA_S3_BUCKET=my-pdga-bronze-bucket
PDGA_DDB_TABLE=pdga-event-index
AWS_REGION=us-east-1
```

## Local Setup

Create and activate a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install package + dev dependencies:

```powershell
pip install -e .[dev]
```

## Running the Ingest

The CLI supports four primary modes.

### 1) Explicit IDs

```powershell
python -m ingest_pdga_event_pages.runner --ids 100001,100002,100003
```

### 2) Inclusive Range

```powershell
python -m ingest_pdga_event_pages.runner --range 100000-100010
```

### 3) Historical Backfill

Sequentially scans upward from a starting event ID until stop condition is met.

```powershell
python -m ingest_pdga_event_pages.runner --backfill-start-id 90000
```

### 4) Incremental Mode

Runs a two-phase process:
1. Re-scrape recent events from DynamoDB metadata using status/date filters.
2. Scan higher IDs forward until stop condition is met.

```powershell
python -m ingest_pdga_event_pages.runner --incremental
```

## Incremental Mode Details

Default candidate statuses (used when `--incremental-statuses` is not provided):
- `Sanctioned`
- `Event report received; official ratings pending.`
- `Event complete; waiting for report.`
- `In progress.`
- `Errata pending.`

`Event complete.` is intentionally excluded from default incremental candidates.

Date window logic in incremental mode:
- include only events where `end_date < today`
- include only events where `end_date >= today - incremental_window_days`
- default `incremental_window_days = 183` (~6 months)

Override statuses if needed:

```powershell
python -m ingest_pdga_event_pages.runner --incremental --incremental-statuses "Sanctioned,Errata pending."
```

Override window length:

```powershell
python -m ingest_pdga_event_pages.runner --incremental --incremental-window-days 183
```

## Forward Scan Stop Rule

Backfill and incremental forward scan stop after `N` consecutive “no event yet” outcomes.

“No event yet” includes:
- parsed unscheduled placeholder pages
- HTTP `404` for `https://www.pdga.com/tour/event/<event_id>`

Default threshold:
- `--backfill-stop-after-unscheduled 5`

Optional safety cap:

```powershell
python -m ingest_pdga_event_pages.runner --incremental --backfill-max-event-id 102500
```

## Dry Run

`--dry-run` fetches and parses pages but does not write to S3 or DynamoDB.

```powershell
python -m ingest_pdga_event_pages.runner --incremental --dry-run --log-level INFO
```

## Storage Behavior

### S3 (Bronze Raw)

Raw payload is written as:
- gzipped HTML
- metadata sidecar JSON

Example key layout:

```text
bronze/pdga/event_page/event_id=12345/fetch_date=2026-03-04/fetch_ts=2026-03-04T12:00:00Z.html.gz
bronze/pdga/event_page/event_id=12345/fetch_date=2026-03-04/fetch_ts=2026-03-04T12:00:00Z.meta.json
```

### DynamoDB

One metadata item per event:
- `pk = EVENT#<event_id>`
- `sk = METADATA`

Metadata includes:
- `name`
- `source_url`
- `raw_date_str`
- `start_date`
- `end_date`
- `status_text`
- `division_rounds`
- `parse_warnings`
- `idempotency_sha256`
- `is_unscheduled_placeholder`
- latest S3 pointers

## Testing

Run all tests:

```powershell
pytest
```

Run targeted suites:

```powershell
pytest tests/test_event_page_parser.py -v
pytest tests/test_runner.py -v
pytest tests/test_dynamo_reader.py -v
pytest tests/test_dynamo_writer.py -v
pytest tests/test_s3_writer.py -v
pytest tests/test_config.py -v
```

## Fixtures and Regression Testing

`tests/fixtures/` stores representative PDGA HTML inputs for parser coverage.

Recommended fixture types:
- normal scheduled event
- multi-day event
- unscheduled placeholder event
- missing status
- no divisions/results table
- any real backfill edge case that previously failed parsing

Regression workflow:
1. Capture failing real-world page shape.
2. Add fixture + failing test.
3. Fix parser/runner behavior.
4. Keep the test to prevent recurrence.

## Operational Notes

- Incremental candidate selection currently uses DynamoDB scans, which is acceptable for modest table size.
- Date comparisons assume ISO format (`YYYY-MM-DD`) in DynamoDB.
- If running in Docker, rebuild image after code changes to avoid running stale installed code.

## Recommended Default Incremental Command

```powershell
python -m ingest_pdga_event_pages.runner --incremental --backfill-stop-after-unscheduled 5 --incremental-window-days 183
```
```