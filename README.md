Here is a merged `README.md` that keeps your current event-pages documentation and adds live-results documentation in the same style.

```md
# DG Wind Effects - PDGA Bronze Ingest

Bronze-layer ingestion for PDGA data sources:

- Event pages:
  - Fetch event page HTML
  - Parse discovery fields such as dates, status, and division round counts
  - Store raw HTML in S3 and metadata in DynamoDB
  - Support idempotent re-runs using a stable content hash
  - Support historical backfill by scanning sequential PDGA event IDs

- Live results API:
  - Generate `(event_id, division, round)` tasks from DynamoDB METADATA
  - Fetch raw live results JSON from PDGA API
  - Store raw JSON in S3 and state/run metadata in DynamoDB
  - Support idempotent re-runs via canonical payload hash
  - Support historical backfill with status/date filters

## Current Scope

This repo currently implements Bronze ingest layers for:

1. `ingest_pdga_event_pages`
2. `ingest_pdga_live_results`

Today it focuses on:

- replayable raw source capture to S3
- lightweight metadata extraction and normalization for downstream processing
- idempotent writes and re-run safety
- historical backfill and incremental refresh modes

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

  ingest_pdga_live_results/
    backfill_live_results_ingested_flag.py
    dynamo_reader.py
    dynamo_writer.py
    http_client.py
    response_handler.py
    runner.py
    s3_writer.py

  tests/
    ingest_pdga_event_pages/
      fixtures/
      test_config.py
      test_dynamo_reader.py
      test_dynamo_writer.py
      test_event_page_parser.py
      test_runner.py
      test_s3_writer.py

    ingest_pdga_live_results/
      test_backfill_live_results_ingest_flag.py
      test_dynamo_reader.py
      test_dynamo_writer.py
      test_http_client.py
      test_response_handler.py
      test_runner.py
      test_s3_writer.py

  README.md
  pyproject.toml
  Dockerfile.event_pages
  Dockerfile.live_results
```

## Environment Variables

The app reads configuration from environment variables, including values loaded from a local `.env` file.

Required:

- `PDGA_S3_BUCKET`
- `PDGA_DDB_TABLE`

Optional:

- `AWS_REGION`
- `PDGA_DDB_STATUS_END_DATE_GSI` (defaults to `gsi_status_end_date`)

Example `.env`:

```dotenv
PDGA_S3_BUCKET=my-pdga-bronze-bucket
PDGA_DDB_TABLE=pdga-event-index
AWS_REGION=us-east-2
PDGA_DDB_STATUS_END_DATE_GSI=gsi_status_end_date
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

## Running Event Page Ingest

### 1) Explicit IDs

```powershell
python -m ingest_pdga_event_pages.runner --ids 100001,100002,100003
```

### 2) Inclusive Range

```powershell
python -m ingest_pdga_event_pages.runner --range 100000-100010
```

### 3) Historical Backfill

Sequentially scans upward from a starting event ID until stop condition is met:

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

### Incremental Mode Details

Default candidate statuses (used when `--incremental-statuses` is not provided):

- `Sanctioned`
- `Event report received; official ratings pending.`
- `Event complete; waiting for report.`
- `In progress.`
- `Errata pending.`

Date window logic in incremental mode:

- include only events where `end_date < today`
- include only events where `end_date >= today - incremental_window_days`
- default `incremental_window_days = 183` (~6 months)

Override statuses:

```powershell
python -m ingest_pdga_event_pages.runner --incremental --incremental-statuses "Sanctioned,Errata pending."
```

Override window length:

```powershell
python -m ingest_pdga_event_pages.runner --incremental --incremental-window-days 183
```

### Forward Scan Stop Rule

Backfill and incremental forward scan stop after `N` consecutive "no event yet" outcomes.

"No event yet" includes:

- parsed unscheduled placeholder pages
- HTTP `404` for `https://www.pdga.com/tour/event/<event_id>`

Default threshold:

- `--backfill-stop-after-unscheduled 5`

Optional safety cap:

```powershell
python -m ingest_pdga_event_pages.runner --incremental --backfill-max-event-id 102500
```

### Dry Run

`--dry-run` fetches and parses pages but does not write to S3 or DynamoDB:

```powershell
python -m ingest_pdga_event_pages.runner --incremental --dry-run --log-level INFO
```

## Running Live Results Ingest

### Explicit Event IDs

```powershell
python -m ingest_pdga_live_results.runner --event-ids 92608,92612
```

### Event IDs from S3 File (optional)

```powershell
python -m ingest_pdga_live_results.runner --event-ids-s3-uri s3://my-bucket/path/to/event_ids.csv
```

### Historical Backfill

```powershell
python -m ingest_pdga_live_results.runner --historical-backfill
```

Historical mode defaults:

- excludes statuses:
  - `Sanctioned`
  - `Event report received; official ratings pending.`
  - `Event complete; waiting for report.`
  - `In progress.`
  - `Errata pending.`
- requires non-empty `division_rounds`
- excludes events already marked `live_results_ingested=true`

### Override Historical Excluded Statuses

```powershell
python -m ingest_pdga_live_results.runner --historical-backfill --historical-excluded-statuses "Sanctioned,In progress."
```

### Dry Run

```powershell
python -m ingest_pdga_live_results.runner --historical-backfill --dry-run
```

### Throughput / Progress Controls

```powershell
python -m ingest_pdga_live_results.runner --historical-backfill --sleep-base 0.7 --sleep-jitter 0.5 --progress-every 50
```

## Storage Behavior

### S3 Bronze Raw

Event page ingest writes:

```text
bronze/pdga/event_page/event_id=<event_id>/fetch_date=<YYYY-MM-DD>/fetch_ts=<UTC_ISO>.html.gz
bronze/pdga/event_page/event_id=<event_id>/fetch_date=<YYYY-MM-DD>/fetch_ts=<UTC_ISO>.meta.json
```

Live results ingest writes:

```text
bronze/pdga/live_results/event_id=<event_id>/division=<division>/round=<round>/fetch_date=<YYYY-MM-DD>/fetch_ts=<UTC_ISO>.json
bronze/pdga/live_results/event_id=<event_id>/division=<division>/round=<round>/fetch_date=<YYYY-MM-DD>/fetch_ts=<UTC_ISO>.meta.json
```

### DynamoDB

Event metadata item:

- `pk = EVENT#<event_id>`
- `sk = METADATA`

Live results state item (per division+round):

- `pk = EVENT#<event_id>`
- `sk = LIVE_RESULTS#DIV#<division>#ROUND#<round>`

Live results run summary item:

- `pk = RUN#<run_id>`
- `sk = LIVE_RESULTS#SUMMARY`

Live results completion markers on METADATA:

- `live_results_ingested = true`
- `live_results_ingested_at`
- `live_results_ingested_run_id`

## Backfill Utility

One-time utility to mark existing events that already have live results state rows:

```powershell
python -m ingest_pdga_live_results.backfill_live_results_ingested_flag
```

## Testing

Run all tests:

```powershell
python -m pytest
```

Run event-pages suite:

```powershell
python -m pytest tests/ingest_pdga_event_pages -v
```

Run live-results suite:

```powershell
python -m pytest tests/ingest_pdga_live_results -v
```

Run targeted suites:

```powershell
python -m pytest tests/ingest_pdga_event_pages/test_event_page_parser.py -v
python -m pytest tests/ingest_pdga_event_pages/test_runner.py -v
python -m pytest tests/ingest_pdga_event_pages/test_dynamo_reader.py -v
python -m pytest tests/ingest_pdga_event_pages/test_dynamo_writer.py -v
python -m pytest tests/ingest_pdga_event_pages/test_s3_writer.py -v
python -m pytest tests/ingest_pdga_event_pages/test_config.py -v

python -m pytest tests/ingest_pdga_live_results/test_dynamo_reader.py -v
python -m pytest tests/ingest_pdga_live_results/test_dynamo_writer.py -v
python -m pytest tests/ingest_pdga_live_results/test_http_client.py -v
python -m pytest tests/ingest_pdga_live_results/test_response_handler.py -v
python -m pytest tests/ingest_pdga_live_results/test_runner.py -v
python -m pytest tests/ingest_pdga_live_results/test_s3_writer.py -v
python -m pytest tests/ingest_pdga_live_results/test_backfill_live_results_ingest_flag.py -v
```

## Fixtures and Regression Testing

`tests/ingest_pdga_event_pages/fixtures/` stores representative PDGA HTML inputs for parser coverage.

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

- If running in Docker, rebuild image after code changes to avoid running stale installed code.
- Date comparisons assume ISO format (`YYYY-MM-DD`) in DynamoDB.
- If historical planning appears slow, verify `live_results_ingested` flags are populated and historical filters are active.

## Recommended Default Commands

Event pages incremental:

```powershell
python -m ingest_pdga_event_pages.runner --incremental --backfill-stop-after-unscheduled 5 --incremental-window-days 183
```

Live results historical:

```powershell
python -m ingest_pdga_live_results.runner --historical-backfill --sleep-base 0.7 --sleep-jitter 0.5 --progress-every 50
```
```