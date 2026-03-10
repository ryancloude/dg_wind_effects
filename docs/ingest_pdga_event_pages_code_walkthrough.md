# Ingest PDGA Event Pages - Code Walkthrough

## Purpose
`ingest_pdga_event_pages` is the Bronze HTML ingestion pipeline for PDGA event pages.

It does three things:
1. Fetch raw event page HTML from `https://www.pdga.com/tour/event/<event_id>`.
2. Parse discovery metadata needed by downstream jobs.
3. Store replayable raw HTML in S3 and normalized metadata in DynamoDB with idempotent behavior.

## Module Map

- `ingest_pdga_event_pages/config.py`
  - Loads `PDGA_S3_BUCKET`, `PDGA_DDB_TABLE`, `AWS_REGION`, `PDGA_DDB_STATUS_END_DATE_GSI`.

- `ingest_pdga_event_pages/http_client.py`
  - Builds retrying HTTP session (`urllib3 Retry`).
  - Adds polite randomized delay between requests.
  - Fetches HTML for one event ID.

- `ingest_pdga_event_pages/event_page_parser.py`
  - Parses event name, date range, status, division rounds, location fields.
  - Detects unscheduled placeholder pages.
  - Computes hashes:
    - `content_sha256`
    - `raw_html_sha256`
    - `idempotency_sha256` (semantic change hash)

- `ingest_pdga_event_pages/s3_writer.py`
  - Writes gzipped HTML to:
    - `bronze/pdga/event_page/event_id=<id>/fetch_date=<YYYY-MM-DD>/fetch_ts=<UTC>.html.gz`
  - Writes sidecar metadata JSON:
    - `...fetch_ts=<UTC>.meta.json`

- `ingest_pdga_event_pages/dynamo_reader.py`
  - Reads current event metadata hash for idempotency checks.
  - Queries GSI for incremental rescrape candidates.
  - Finds max known `event_id` for forward scanning.

- `ingest_pdga_event_pages/dynamo_writer.py`
  - Upserts event metadata item:
    - `pk=EVENT#<event_id>`
    - `sk=METADATA`

- `ingest_pdga_event_pages/runner.py`
  - CLI and orchestration for explicit IDs, ranges, backfill, and incremental mode.

## Runner Modes

### Explicit IDs
- Input: `--ids 100001,100002`
- Behavior: processes exactly those IDs.

### Explicit Range
- Input: `--range 100000-100050`
- Behavior: inclusive range processing.

### Forward Backfill
- Input: `--backfill-start-id <id>`
- Behavior:
  - Scans upward sequentially.
  - Stops after N consecutive “no-event-yet” outcomes.
  - “No event yet” is either:
    - unscheduled placeholder page, or
    - HTTP 404
  - Stop threshold controlled by `--backfill-stop-after-unscheduled`.

### Incremental
- Input: `--incremental`
- Two-phase process:
1. Rescrape existing recent candidates from DynamoDB via status/date GSI query.
2. Forward scan from `max_known_event_id + 1` until stop condition.

Defaults:
- statuses:
  - `Sanctioned`
  - `Event report received; official ratings pending.`
  - `Event complete; waiting for report.`
  - `In progress.`
  - `Errata pending.`
- window: 183 days (override with `--incremental-window-days`)

## Per-Event Processing Lifecycle

1. Build page URL (`/tour/event/<id>`).
2. Fetch HTML with retries/backoff.
3. Parse HTML into structured metadata.
4. If not dry-run:
  - Read existing `idempotency_sha256` from DynamoDB.
  - If unchanged:
    - skip S3 and metadata rewrite.
  - If changed/new:
    - write raw HTML + sidecar to S3.
    - upsert METADATA item in DynamoDB.
5. Emit structured log (`event_ok`) with change type and placeholder flag.
6. Sleep politely before next event.

## Idempotency Design

Primary semantic idempotency key:
- `idempotency_sha256` from parsed meaningful fields:
  - name, date range, status, division rounds, placeholder flag, location fields.

Effect:
- Raw HTML/meta writes and metadata updates occur only when semantic content changes.
- Safe to rerun repeatedly without duplicate event versions for unchanged pages.

## Hashes and Their Roles

- `content_sha256`:
  - hash of current HTML content.
- `raw_html_sha256`:
  - hash of raw HTML payload.
- `idempotency_sha256`:
  - hash of normalized parsed fields that define business-level change.

## DynamoDB Item Written

Key:
- `pk=EVENT#<event_id>`
- `sk=METADATA`

Core attributes:
- event basics: `event_id`, `name`, `source_url`, dates, `status_text`
- structure: `division_rounds`
- location: `location_raw`, `city`, `state`, `country`
- parsing: `parser_version`, `parse_warnings`, `is_unscheduled_placeholder`
- hashes: `content_sha256`, `raw_html_sha256`, `idempotency_sha256`
- S3 pointers: `latest_s3_html_key`, `latest_s3_meta_key`
- timestamps: `first_seen_at`, `last_fetched_at`

## Failure Handling

- Non-404 HTTP errors: counted as failures, logged, pipeline continues.
- 404 in forward scan:
  - treated as no-event-yet signal for stop-streak logic.
- Parser edge cases:
  - represented in `parse_warnings` instead of crashing when recoverable.

## Observability

- Logs include:
  - `event_id`
  - `change_type` (`new`, `updated`, `unchanged`)
  - placeholder detection
  - divisions count
  - S3 key pointer
- Final summary metrics:
  - scraped
  - new_scraped
  - updated_scraped
  - unchanged_scraped
  - not_found_404
  - failed

## Practical Notes

- Incremental mode depends on `status_text/end_date` GSI quality.
- Forward scan relies on stop-after-consecutive-no-event heuristic; tune for sensitivity.
- Always rebuild Docker image after code changes to avoid stale installed package behavior.