# Ingest PDGA Live Results - Code Walkthrough

## Purpose
`ingest_pdga_live_results` is the Bronze JSON ingestion pipeline for PDGA live-results API.

It does five things:
1. Build `(event_id, division, round_number)` tasks from event metadata.
2. Fetch JSON payloads from PDGA live API.
3. Classify each response (`success`, `empty`, `not_found`, `failed`).
4. Write changed payloads to Bronze S3.
5. Maintain per-task state and run summary in DynamoDB, including event-level ingested marker.

## Module Map

- `ingest_pdga_live_results/dynamo_reader.py`
  - Loads candidate `METADATA` items.
  - Expands `division_rounds` into per-division/per-round tasks.
  - Supports explicit IDs and historical backfill GSI path.

- `ingest_pdga_live_results/http_client.py`
  - Builds live-results API URL:
    - `...live_results_fetch_round?TournID=<id>&Division=<div>&Round=<n>`
  - Fetches JSON payload.

- `ingest_pdga_live_results/response_handler.py`
  - Canonicalizes JSON and computes payload hash.
  - Detects empty/sparse payload patterns.
  - Classifies response status.

- `ingest_pdga_live_results/s3_writer.py`
  - Writes raw JSON to:
    - `bronze/pdga/live_results/event_id=<id>/division=<div>/round=<n>/fetch_date=<YYYY-MM-DD>/fetch_ts=<UTC>.json`
  - Writes sidecar metadata JSON:
    - `...fetch_ts=<UTC>.meta.json`

- `ingest_pdga_live_results/dynamo_writer.py`
  - Upserts per-task state item:
    - `pk=EVENT#<id>`
    - `sk=LIVE_RESULTS#DIV#<division>#ROUND#<round>`
  - Writes run summary:
    - `pk=RUN#<run_id>`
    - `sk=LIVE_RESULTS#SUMMARY`
  - Marks event metadata as ingested:
    - `live_results_ingested=true`

- `ingest_pdga_live_results/runner.py`
  - CLI entrypoint and orchestration loop.

## Task Model

Task dataclass:
- `event_id`
- `division`
- `round_number`

Task generation uses `division_rounds` map in METADATA:
- each `division -> max_round` expands to rounds `1..max_round`.

## Runner Modes

### Explicit Event IDs
- `--event-ids 92608,92612`
- optionally `--event-ids-s3-uri s3://bucket/file.csv`

### Historical Backfill
- `--historical-backfill`
- Uses status/date GSI path.
- Requires non-empty `division_rounds`.
- Excludes statuses that are not terminal by default:
  - `Sanctioned`
  - `Event report received; official ratings pending.`
  - `Event complete; waiting for report.`
  - `In progress.`
  - `Errata pending.`
- Excludes events already marked `live_results_ingested=true`.

## Per-Task Processing Lifecycle

1. Build API URL for `(event, division, round)`.
2. Call live-results endpoint.
3. Classify response:
  - `not_found` for HTTP 404
  - `failed` for non-200 or exception
  - `empty` for empty/sparse payload
  - `success` for non-empty 200 payload
4. For `success` and `empty`:
  - Compute canonical payload hash (`content_sha256`).
  - Compare with existing state hash in DynamoDB.
  - If unchanged:
    - skip S3 raw write.
  - If changed:
    - write raw JSON + sidecar metadata to S3.
  - Upsert state item regardless (captures latest status/hash/run).
5. Emit structured task log.
6. Sleep politely before next task.

## Idempotency Design

Per-task semantic hash:
- canonical JSON hash over payload (`sort_keys=True`, compact separators).

Rules:
- Same hash as current state:
  - classify as unchanged; skip S3 raw rewrite.
- Different hash:
  - classify as changed; write new Bronze object and update state pointers.

This prevents duplicate payload storage for unchanged task content.

## Response Classification Logic

`classify_response(status_code, payload, error)` returns:
- `failed` if exception present
- `not_found` if 404
- `failed` if non-200
- `empty` if payload is:
  - `None`, `{}`, `[]`, or
  - dict with empty list under known keys (`results`, `players`, `cards`, `rows`)
- `success` otherwise

## DynamoDB Items Written

### Live Results State Item
Key:
- `pk=EVENT#<event_id>`
- `sk=LIVE_RESULTS#DIV#<division>#ROUND#<round>`

Attributes:
- `event_id`, `division`, `round_number`
- `source_url`
- `fetch_status`
- `content_sha256`
- `latest_s3_json_key`, `latest_s3_meta_key`
- `first_seen_at`, `last_fetched_at`, `last_run_id`

### Run Summary Item
Key:
- `pk=RUN#<run_id>`
- `sk=LIVE_RESULTS#SUMMARY`

Attributes:
- `attempted`
- `success`
- `not_found_404`
- `empty`
- `changed`
- `unchanged`
- `failed`
- `created_at`, `run_id`

### Event Ingested Marker (on METADATA)
On success-only runs (`failed == 0`):
- `live_results_ingested = true`
- `live_results_ingested_at = <UTC>`
- `live_results_ingested_run_id = <run_id>`

## Failure Handling

- Task-level failures are logged and counted; loop continues.
- End-of-run exit code:
  - `0` when `failed == 0`
  - `2` otherwise
- Event-level ingested markers are only written when no task failures occurred.

## Throughput and Progress

Configurable controls:
- `--sleep-base`
- `--sleep-jitter`
- `--progress-every`

Progress logs include:
- attempted/total
- pct complete
- throughput
- ETA
- success/empty/not_found/changed/unchanged/failed

## Practical Notes

- If GSI does not project `live_results_ingested`, reader has fallback behavior for query filter.
- Sidecar metadata contains enough lineage (`run_id`, source URL, hash, fetch time) to replay and audit.
- Historical runs are safe to rerun due to content-hash idempotency.