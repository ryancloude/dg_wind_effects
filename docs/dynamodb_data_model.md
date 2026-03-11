# DynamoDB Data Model Reference

## Table
- Logical name: `pdga-event-index`
- Config env var: `PDGA_DDB_TABLE`
- Primary key:
  - `pk` (partition key)
  - `sk` (sort key)

## Existing Bronze Items

### 1) Event Metadata Item
Key:
- `pk = EVENT#<event_id>`
- `sk = METADATA`

Typical attributes:
- Event basics:
  - `event_id`
  - `source_url`
  - `name`
  - `raw_date_str`
  - `start_date`
  - `end_date`
  - `status_text`
- Division planning:
  - `division_rounds` (map: division -> max rounds)
- Location:
  - `location_raw` (or `raw_location`)
  - `city`
  - `state`
  - `country`
- Parsing:
  - `parse_warnings`
  - `parser_version`
- Hashes:
  - `content_sha256`
  - `idempotency_sha256`
  - `raw_html_sha256`
- S3 pointers:
  - `latest_s3_html_key`
  - `latest_s3_meta_key`
- Timestamps:
  - `first_seen_at`
  - `last_fetched_at`
- Live-results ingest marker:
  - `live_results_ingested`
  - `live_results_ingested_at`
  - `live_results_ingested_run_id`

### 2) Live Results State Item (Per Event/Division/Round)
Key:
- `pk = EVENT#<event_id>`
- `sk = LIVE_RESULTS#DIV#<division>#ROUND#<round_number>`

Typical attributes:
- `event_id`
- `division`
- `round_number`
- `source_url`
- `fetch_status`
- `content_sha256`
- `latest_s3_json_key`
- `latest_s3_meta_key`
- `first_seen_at`
- `last_fetched_at`
- `last_run_id`

### 3) Live Results Bronze Run Summary
Key:
- `pk = RUN#<run_id>`
- `sk = LIVE_RESULTS#SUMMARY`

Typical attributes:
- `run_id`
- `created_at`
- `attempted`
- `success`
- `not_found_404`
- `empty`
- `changed`
- `unchanged`
- `failed`

## Silver Items (Added by Silver Layer)

### 4) Silver Event Checkpoint
Key:
- `pk = PIPELINE#SILVER_LIVE_RESULTS`
- `sk = EVENT#<event_id>`

Core attributes:
- `event_id`
- `pipeline = silver_live_results`
- `status`:
  - `success`
  - `dq_failed`
  - `failed`
- `event_source_fingerprint`
- `last_run_id`
- `updated_at`

Common success attributes:
- `event_year`
- `round_rows`
- `hole_rows`
- `has_hole_detail`
- `round_s3_key`
- `hole_s3_key` (blank when no hole detail)

Common dq_failed attributes:
- `error_count`
- `errors` (truncated list)
- `quarantine_key`

Common failed attributes:
- `error_message`

### 5) Silver Run Summary
Key:
- `pk = RUN#<run_id>`
- `sk = SILVER_LIVE_RESULTS#SUMMARY`

Typical attributes:
- `run_id`
- `created_at`
- `attempted_events`
- `processed_events`
- `skipped_unchanged_events`
- `failed_events`
- `dq_failed_events`
- `events_without_hole_detail`
- `round_rows_written`
- `hole_rows_written`

## GSI
Existing Bronze/Silver selection path uses:
- Name: `gsi_status_end_date`
- PK: `status_text`
- SK: `end_date`
