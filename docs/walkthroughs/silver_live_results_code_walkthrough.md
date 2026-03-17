# Silver Live Results Code Walkthrough

## Purpose
This document explains how `silver_pdga_live_results` builds normalized Silver outputs from finalized PDGA Bronze live-results JSON.

## Module Map

- `silver_pdga_live_results/config.py`
  - Loads runtime config:
    - `PDGA_S3_BUCKET`
    - `PDGA_DDB_TABLE`
    - `AWS_REGION`
    - `PDGA_DDB_STATUS_END_DATE_GSI`

- `silver_pdga_live_results/models.py`
  - Shared constants:
    - final-status filters
    - logical PK columns
    - tie-break columns
    - required lineage columns
  - `BronzeRoundSource` dataclass

- `silver_pdga_live_results/dynamo_io.py`
  - Loads candidate `METADATA` events
  - Loads live-results state rows (`LIVE_RESULTS#DIV#...#ROUND#...`)
  - Reads/writes Silver event checkpoints
  - Writes Silver run summary

- `silver_pdga_live_results/bronze_io.py`
  - Resolves Bronze source keys for event/division/round
  - Loads payload + sidecar metadata
  - Computes payload/content fingerprints
  - Builds event source fingerprint for incremental skip behavior

- `silver_pdga_live_results/normalize.py`
  - Transforms Bronze payloads into:
    - `player_rounds`
    - `player_holes`
  - Enriches event location fields from `METADATA`
  - Computes deterministic `player_key` fallback:
    - PDGA -> ResultID -> NAMEHASH
  - Computes round-date interpolation (`round_date_interp*`)
  - Computes tee-time estimation fields (`tee_time_est*`, lag fields)
  - Computes weather-join tee timestamp (`tee_time_join_*`)
  - Uses fixed 4-hour round duration (`round_duration_est_minutes=240`)
  - Computes estimated hole windows (`hole_start_est_ts`, `hole_end_est_ts`)
  - Computes row-level deterministic hash (`row_hash_sha256`)

- `silver_pdga_live_results/quality.py`
  - DQ checks:
    - duplicate logical keys
    - required lineage fields
    - hole-parent integrity
    - domain checks
    - division collision guard

- `silver_pdga_live_results/parquet_io.py`
  - Writes parquet bytes with `pyarrow`
  - Uses tmp-write then deterministic final copy
  - Removes stale `player_holes.parquet` when event has no hole detail
  - Writes DQ quarantine reports

- `silver_pdga_live_results/runner.py`
  - CLI entrypoint and orchestration
  - Candidate selection, normalization, dedup, DQ, write, checkpointing
  - Progress + summary logging

## Runner CLI and Event Selection

### Arguments
- `--event-ids` explicit event list
- `--run-mode`:
  - `pending_only` (default)
  - `full_check`
- `--include-dq-failed-in-pending`
- `--force-events`
- `--dry-run`
- `--progress-every`
- `--log-level`
- `--bucket`, `--ddb-table` overrides

### Selection behavior
If `--event-ids` is provided:
- process those IDs directly (subject to in-loop skip logic unless forced)

If `--run-mode pending_only`:
- include events with no checkpoint
- include events with `failed`
- include events with blank/legacy status
- include `success` only when fingerprint is missing
- exclude `success` with valid fingerprint
- exclude `dq_failed` unless `--include-dq-failed-in-pending` is set

If `--run-mode full_check`:
- evaluate all candidate events

### In-loop unchanged skip
For selected events, skip only when:
- checkpoint status is `success`
- checkpoint fingerprint equals current event fingerprint
- `--force-events` is not set

## Per-Event Processing Lifecycle

1. Load event state rows from DynamoDB.
2. Resolve Bronze round sources from S3 + state rows.
3. Validate expected division/round coverage from `division_rounds`.
4. Compute event source fingerprint.
5. Optionally skip unchanged (unless forced).
6. Normalize payloads into round/hole rows.
7. Deduplicate rows by logical keys + tie-break columns.
8. Validate DQ checks.
9. On DQ pass:
   - write parquet outputs
   - write success checkpoint
10. On DQ fail:
   - write quarantine report
   - write `dq_failed` checkpoint
11. On exception:
   - write `failed` checkpoint

## Normalization Details

### Round interpolation
`normalize.py` derives `max_round_number` and computes:
- `round_date_interp`
- `round_date_interp_method`
- `round_date_interp_confidence`

Multi-day events always use linear event-span interpolation.

### Tee-time estimation
`normalize.py` computes:
- `tee_time_est_ts`
- `tee_time_est_method`
- `tee_time_est_confidence`
- `lag_minutes_used`
- `lag_bucket_used`
- `lag_sample_size` (rounds only)
- `round_duration_est_minutes`

Methods:
- `raw_tee_time`
- `raw_tee_time_no_score_ts`
- `score_minus_global_median_lag`
- `missing_inputs`

Current policy:
- `round_duration_est_minutes` is fixed to `240` for all rows.

### Weather-join tee timestamp
`normalize.py` computes:
- `tee_time_join_ts`
- `tee_time_join_method`
- `tee_time_join_confidence`

Priority:
1. `round_date_interp_plus_raw_tee`
2. `fallback_score_based`
3. `round_date_interp_noon_fallback`
4. `event_start_noon_fallback`
5. `missing_inputs`

All values are local-time strings; Silver does not apply timezone conversion.

### Hole-time estimation
`normalize.py` computes:
- `hole_start_est_ts`
- `hole_end_est_ts`
- `hole_time_est_method`
- `hole_time_est_confidence`

Method:
- uniform split of fixed 240-minute duration across scored holes
- if missing round timing inputs, hole estimates remain blank with low confidence

## Dedup Strategy

### player_rounds
- Dedup key: `(tourn_id, round_number, player_key)`

### player_holes
- Dedup key: `(tourn_id, round_number, hole_number, player_key)`

Tie-break order:
1. `source_fetched_at_utc`
2. `scorecard_updated_at_ts`
3. `update_date_ts`
4. `source_json_key`

## DQ Strategy

Checks:
- duplicate round/hole keys
- required lineage fields present
- hole rows have round parent
- `round_number >= 1`
- `hole_number >= 1`
- `hole_number <= layout_holes` when known
- no event/player division collisions

Failure behavior:
- event marked `dq_failed`
- quarantine report written
- final partition not updated for that run

## S3 Writes and Event-Level Idempotency

Final event-level deterministic keys:
- `silver/pdga/live_results/player_rounds/event_year=<YYYY>/tourn_id=<event_id>/player_rounds.parquet`
- `silver/pdga/live_results/player_holes/event_year=<YYYY>/tourn_id=<event_id>/player_holes.parquet`

Write pattern:
- write temp parquet under `_tmp/run_id=...`
- copy to final deterministic key
- delete temp object
- for round-only events, delete stale hole parquet if present

## Checkpoint and Summary Records

Event checkpoint key:
- `pk=PIPELINE#SILVER_LIVE_RESULTS`
- `sk=EVENT#<event_id>`

Checkpoint statuses:
- `success`
- `dq_failed`
- `failed`

Run summary key:
- `pk=RUN#<run_id>`
- `sk=SILVER_LIVE_RESULTS#SUMMARY`

## Run Metrics (`RunStats`)

- `attempted_events`
- `processed_events`
- `skipped_unchanged_events`
- `failed_events`
- `dq_failed_events`
- `events_without_hole_detail`
- `round_rows_written`
- `hole_rows_written`

Progress is emitted every `--progress-every` attempts and final summary is always emitted at run end.

## Operational Notes

- `pending_only` is optimized for fast recurring runs.
- `full_check --force-events` is the standard choice for full Silver reprocessing.
- Round-only events are valid and should not fail the run.
- Keep schema docs, normalization fields, and Athena table columns synchronized after any field additions.