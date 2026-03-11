# Silver Live Results Code Walkthrough

## Purpose
This document explains how `silver_pdga_live_results` works end-to-end.

## Module Map

- `silver_pdga_live_results/config.py`
  - Loads env configuration (`PDGA_S3_BUCKET`, `PDGA_DDB_TABLE`, `AWS_REGION`, `PDGA_DDB_STATUS_END_DATE_GSI`).

- `silver_pdga_live_results/models.py`
  - Shared constants:
    - final statuses
    - logical PK columns
    - tie-break columns
    - required lineage columns
  - Dataclass:
    - `BronzeRoundSource`

- `silver_pdga_live_results/dynamo_io.py`
  - Loads candidate events from DynamoDB (`METADATA`).
  - Loads Silver checkpoints (`PIPELINE#SILVER_LIVE_RESULTS`).
  - Loads live-results state rows (`LIVE_RESULTS#DIV#...#ROUND#...`).
  - Reads/writes Silver event checkpoints.
  - Writes Silver run summaries.

- `silver_pdga_live_results/bronze_io.py`
  - Resolves Bronze JSON source keys from state rows.
  - Fallbacks to S3 prefix listing when state key is blank.
  - Loads payload and optional sidecar metadata.
  - Computes source hash/timestamps when missing.
  - Computes event fingerprint for incremental skip logic.

- `silver_pdga_live_results/normalize.py`
  - Converts Bronze payloads to normalized row dicts:
    - `player_rounds`
    - `player_holes`
  - Applies deterministic player key fallback:
    - PDGA -> ResultID -> NameHash
  - Enriches with event location fields from METADATA.
  - Builds row hashes.

- `silver_pdga_live_results/quality.py`
  - Runs DQ checks before publish:
    - duplicate keys
    - lineage completeness
    - parent-child integrity
    - domain checks
    - division collision guard

- `silver_pdga_live_results/parquet_io.py`
  - Writes Parquet bytes with `pyarrow`.
  - Performs tmp write -> final copy for deterministic keys.
  - Deletes stale hole parquet when event has no hole detail.
  - Writes quarantine report JSON.

- `silver_pdga_live_results/runner.py`
  - CLI entrypoint and orchestrator.
  - Handles run mode selection (`pending_only` vs `full_check`).
  - Event loop:
    - load candidates
    - filter pending (run mode dependent)
    - resolve Bronze sources
    - fingerprint compare with checkpoint
    - normalize
    - dedup
    - DQ
    - write parquet
    - checkpoint + run summary
  - Emits structured progress logs and summary.

## Run Modes

### pending_only (default)
Used when `--event-ids` is not provided.

Selection logic:
- include event if checkpoint missing
- include event if checkpoint status is `failed` or `dq_failed` (or blank)
- include event if checkpoint status is `success` but fingerprint is blank
- exclude event if checkpoint status is `success` with valid fingerprint

This is the fast resume mode for stable/final Bronze data.

### full_check
Used when you want all candidates evaluated again.
- Still uses per-event fingerprint skip in-processing.
- More expensive than `pending_only`.

### event-ids override
If `--event-ids` is provided, explicit IDs are used directly regardless of run mode filtering.

## Processing Lifecycle (Per Selected Event)

1. Load live-results state rows from DynamoDB.
2. Resolve Bronze source payloads (`BronzeRoundSource` list).
3. Validate expected `(division, round)` coverage from `division_rounds`.
4. Compute `event_source_fingerprint`.
5. Skip as unchanged only when:
   - checkpoint status is `success`
   - fingerprint matches
   - `--force-events` is not set
6. Normalize to `player_rounds` + `player_holes`.
7. Deduplicate by logical key with deterministic tie-break columns.
8. Validate DQ checks.
9. On DQ pass:
   - write Parquet
   - write success checkpoint
10. On DQ fail:
    - write quarantine report
    - write `dq_failed` checkpoint
11. On exception:
    - write `failed` checkpoint
12. Continue to next event.

## Run-Level Metrics (`RunStats`)
- `attempted_events`
- `processed_events`
- `skipped_unchanged_events`
- `failed_events`
- `dq_failed_events`
- `events_without_hole_detail`
- `round_rows_written`
- `hole_rows_written`

These are printed and stored in DynamoDB run summary.

## Why This Design
- `pending_only` keeps reruns fast for finalized Bronze datasets.
- Checkpoint-driven selection avoids wasteful full event rechecks.
- Fingerprint checks preserve idempotency and replayability.
- Deterministic output keys guarantee overwrite-safe writes.
- DQ gate prevents silent corruption.
- Round-only event support avoids false failures when hole detail is unavailable.

## Known Edge Cases Handled
- Missing `latest_s3_json_key` in state rows
- Missing sidecar metadata object
- Missing `PDGANum`
- Sparse payloads where hole detail is absent
- GSI projection gaps for `live_results_ingested` filter
- Retry path where failed/dq_failed events should not be skipped