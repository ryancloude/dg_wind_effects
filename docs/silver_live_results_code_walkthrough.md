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
  - Event loop:
    - load candidates
    - resolve Bronze sources
    - fingerprint compare with checkpoint
    - normalize
    - dedup
    - DQ
    - write parquet
    - checkpoint + run summary
  - Emits structured progress logs and summary.

## Processing Lifecycle (Per Event)

1. Load live-results state rows from DynamoDB.
2. Resolve Bronze source payloads (`BronzeRoundSource` list).
3. Validate expected `(division, round)` coverage from `division_rounds`.
4. Compute `event_source_fingerprint`.
5. Compare against checkpoint fingerprint.
6. Skip unchanged unless `--force-events`.
7. Normalize to `player_rounds` + `player_holes`.
8. Deduplicate by logical key with deterministic tie-break columns.
9. Validate DQ checks.
10. On DQ pass:
    - write Parquet
    - write success checkpoint
11. On DQ fail:
    - write quarantine report
    - write `dq_failed` checkpoint
12. On exception:
    - write `failed` checkpoint
13. Continue to next event.

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
- Event-level fingerprint + checkpoint yields cheap incremental reruns.
- Deterministic output keys guarantee idempotency.
- Denormalized Silver tables simplify analytics and Gold modeling.
- DQ gate prevents silent corruption.
- Round-only event support avoids false failures when hole detail is unavailable.

## Known Edge Cases Handled
- Missing `latest_s3_json_key` in state rows
- Missing sidecar metadata object
- Missing `PDGANum`
- Sparse payloads where hole detail is absent
- GSI projection gaps for `live_results_ingested` filter