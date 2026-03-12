# Silver Live Results Code Walkthrough

## Purpose
Explains how `silver_pdga_live_results` transforms Bronze live-results JSON into Silver Parquet outputs.

## Module map

`config.py`
- Loads env config for bucket/table/region/GSI.

`models.py`
- Shared constants:
  - logical PK columns
  - tiebreak columns
  - required lineage columns
  - final-event statuses
- `BronzeRoundSource` dataclass.

`dynamo_io.py`
- Candidate event loading from metadata.
- Event checkpoint loading/writing.
- Live-results state loading.
- Silver run summary writing.

`bronze_io.py`
- Resolves Bronze JSON keys by event/division/round.
- Fallback to S3 listing if key missing.
- Loads payload + optional sidecar metadata.
- Computes source fingerprints.

`normalize.py`
- Parses Bronze payload into `player_rounds` and `player_holes`.
- Applies deterministic player key fallback.
- Enriches rows with event/location metadata.
- Computes `round_date_interp` for both tables using event-span interpolation.
- Computes deterministic `row_hash_sha256`.

`quality.py`
- DQ checks on normalized rows.

`parquet_io.py`
- Writes event parquet via tmp-then-copy.
- Removes stale hole file for round-only events.
- Writes quarantine error reports.

`runner.py`
- CLI orchestration and event loop.
- Selection logic, skip logic, DQ handling, writes, checkpoints, summaries.

## Selection logic in runner

`--run-mode pending_only` (default):
- includes missing checkpoint
- includes `failed`
- excludes `success` with fingerprint
- excludes `dq_failed` by default
- includes `dq_failed` only with `--include-dq-failed-in-pending`

`--run-mode full_check`:
- evaluates all candidates

`--force-events`:
- disables unchanged fingerprint skip for selected events

## Per-event lifecycle

1. Load event live-results state rows.
2. Resolve Bronze sources.
3. Validate expected division/round coverage.
4. Compute event fingerprint.
5. Skip unchanged on matching successful fingerprint (unless `--force-events`).
6. Normalize records:
   - round rows
   - hole rows
   - `round_date_interp` fields
7. Dedup by logical keys + tiebreak.
8. Run DQ checks.
9. On pass:
   - write parquet
   - write success checkpoint
10. On DQ fail:
   - write quarantine report
   - checkpoint status `dq_failed`
11. On exception:
   - checkpoint status `failed`

## Round interpolation details

`normalize.py` computes event `max_round_number` and applies:
- multi-day event: linear span interpolation by round number
- single-day/no-span: `event_start_date`
- missing start date: blank value + fallback method

Fields added to both row types:
- `round_date_interp`
- `round_date_interp_method`
- `round_date_interp_confidence`

## Run metrics

Runner tracks:
- `attempted_events`
- `processed_events`
- `skipped_unchanged_events`
- `failed_events`
- `dq_failed_events`
- `events_without_hole_detail`
- `round_rows_written`
- `hole_rows_written`

Progress emitted every `--progress-every` events.
Run summary written to DynamoDB:
- `pk=RUN#<run_id>`
- `sk=SILVER_LIVE_RESULTS#SUMMARY`