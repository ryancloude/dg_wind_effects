# Silver Weather Enriched - Code Walkthrough

## Purpose
`silver_weather_enriched` enriches the existing Silver scoring tables with weather columns while preserving original row grain and keys.

Outputs:
- `player_rounds_weather` (same grain as `player_rounds`)
- `player_holes_weather` (same grain as `player_holes`)

## Inputs
- Silver live round table (`round_s3_key`) from `PIPELINE#SILVER_LIVE_RESULTS`
- Silver live hole table (`hole_s3_key`) from `PIPELINE#SILVER_LIVE_RESULTS`
- Silver weather observations table (`observations_s3_key`) from `PIPELINE#SILVER_WEATHER_OBSERVATIONS`

Only events with success checkpoints in both upstream Silver pipelines are candidates.

## Module map
- `models.py`:
  - pipeline constants
  - appended weather columns
  - join policy version
- `time_align.py`:
  - deterministic UTC hour bucketing from round/hole timestamps
- `join.py`:
  - weather lookup build with deterministic tie-break
  - round/hole enrichment functions
  - event source fingerprint
- `silver_io.py`:
  - parquet loading helpers + source fingerprint helper
- `quality.py`:
  - row count + PK preservation checks
  - required weather-column presence checks
- `parquet_io.py`:
  - writes enriched round/hole parquet outputs
  - writes quarantine report
- `dynamo_io.py`:
  - candidate loading from upstream checkpoints
  - enriched checkpoint + run summary read/write
- `runner.py`:
  - orchestration + run modes + progress + checkpointing

## Join policy
Round rows:
- observation hour from `tee_time_join_ts` (UTC hour floor)
- fallback to `round_date_interp` at deterministic noon UTC

Hole rows:
- observation hour from `hole_time_est_ts`
- fallback to `tee_time_join_ts`
- fallback to `round_date_interp` noon UTC

Weather lookup key:
- `(event_id, round_number, observation_hour_utc)`

Collision tie-break:
- newest `source_fetched_at_utc`
- then lexicographic `source_json_key`
- then `weather_obs_pk`

## Idempotency and incremental
Event fingerprint is deterministic over:
- input round rows projection
- input hole rows projection
- input weather rows projection
- `JOIN_POLICY_VERSION`

Checkpoint item:
- `pk=PIPELINE#SILVER_WEATHER_ENRICHED`
- `sk=EVENT#<event_id>`

Unchanged skip rule:
- checkpoint status `success`
- stored `event_source_fingerprint` == newly computed fingerprint

## DQ behavior
Rules:
- output row counts equal input row counts
- PK sets preserved for round/hole tables
- all expected weather columns present
- `wx_weather_missing_flag` non-null

On DQ fail:
- checkpoint status `dq_failed`
- quarantine report written

## Output paths
- `silver/pdga/live_results_enriched/player_rounds_weather/event_year=<YYYY>/tourn_id=<event_id>/player_rounds_weather.parquet`
- `silver/pdga/live_results_enriched/player_holes_weather/event_year=<YYYY>/tourn_id=<event_id>/player_holes_weather.parquet`
- `silver/pdga/live_results_enriched/quarantine/event_year=<YYYY>/tourn_id=<event_id>/run_id=<run_id>/dq_errors.json`

## Run modes
- `pending_only` (default)
- `full_check`
- `--include-dq-failed-in-pending`
- `--force-events`
- `--dry-run`