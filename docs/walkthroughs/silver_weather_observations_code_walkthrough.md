# Silver Weather Observations - Code Walkthrough

## Purpose
`silver_weather_observations` transforms Bronze weather payloads into a normalized hourly Silver table for analytics and downstream weather-score joins.

It does six things:
1. Select candidate events from weather event summaries.
2. Load Bronze weather round sources from DynamoDB state + S3 raw payloads.
3. Compute deterministic event source fingerprint.
4. Normalize Open-Meteo hourly arrays into canonical row format.
5. Enforce data quality checks and quarantine on failure.
6. Write event-level parquet output and checkpoint/run summaries in DynamoDB.

## Module Map

- `silver_weather_observations/config.py`
  - Loads `PDGA_S3_BUCKET`, `PDGA_DDB_TABLE`, optional `AWS_REGION`.

- `silver_weather_observations/models.py`
  - Defines Bronze source dataclass and key/lineage column constants.

- `silver_weather_observations/dynamo_io.py`
  - Loads weather event summaries and weather round state items.
  - Reads/writes Silver weather event checkpoints and run summaries.
  - Reads event `METADATA`.

- `silver_weather_observations/bronze_io.py`
  - Hydrates `BronzeWeatherRoundSource` from weather state items + S3 JSON/meta.
  - Computes deterministic event source fingerprint.

- `silver_weather_observations/normalize.py`
  - Converts hourly arrays (`time`, `wind_speed_10m`, etc.) into row records.
  - Builds deterministic `weather_obs_pk`.
  - Preserves lineage columns.

- `silver_weather_observations/quality.py`
  - Validates not-null, uniqueness, numeric ranges, and lineage fields.
  - Returns structured DQ error list.

- `silver_weather_observations/parquet_io.py`
  - Writes event-level Silver parquet output.
  - Writes DQ quarantine report JSON.

- `silver_weather_observations/runner.py`
  - Orchestrates run modes, processing loop, checkpoint writes, and progress logs.

## Output Contract

Primary output:
- `silver/weather/observations_hourly/event_year=<YYYY>/event_id=<id>/observations_hourly.parquet`

Quarantine output:
- `silver/weather/quarantine/observations_hourly/event_year=<YYYY>/event_id=<id>/run_id=<run_id>/dq_errors.json`

## Checkpoint Contract

Event checkpoint:
- `pk=PIPELINE#SILVER_WEATHER_OBSERVATIONS`
- `sk=EVENT#<event_id>`

Run summary:
- `pk=RUN#<run_id>`
- `sk=SILVER_WEATHER_OBSERVATIONS#SUMMARY`

Checkpoint status values:
- `success`
- `dq_failed`
- `failed`

## Runner Modes

- `pending_only` (default):
  - includes missing checkpoints
  - includes `failed`
  - excludes `success` with fingerprint
  - excludes `dq_failed` by default
  - include `dq_failed` with `--include-dq-failed-in-pending`
- `full_check`:
  - evaluates all candidate events
- `--force-events`:
  - bypass unchanged fingerprint skip

## Idempotency

Event-level unchanged skip requires:
- prior checkpoint status = `success`
- checkpoint `event_source_fingerprint` equals freshly computed fingerprint

When unchanged:
- event is skipped
- no parquet rewrite

## Failure Behavior

DQ failures:
- event marked `dq_failed`
- quarantine report written
- processing continues to next event

Unexpected exceptions:
- event marked `failed`
- processing continues

Exit code:
- `0` if no failed events
- `2` otherwise