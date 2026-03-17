# Ingest Weather Observations - Code Walkthrough

## Purpose
`ingest_weather_observations` is the Bronze weather ingestion pipeline for this project.

It does eight things:
1. Select candidate events from successful Silver live-results checkpoints.
2. Resolve event coordinates (metadata first, then deterministic geocode cache, then geocoding API fallback).
3. Read Silver `player_rounds` parquet and derive per-round play dates from `tee_time_join_ts`.
4. Build deterministic Open-Meteo archive requests per event/round.
5. Fetch weather payloads (hourly + daily sunrise/sunset).
6. Compute content/request/timing fingerprints for idempotent reruns.
7. Write changed raw payloads to Bronze S3 with sidecar metadata.
8. Maintain per-round state, event summary, geocode cache/resolution, and run summary in DynamoDB.

## Module Map

- `ingest_weather_observations/config.py`
  - Loads `PDGA_S3_BUCKET`, `PDGA_DDB_TABLE`, `AWS_REGION`, and optional GSI env.
  - Mirrors existing pipeline config style.

- `ingest_weather_observations/models.py`
  - Defines `GeoPoint`, `WeatherFetchWindow`, `WeatherObservationTask`.
  - Centralizes deterministic task identity (`provider`, `source_id`).

- `ingest_weather_observations/location.py`
  - Extracts direct coordinates if present in event metadata.
  - Supports multiple key variants (`latitude/longitude`, `lat/lon`, nested `location`).

- `ingest_weather_observations/geocode.py`
  - Builds deterministic geocode query from `city/state/country` or `location_raw`.
  - Computes query fingerprint for cache keying.
  - Ranks and selects best result from geocoding response.

- `ingest_weather_observations/http_client.py`
  - Builds retry-capable `requests` session.
  - Fetches Open-Meteo archive JSON.
  - Fetches Open-Meteo geocoding JSON.

- `ingest_weather_observations/windowing.py`
  - Derives local play dates from `tee_time_join_ts`.
  - Builds round date overrides and fetch windows.
  - Expands sunrise/sunset bounds and filters hourly rows to daylight overlap.

- `ingest_weather_observations/silver_reader.py`
  - Reads Silver `player_rounds` parquet from S3.
  - Computes deterministic `tee_time_source_fingerprint`.

- `ingest_weather_observations/response_handler.py`
  - Computes canonical payload hash.
  - Expands hourly/daily arrays into row records.
  - Produces daylight-hour subset stats.

- `ingest_weather_observations/s3_writer.py`
  - Writes raw weather JSON to Bronze S3.
  - Writes sidecar metadata with request lineage/fingerprints.

- `ingest_weather_observations/dynamo_reader.py`
  - Loads candidate events from Silver success checkpoints.
  - Loads event weather summary for incremental skip decisions.
  - Loads cached geocode by query fingerprint.

- `ingest_weather_observations/dynamo_writer.py`
  - Upserts weather per-round state item.
  - Upserts event weather summary and run summary.
  - Writes geocode cache item and per-event geocode resolution item.

- `ingest_weather_observations/runner.py`
  - CLI entrypoint and orchestration loop.
  - Coordinates selection, geocoding, weather fetches, writes, and progress logging.

## CLI and Run Modes

Primary flags:
- `--event-ids 90008,90009`
- `--incremental`
- `--historical-backfill`
- `--dry-run`
- `--force-events`
- `--round-padding-days`
- `--progress-every`
- `--timeout`

Behavior:
- If neither `--incremental` nor `--historical-backfill` nor `--event-ids` is passed, runner defaults to incremental mode.
- `--event-ids` and `--historical-backfill` are mutually exclusive.

## Candidate Event Selection

Source:
- DynamoDB Silver checkpoint items:
  - `pk=PIPELINE#SILVER_LIVE_RESULTS`
  - `sk=EVENT#<event_id>`

Filter:
- `status == success`
- `round_s3_key` exists

Hydration:
- For each candidate event, load event `METADATA` item:
  - `pk=EVENT#<event_id>`
  - `sk=METADATA`

## Coordinate Resolution Strategy

Order:
1. Direct metadata coordinates (`location.py` extraction).
2. Geocode cache lookup by query fingerprint:
  - `pk=GEO#QUERY#<fingerprint>`
  - `sk=WEATHER_GEO#CACHE`
3. Open-Meteo geocoding API lookup + deterministic best-result selection.

On successful geocode API resolution:
- cache entry is written for reuse.
- per-event resolution item is written:
  - `pk=EVENT#<event_id>`
  - `sk=WEATHER_GEO#RESOLVED`

## Time Window Strategy

Primary timing source:
- Silver `player_rounds` `tee_time_join_ts` (assumed local event wall-clock).

Per-event derivations:
- Round-level date overrides from tee-time date mode.
- Round play-date sets for filtering.
- Fallback to event `start_date/end_date` inference when tee-time coverage is missing.

Per-round request window:
- Built from resolved round date and `--round-padding-days`.
- Query date range is deterministic.

Daylight filtering:
- Uses Open-Meteo `daily.sunrise/sunset`.
- Hour row interval treated as `[hour_start, hour_start + 1h)`.
- Keep hour when it overlaps daylight interval.

## Open-Meteo Request Contract

Archive endpoint:
- `https://archive-api.open-meteo.com/v1/archive`

Key request params:
- `latitude`
- `longitude`
- `start_date`
- `end_date`
- `hourly` wind/temperature/humidity/pressure/precip fields
- `timezone=UTC` (in request object contract)

Geocoding endpoint:
- `https://geocoding-api.open-meteo.com/v1/search`

Geocode params:
- `name`
- `count`
- `language=en`
- `format=json`
- optional `countryCode`

## Idempotency Design

Three checks drive changed vs unchanged:
1. `request_fingerprint` (URL + params canonical hash)
2. `content_sha256` (canonical payload hash)
3. `tee_time_source_fingerprint` (Silver timing source hash)

Unchanged rule:
- if all three match previous state item, do not rewrite S3 raw payload.
- still update state metadata/status for observability.

## Bronze S3 Contract

Raw payload key:
- `bronze/weather/observations/provider=<provider>/event_id=<id>/round=<n>/source_id=<source_id>/fetch_date=<YYYY-MM-DD>/fetch_ts=<UTC_SAFE>.json`

Sidecar key:
- same prefix, `.meta.json`

Sidecar metadata includes:
- event + round identity
- source URL
- request params + request fingerprint
- tee-time source fingerprint
- fetch timestamps
- payload hash + content length
- daylight hour count
- run id
- S3 lineage pointer

## DynamoDB Items Written

### Weather Per-Round State
Key:
- `pk=EVENT#<event_id>`
- `sk=WEATHER_OBS#ROUND#<round_number>#PROV#<provider>#SRC#<source_id>`

Typical attributes:
- `event_id`, `round_number`, `provider`, `source_id`
- `source_url`
- `request_fingerprint`
- `tee_time_source_fingerprint`
- `fetch_status` (`success` or `unchanged`)
- `content_sha256`
- `latest_s3_json_key`, `latest_s3_meta_key`
- `first_seen_at`, `last_fetched_at`, `last_run_id`

### Event Weather Summary
Key:
- `pk=EVENT#<event_id>`
- `sk=WEATHER_OBS#SUMMARY`

Typical attributes:
- per-event task counts and daylight stats
- `last_silver_checkpoint_updated_at`
- `last_run_id`, `updated_at`

### Geocode Cache
Key:
- `pk=GEO#QUERY#<query_fingerprint>`
- `sk=WEATHER_GEO#CACHE`

Typical attributes:
- `query_text`
- `latitude`, `longitude`
- source place fields (`source_name`, `source_admin1`, `source_country`, `source_country_code`)
- `updated_at`, `last_run_id`

### Event Geocode Resolution
Key:
- `pk=EVENT#<event_id>`
- `sk=WEATHER_GEO#RESOLVED`

Typical attributes:
- `query_fingerprint`, `query_text`
- `latitude`, `longitude`
- `resolution_source`
- `first_seen_at`, `updated_at`, `last_run_id`

### Weather Run Summary
Key:
- `pk=RUN#<run_id>`
- `sk=WEATHER_OBS#SUMMARY`

Typical attributes:
- attempted/processed/skipped/failed event counts
- round task changed/unchanged/failed counts
- daylight totals
- geocode source counters
- `created_at`, `run_id`

## Incremental Logic

Skip condition:
- Load existing `WEATHER_OBS#SUMMARY` event item.
- Compare stored `last_silver_checkpoint_updated_at` with current Silver checkpoint `updated_at`.
- If equal and not `--force-events`, skip event in incremental mode.

This ties weather recompute to Silver timing updates.

## Failure Handling

- Per-round errors are isolated and counted; processing continues.
- Per-event exceptions are logged and counted; loop continues.
- Exit code:
  - `0` if no event or round failures
  - `2` otherwise

## Observability

Structured logs:
- run plan
- per-event processed/skipped
- per-round failure context (`event_id`, `round_number`, `run_id`)
- periodic progress summary
- final run summary

Counters tracked:
- event-level throughput/skip/failure
- round-level changed/unchanged/failure
- daylight-hour totals
- coordinate source (`metadata`, `cache`, `geocode_api`)