```md
# DG Wind Effects (`dg_wind_effects`)

## Project End Goal
Build an AWS-native, production-style data platform that quantifies disc golf wind impact on scoring by:
- ingesting PDGA event/live scoring data and weather data on schedules and backfills,
- storing replayable Bronze raw data in S3,
- producing normalized Silver datasets and analytics-ready Gold features,
- aligning weather with round/hole timing,
- estimating “strokes added by wind” by course/layout/division/conditions.

## Current Status

Implemented:
- Bronze ingestion:
  - `ingest_pdga_event_pages`
  - `ingest_pdga_live_results`
  - `ingest_weather_observations`
- Silver normalization:
  - `silver_pdga_live_results`
  - `silver_weather_observations`
- Checkpointing/state:
  - DynamoDB state and run summaries for event pages, live results, silver live results, and weather ingestion
- Containerized runners:
  - Event pages, live results, silver live results, weather observations

Left to build:
- Silver weather normalization layer (`silver_weather_observations`)
- Join-ready weather-to-round/hole outputs in Silver
- Gold fact/dim/feature models for wind effect analysis
- Baseline expected scoring model + wind impact estimation logic
- Final analytics outputs (notebook/dashboard + publication-ready summary tables)

---

## Current Data Flow

### 1) Bronze Event Metadata (`ingest_pdga_event_pages`)
Purpose:
- Discover and track PDGA event-level metadata and structure.

Input:
- PDGA event HTML pages.

Output:
- S3 raw HTML + sidecar metadata.
- DynamoDB `EVENT#<id>/METADATA` items with event dates, location fields (`city/state/country`), division/round structure, parser hashes/status.

Common commands:
```powershell
python -m ingest_pdga_event_pages.runner --incremental
python -m ingest_pdga_event_pages.runner --ids 90008,90009
python -m ingest_pdga_event_pages.runner --range 90000-90100
```

---

### 2) Bronze Live Results (`ingest_pdga_live_results`)
Purpose:
- Pull per-event/per-division/per-round live scoring payloads.

Input:
- `METADATA` records (`division_rounds`) from DynamoDB.

Output:
- S3 raw live-results JSON + sidecar metadata.
- DynamoDB per-round state items (`LIVE_RESULTS#DIV#...#ROUND#...`).
- Run summary item (`RUN#<run_id>/LIVE_RESULTS#SUMMARY`).
- Event marker `live_results_ingested=true` when successful.

Common commands:
```powershell
python -m ingest_pdga_live_results.runner --historical-backfill
python -m ingest_pdga_live_results.runner --event-ids 90008,90009
python -m ingest_pdga_live_results.runner --historical-backfill --dry-run
```

---

### 3) Silver Live Results (`silver_pdga_live_results`)
Purpose:
- Normalize Bronze live-results data into stable analysis tables.

Input:
- Bronze live-results S3 payloads + event metadata/state.

Output:
- `silver/pdga/live_results/player_rounds/.../player_rounds.parquet`
- `silver/pdga/live_results/player_holes/.../player_holes.parquet`
- Quarantine reports for DQ failures.
- DynamoDB Silver checkpoints (`PIPELINE#SILVER_LIVE_RESULTS`) + run summary.
- Includes timing fields like `round_date_interp` and `tee_time_join_ts` used by weather ingestion.

Common commands:
```powershell
python -m silver_pdga_live_results.runner --run-mode pending_only --progress-every 25
python -m silver_pdga_live_results.runner --run-mode full_check --force-events
python -m silver_pdga_live_results.runner --event-ids 90008,90009 --force-events
```

---

### 4) Bronze Weather Observations (`ingest_weather_observations`)
Purpose:
- Pull historical weather observations aligned to event timing/location.

Input:
- Silver live-results checkpoint + `player_rounds` parquet (`tee_time_join_ts`).
- Event location fields (`city/state/country`) and/or coordinates.

Processing:
- Resolve coordinates by priority:
  1. direct metadata lat/lon
  2. cached geocode in DynamoDB
  3. Open-Meteo geocoding fallback, then cache
- Build round-level weather windows from tee-time-derived local dates.
- Fetch Open-Meteo archive weather payloads.
- Track idempotency via request/content/timing fingerprints.

Output:
- S3 raw weather JSON + sidecar metadata.
- DynamoDB weather round state (`WEATHER_OBS#ROUND#...`), weather event summary (`WEATHER_OBS#SUMMARY`), geocode cache, and weather run summary.

Common commands:
```powershell
python -m ingest_weather_observations.runner --incremental --progress-every 25
python -m ingest_weather_observations.runner --event-ids 90008,90009 --dry-run
python -m ingest_weather_observations.runner --historical-backfill --progress-every 25
```
...

## Silver runner (`silver_weather_observations`)

What Silver weather does:
- Selects weather-ingested event candidates from DynamoDB
- Hydrates Bronze weather round sources from S3
- Normalizes hourly observations into canonical rows
- Enforces DQ rules and writes quarantine reports
- Writes event-level parquet outputs
- Tracks event checkpoints + run summaries in DynamoDB

Run examples:

```powershell
python -m silver_weather_observations.runner --dry-run --log-level INFO
python -m silver_weather_observations.runner --run-mode pending_only --progress-every 25
python -m silver_weather_observations.runner --run-mode full_check --force-events --progress-every 25
python -m silver_weather_observations.runner --event-ids 90008,90009 --force-events
---

## Typical End-to-End Run Sequence (Current)

```powershell
python -m ingest_pdga_event_pages.runner --incremental
python -m ingest_pdga_live_results.runner --historical-backfill
python -m silver_pdga_live_results.runner --run-mode pending_only --progress-every 25
python -m ingest_weather_observations.runner --incremental --progress-every 25
```

---

## Testing

All tests:
```powershell
python -m pytest -v
```

Weather ingestion tests:
```powershell
python -m pytest tests/ingest_weather_observations -v
```
```