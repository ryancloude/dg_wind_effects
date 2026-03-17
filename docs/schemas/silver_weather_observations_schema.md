# Silver Weather Observations Schema

## Purpose
`silver_weather_observations` produces a normalized, analysis-ready hourly weather table from Bronze Open-Meteo archive payloads, aligned at event/round/source/hour grain and carrying full lineage for reproducibility.

Primary output path:
- `silver/weather/observations_hourly/event_year=<YYYY>/event_id=<event_id>/observations_hourly.parquet`

## Grain
One row per:
- `event_id`
- `round_number`
- `provider`
- `source_id`
- `observation_hour_utc`

Dedup primary key logic uses:
- `weather_obs_pk` = deterministic hash of `(event_id, round_number, provider, source_id, observation_hour_utc)`

---

## Column Dictionary

### Keys and identity
- `weather_obs_pk` (`string`, required)
  - Deterministic row identifier.
- `event_id` (`int`, required)
  - PDGA event id.
- `event_year` (`int`, required)
  - Event year partition field.
- `round_number` (`int`, required)
  - Event round number.
- `provider` (`string`, required)
  - Weather provider identifier (currently `open_meteo_archive`).
- `source_id` (`string`, required)
  - Source/location identifier (for example `GRID#<lat>_<lon>`).

### Time fields
- `observation_ts_utc` (`string`, required)
  - Observation timestamp in UTC ISO format.
- `observation_hour_utc` (`string`, required)
  - Hour bucket timestamp in UTC ISO format (top of hour).

### Weather measures
- `wind_speed_mps` (`double`, nullable)
  - Wind speed at 10m in m/s.
- `wind_gust_mps` (`double`, nullable)
  - Wind gust at 10m in m/s.
- `wind_dir_deg` (`double`, nullable)
  - Wind direction in degrees `[0, 360]`.
- `temperature_c` (`double`, nullable)
  - Temperature at 2m in Celsius.
- `pressure_hpa` (`double`, nullable)
  - Mean sea-level pressure (hPa).
- `relative_humidity_pct` (`double`, nullable)
  - Relative humidity percent `[0,100]`.
- `precip_mm` (`double`, nullable)
  - Precipitation in millimeters.
- `daylight_flag` (`boolean` or nullable placeholder)
  - Reserved for explicit daylight row tagging (currently nullable in v1).

### Location/context
- `event_latitude` (`double`, nullable)
  - Event-level resolved latitude if available.
- `event_longitude` (`double`, nullable)
  - Event-level resolved longitude if available.
- `obs_latitude` (`double`, nullable)
  - Observation payload latitude (provider-reported).
- `obs_longitude` (`double`, nullable)
  - Observation payload longitude (provider-reported).
- `city` (`string`, nullable)
- `state` (`string`, nullable)
- `country` (`string`, nullable)

### Lineage and reproducibility
- `event_source_fingerprint` (`string`, required)
  - Event-level fingerprint of Bronze round sources used in this Silver run.
- `source_json_key` (`string`, required)
  - Bronze weather payload object key.
- `source_meta_key` (`string`, nullable)
  - Bronze metadata sidecar key.
- `source_content_sha256` (`string`, required)
  - Hash of source payload content.
- `source_fetched_at_utc` (`string`, required)
  - Bronze fetch timestamp.
- `request_fingerprint` (`string`, nullable)
  - Fingerprint of request URL + params.
- `tee_time_source_fingerprint` (`string`, nullable)
  - Fingerprint of Silver tee-time inputs used by Bronze weather ingestion.
- `silver_run_id` (`string`, required)
  - Silver weather run id.
- `silver_processed_at_utc` (`string`, required)
  - Processing timestamp for this Silver row set.

---

## Partitioning Strategy
Current event-level deterministic output:
- Partition directories:
  - `event_year=<YYYY>`
  - `event_id=<event_id>`
- File name:
  - `observations_hourly.parquet`

Rationale:
- Event overwrite semantics are simple and idempotent.
- Re-runs replace event artifact for same fingerprinted inputs.
- Efficient event-scoped backfills and troubleshooting.

---

## Data Quality Rules (v1)

### Required non-null fields
- `weather_obs_pk`
- `event_id`
- `round_number`
- `provider`
- `source_id`
- `observation_hour_utc`

### Lineage required fields
- `source_json_key`
- `source_content_sha256`
- `source_fetched_at_utc`
- `silver_run_id`

### Uniqueness
- Unique `weather_obs_pk` after dedup.

### Accepted ranges
- `relative_humidity_pct` in `[0, 100]` when non-null.
- `wind_speed_mps >= 0` when non-null.
- `wind_gust_mps >= 0` when non-null.
- `wind_dir_deg` in `[0, 360]` when non-null.
- `precip_mm >= 0` when non-null.

### Completeness
- Normalized event output must be non-empty.
- At least one hourly row for each loaded round-source group.

### DQ failure handling
On DQ failure:
- Event checkpoint status is set to `dq_failed`.
- Quarantine report written to:
  - `silver/weather/quarantine/observations_hourly/event_year=<YYYY>/event_id=<event_id>/run_id=<run_id>/dq_errors.json`

---

## Checkpoint and Run Summary Contract

Event checkpoint item:
- `pk=PIPELINE#SILVER_WEATHER_OBSERVATIONS`
- `sk=EVENT#<event_id>`

Run summary item:
- `pk=RUN#<run_id>`
- `sk=SILVER_WEATHER_OBSERVATIONS#SUMMARY`

Checkpoint statuses:
- `success`
- `dq_failed`
- `failed`

---

## Example Row (illustrative)

```json
{
  "weather_obs_pk": "b53b4c...d4",
  "event_id": 90008,
  "event_year": 2026,
  "round_number": 1,
  "provider": "open_meteo_archive",
  "source_id": "GRID#30.2672_-97.7431",
  "observation_ts_utc": "2026-03-10T08:00:00Z",
  "observation_hour_utc": "2026-03-10T08:00:00Z",
  "wind_speed_mps": 4.2,
  "wind_gust_mps": 6.0,
  "wind_dir_deg": 120.0,
  "temperature_c": 19.5,
  "pressure_hpa": 1012.2,
  "relative_humidity_pct": 72.0,
  "precip_mm": 0.0,
  "daylight_flag": null,
  "event_latitude": 30.2672,
  "event_longitude": -97.7431,
  "obs_latitude": 30.2672,
  "obs_longitude": -97.7431,
  "city": "Austin",
  "state": "TX",
  "country": "United States",
  "event_source_fingerprint": "5f6b...2a",
  "source_json_key": "bronze/weather/observations/provider=open_meteo_archive/event_id=90008/round=1/source_id=GRID#30.2672_-97.7431/fetch_date=2026-03-16/fetch_ts=2026-03-16T12_00_00Z.json",
  "source_meta_key": "bronze/weather/observations/provider=open_meteo_archive/event_id=90008/round=1/source_id=GRID#30.2672_-97.7431/fetch_date=2026-03-16/fetch_ts=2026-03-16T12_00_00Z.meta.json",
  "source_content_sha256": "9ab1...ce",
  "source_fetched_at_utc": "2026-03-16T12:00:00Z",
  "request_fingerprint": "3ef9...7d",
  "tee_time_source_fingerprint": "0a21...bc",
  "silver_run_id": "silver-weather-observations-20260316T210000Z",
  "silver_processed_at_utc": "2026-03-16T21:00:05Z"
}