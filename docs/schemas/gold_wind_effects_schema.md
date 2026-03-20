# Gold Wind Effects Schema

## Purpose
`gold_wind_effects` is the first Gold layer on top of `silver_weather_enriched`.  
It preserves the same row grains as Silver weather-enriched outputs and appends modeling-ready Gold fields.

Outputs:
- `gold/pdga/wind_effects/player_rounds_features/.../player_rounds_features.parquet`
- `gold/pdga/wind_effects/player_holes_features/.../player_holes_features.parquet`

## Grains

### player_rounds_features
One row per:
- `(tourn_id, round_number, player_key)`

### player_holes_features
One row per:
- `(tourn_id, round_number, hole_number, player_key)`

## Partitioning
Both outputs are partitioned by:
- `event_year`
- `tourn_id`

## Incremental/Idempotent Contract
- Event-level deterministic fingerprint over source scoring/weather fields.
- Event checkpoint item in DynamoDB:
  - `pk = PIPELINE#GOLD_WIND_EFFECTS`
  - `sk = EVENT#<event_id>`
- Event is skipped in incremental mode when:
  - checkpoint status is `success`, and
  - checkpoint fingerprint equals current event fingerprint.
- Re-runs are idempotent because each event write overwrites deterministic object keys.

## Appended Gold Fields

### Common added fields (round + hole)
- `gold_grain` (`round` or `hole`)
- `gold_model_version` (currently `v1`)
- `gold_run_id`
- `gold_processed_at_utc`
- `actual_strokes`
- `strokes_over_par`
- `weather_available_flag`
- `wind_speed_bucket` (`unknown`, `calm`, `light`, `moderate`, `strong`, `very_strong`)
- `row_hash_sha256`

## Data Quality Rules
- Output row counts must match input row counts.
- Output PK sets must match input PK sets (no dropped/added keys).
- Required Gold columns must exist on every row.
- `row_hash_sha256` must be non-null.
- DQ failures are quarantined to:
  - `gold/pdga/wind_effects/quarantine/event_year=<YYYY>/tourn_id=<id>/run_id=<run_id>/dq_errors.json`

## Lineage
Each row carries source lineage inherited from Silver:
- `source_json_key`
- `source_content_sha256`
- weather lineage columns (`wx_source_json_key`, etc.)

Each row also inherits full player biography fields from Silver:
- `player_name`
- `first_name`
- `last_name`
- `short_name`
- `profile_url`
- `player_city`
- `player_state_prov`
- `player_country`
- `player_full_location`
- `player_rating`