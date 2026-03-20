# Gold Wind Model Inputs Schema

## Purpose
`gold_wind_model_inputs` is the curated modeling contract layer built from `gold_wind_effects`.

Outputs:
- `gold/pdga/wind_effects/model_inputs_hole/.../model_inputs_hole.parquet`
- `gold/pdga/wind_effects/model_inputs_round/.../model_inputs_round.parquet` (optional if round input present)

## Grains

### model_inputs_hole
One row per:
- `(tourn_id, round_number, hole_number, player_key)`

### model_inputs_round
One row per:
- `(tourn_id, round_number, player_key)`

## Partitioning
Both outputs are partitioned by:
- `event_year`
- `tourn_id`

## Required Fields

### model_inputs_hole required
- `event_year`
- `tourn_id`
- `round_number`
- `hole_number`
- `player_key`
- `model_inputs_grain`
- `model_inputs_version`
- `target_strokes_over_par`
- `weather_available_flag`
- `row_hash_sha256`

### model_inputs_round required
- `event_year`
- `tourn_id`
- `round_number`
- `player_key`
- `model_inputs_grain`
- `model_inputs_version`
- `target_strokes_over_par`
- `weather_available_flag`
- `row_hash_sha256`

## Derived Modeling Fields

Examples (hole):
- `target_strokes_over_par`
- `feature_wind_speed_mps`
- `feature_wind_gust_mps`
- `feature_wind_dir_deg`
- `feature_temp_c`
- `feature_precip_mm`
- `feature_pressure_hpa`
- `feature_humidity_pct`
- `feature_hole_par`
- `feature_layout_id`
- `feature_course_id`
- `wind_speed_bucket`

## Inherited Player Biography Fields
`gold_wind_model_inputs` preserves all upstream columns from `gold_wind_effects`, including:
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

## Incremental/Idempotent Contract
- Source candidates come from successful `PIPELINE#GOLD_WIND_EFFECTS`.
- Event fingerprint is computed from source Gold feature rows.
- Checkpoint family:
  - `pk = PIPELINE#GOLD_WIND_MODEL_INPUTS`
  - `sk = EVENT#<event_id>`
- Deterministic output key per event enables safe overwrite reruns.

## Data Quality Rules
- Row count preservation (input vs output).
- PK preservation and PK uniqueness.
- Required columns present.
- `target_strokes_over_par` not null.
- `row_hash_sha256` not null.
- Quarantine path:
  - `gold/pdga/wind_effects/model_inputs_quarantine/event_year=<YYYY>/tourn_id=<id>/run_id=<run_id>/dq_errors.json`