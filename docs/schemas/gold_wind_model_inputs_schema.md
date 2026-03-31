# Gold Wind Model Inputs Schema

## Purpose
`gold_wind_model_inputs` is the canonical, versioned, round-level modeling contract built from `gold_wind_effects`.

This layer exists to produce the stable feature table used by the production round-level wind model.

Current design:
- input: Gold hole feature rows from `gold_wind_effects`
- output: aggregated round-level model-input rows
- grain: one row per player-round
- output path:
  - `gold/pdga/wind_effects/model_inputs_round/event_year=<YYYY>/tourn_id=<ID>/model_inputs_round.parquet`

This step is intentionally round-only. It no longer produces hole-level model-input tables.

---

## Grain

### model_inputs_round
One row per:
- `(tourn_id, round_number, player_key)`

This row is built by aggregating all hole rows for a player within a round.

---

## Partitioning
Output is partitioned by:
- `event_year`
- `tourn_id`

S3 output key pattern:
- `gold/pdga/wind_effects/model_inputs_round/event_year=<YYYY>/tourn_id=<ID>/model_inputs_round.parquet`

---

## Input Contract

`gold_wind_model_inputs` consumes:
- Gold hole feature parquet from `gold_wind_effects`

Expected upstream grain:
- one row per `(tourn_id, round_number, hole_number, player_key)`

Important upstream fields used during aggregation include:
- identifiers:
  - `event_year`
  - `tourn_id`
  - `round_number`
  - `hole_number`
  - `player_key`
  - `course_id`
  - `layout_id`
  - `division`
  - `player_rating`
- scoring:
  - `actual_strokes`
  - `strokes_over_par`
  - `hole_par`
  - `hole_length`
- weather:
  - `weather_available_flag`
  - `wx_wind_speed_mps`
  - `wx_wind_gust_mps`
  - `wx_temperature_c`
  - `wx_precip_mm`
  - `wx_pressure_hpa`
  - `wx_relative_humidity_pct`
- lineage:
  - `source_content_sha256`

---

## Required Output Fields

All `model_inputs_round` rows must include:

- `event_year`
- `tourn_id`
- `round_number`
- `player_key`
- `model_inputs_grain`
- `model_inputs_version`
- `model_inputs_run_id`
- `model_inputs_processed_at_utc`
- `row_hash_sha256`
- `actual_round_strokes`
- `round_strokes_over_par`
- `weather_available_flag`
- `hole_count`
- `round_total_hole_length`
- `round_avg_hole_length`
- `round_total_par`
- `round_avg_hole_par`
- `round_length_over_par`
- `round_wind_speed_mps_mean`
- `round_wind_speed_mps_max`
- `round_wind_gust_mps_mean`
- `round_wind_gust_mps_max`
- `round_temp_c_mean`
- `round_precip_mm_sum`
- `round_precip_mm_mean`
- `round_pressure_hpa_mean`
- `round_humidity_pct_mean`
- `round_wind_speed_bucket`
- `round_wind_gust_bucket`

Important first-class metadata fields also expected on output:
- `course_id`
- `layout_id`
- `division`
- `player_rating`

---

## Output Field Definitions

### Keys and partitioning
- `event_year`
  - event year used for partitioning and convenience
- `tourn_id`
  - tournament/event identifier
- `round_number`
  - round number within event
- `player_key`
  - stable player identifier

### Lineage and processing metadata
- `model_inputs_grain`
  - always `round`
- `model_inputs_version`
  - transform/business logic version for this dataset
- `model_inputs_run_id`
  - pipeline run identifier for the event-level build
- `model_inputs_processed_at_utc`
  - UTC timestamp for row creation
- `row_hash_sha256`
  - deterministic row-level hash of the canonical model-input payload

### Preserved descriptive metadata
- `course_id`
  - course identifier inherited from Gold hole rows
- `layout_id`
  - layout identifier inherited from Gold hole rows
- `division`
  - division code inherited from Gold hole rows
- `player_rating`
  - player rating inherited from Gold hole rows

### Round scoring targets
- `actual_round_strokes`
  - sum of hole-level `actual_strokes`
- `round_strokes_over_par`
  - sum of hole-level `strokes_over_par`
  - fallback: `actual_round_strokes - round_total_par` if needed

### Round structural features
- `hole_count`
  - distinct count of holes for the player-round
- `round_total_hole_length`
  - sum of `hole_length`
- `round_avg_hole_length`
  - average hole length within the round
- `round_total_par`
  - sum of `hole_par`
- `round_avg_hole_par`
  - average hole par within the round
- `round_length_over_par`
  - `round_total_hole_length / round_total_par`

### Round weather features
- `weather_available_flag`
  - `True` only when all contributing hole rows are weather-available
- `round_wind_speed_mps_mean`
  - mean hole-level wind speed across the round
- `round_wind_speed_mps_max`
  - max hole-level wind speed across the round
- `round_wind_gust_mps_mean`
  - mean hole-level wind gust across the round
- `round_wind_gust_mps_max`
  - max hole-level wind gust across the round
- `round_temp_c_mean`
  - mean hole-level temperature across the round
- `round_precip_mm_sum`
  - sum of hole-level precipitation
- `round_precip_mm_mean`
  - mean hole-level precipitation
- `round_pressure_hpa_mean`
  - mean hole-level pressure
- `round_humidity_pct_mean`
  - mean hole-level relative humidity

### Derived buckets
- `round_wind_speed_bucket`
  - derived from `round_wind_speed_mps_mean`
  - allowed values:
    - `calm`
    - `light`
    - `moderate`
    - `strong`
    - `very_strong`
    - `unknown`
- `round_wind_gust_bucket`
  - derived from `round_wind_gust_mps_mean`
  - allowed values:
    - `low`
    - `mild`
    - `high`
    - `very_high`
    - `unknown`

---

## Aggregation Rules

`model_inputs_round` is built by grouping hole rows on:
- `tourn_id`
- `round_number`
- `player_key`

Aggregation behavior:
- additive scoring/structure fields use sums
- average structure/weather fields use means
- max wind/gust fields use max
- identity-like fields such as `course_id`, `layout_id`, `division`, `player_rating` are taken from the first non-null value within the group
- `hole_count` is based on distinct `hole_number`

---

## Incremental and Idempotent Contract

This step is event-level, incremental, and safe to rerun.

### Candidate selection
Source candidates come from successful `PIPELINE#GOLD_WIND_EFFECTS` checkpoints.

### Event fingerprint
A deterministic event fingerprint is computed from the source Gold hole rows plus the model-input policy version.

This means:
- if an event’s Gold hole inputs have not changed
- and the model-input policy version has not changed

then the event can be skipped safely.

### Checkpoint family
- `pk = PIPELINE#GOLD_WIND_MODEL_INPUTS`
- `sk = EVENT#<event_id>`

### Idempotent write behavior
Output S3 key is deterministic per event:
- the same event always overwrites the same `model_inputs_round.parquet`

That makes reruns safe and avoids duplicate outputs.

---

## Data Quality Rules

The model-input step validates the round output before writing success checkpoints.

Current checks include:
- round PK preservation from source hole rows
- round PK uniqueness
- required columns present
- required core fields not null
- `model_inputs_grain == "round"`
- valid `round_wind_speed_bucket` values
- non-null `row_hash_sha256`

Quarantine path:
- `gold/pdga/wind_effects/model_inputs_quarantine/event_year=<YYYY>/tourn_id=<ID>/run_id=<RUN_ID>/dq_errors.json`

Events that fail DQ are marked with checkpoint status:
- `dq_failed`

---

## Relationship to Training and Inference

`gold_wind_model_inputs` does not train or score the model.

Its responsibility is to produce the stable round-level feature table used by:
- training jobs
- inference/scoring jobs
- notebook analysis
- downstream reporting

This separation keeps:
- preprocessing deterministic
- model training reproducible
- inference aligned to the same feature contract

---

## Relationship to `gold_wind_effects`

`gold_wind_effects` remains the reusable enriched fact layer.

`gold_wind_model_inputs` is the first model-specific layer.

The intended boundary is:
- `gold_wind_effects`
  - general-purpose enriched scoring/weather facts
- `gold_wind_model_inputs`
  - canonical round-level model-ready feature table
