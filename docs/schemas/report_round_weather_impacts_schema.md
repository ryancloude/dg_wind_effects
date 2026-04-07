# Report Round Weather Impacts Schema

## Purpose

`report_round_weather_impacts` is the production reporting step that converts row-level scored outputs into dashboard-ready aggregate tables.

It consumes:
- event-level scored round outputs from `score_round_wind_model`

It produces:
- event-level intermediate contribution tables
- published aggregate reporting tables
- dashboard-ready weather summaries in user-facing units
- event-level incremental checkpoints in DynamoDB

This step is the semantic layer for the dashboard.

---

## Design Goal

The reporting step is designed around the dashboard, not as a generic aggregate dump.

Key goals:
- dashboard-first reporting outputs
- event-level incremental processing
- idempotent deterministic writes
- user-facing wind in `mph`
- user-facing temperature in `Fahrenheit`

---

## Input Contract

### Source location
The reporting step reads scored round outputs from:

- `gold/pdga/wind_effects/scored_rounds/event_year=<YYYY>/tourn_id=<ID>/scored_rounds.parquet`

### Input grain
One row per:
- `(tourn_id, round_number, player_key)`

### Required input columns
The reporting step expects at least:

- `event_year`
- `tourn_id`
- `round_number`
- `player_key`
- `course_id`
- `layout_id`
- `division`
- `player_rating`
- `actual_round_strokes`
- `predicted_round_strokes`
- `predicted_round_strokes_wind_reference`
- `estimated_wind_impact_strokes`
- `estimated_temperature_impact_strokes`
- `estimated_total_weather_impact_strokes`
- `round_wind_speed_mps_mean`
- `round_temp_c_mean`

Optional but preferred fields for richer reporting:
- `event_name`
- `state`
- `city`
- `lat`
- `lon`
- `round_date`
- `round_precip_mm_sum`

---

## Derived Reporting Dimensions

The reporting step derives dashboard-facing dimensions from scored rows.

## Derived unit fields
- `observed_wind_mph`
- `observed_temp_f`

### Conversion rules
- `observed_wind_mph = round_wind_speed_mps_mean * 2.23694`
- `observed_temp_f = round_temp_c_mean * 9/5 + 32`

## Derived categorical dimensions
- `rating_band`
- `temperature_band_f`
- `precip_flag`
- `round_year`
- `round_month`
- `round_month_label`

### Rating band values
Recommended:
- `<900`
- `900-939`
- `940-969`
- `970+`
- `Unknown`

### Temperature band values
Recommended:
- `<41F`
- `41-49F`
- `50-59F`
- `60-69F`
- `70-79F`
- `80F+`
- `Unknown`

### Precipitation flag
- `No Precip`
- `Precip`

---

## Two-Layer Reporting Output Model

The reporting step writes two layers of output.

## Layer 1: Intermediate event contributions
These are deterministic event-level aggregate files.

Purpose:
- incremental processing
- event-level idempotent overwrite behavior
- stable building blocks for published tables

## Layer 2: Published dashboard tables
These are global aggregate tables rebuilt from all event contribution files.

Purpose:
- dashboard consumption
- simplified query surface
- stable semantic layer for visualizations

---

## Intermediate Output Contract

### Intermediate base prefix
- `gold/pdga/wind_effects/reports/intermediate/`

### Path pattern
For each report table:
- `gold/pdga/wind_effects/reports/intermediate/<table_name>/event_year=<YYYY>/tourn_id=<ID>/part.parquet`

### General design
Intermediate tables store:
- grouping dimensions
- additive counts
- additive sums

Published tables recompute averages from those sums.

### Standard additive metrics
Intermediate tables may include:
- `rounds_scored`
- `events_scored`
- `players_scored`
- `sum_observed_wind_mph`
- `sum_observed_temp_f`
- `sum_actual_round_strokes`
- `sum_predicted_round_strokes`
- `sum_predicted_round_strokes_wind_reference`
- `sum_estimated_wind_impact_strokes`
- `sum_estimated_temperature_impact_strokes`
- `sum_estimated_total_weather_impact_strokes`

---

## Intermediate Report Tables

## 1. `weather_overview`
### Grain
- one row per event

### Purpose
- supports top-level overview aggregation

## 2. `weather_by_wind_bucket`
### Grain
- one row per:
  - event
  - wind speed bucket
  - subgroup slice

### Purpose
- supports Overview page hero chart

## 3. `weather_by_temperature_band`
### Grain
- one row per:
  - event
  - temperature band
  - subgroup slice

### Purpose
- supports temperature chart on Overview page

## 4. `weather_by_month`
### Grain
- one row per:
  - event
  - month
  - subgroup slice

### Purpose
- supports seasonal trend visuals

## 5. `weather_by_event_geo`
### Grain
- one row per event

### Purpose
- supports event point map

## 6. `weather_by_state`
### Grain
- one row per:
  - event
  - state
  - subgroup slice

### Purpose
- supports state choropleth and state comparisons

## 7. `weather_by_division`
### Grain
- one row per:
  - event
  - division

### Purpose
- supports division comparison charts

## 8. `weather_by_rating_band`
### Grain
- one row per:
  - event
  - rating band

### Purpose
- supports player skill comparison charts

## 9. `weather_by_course_layout`
### Grain
- one row per:
  - event
  - course/layout
  - optional subgroup slice

### Purpose
- supports venue ranking tables and scatter plots

## 10. `weather_by_event`
### Grain
- one row per event

### Purpose
- supports Event Explorer event summaries

## 11. `weather_by_event_round`
### Grain
- one row per:
  - event
  - round number
  - optional division

### Purpose
- supports Event Explorer round-level trends

---

## Published Output Contract

### Published base prefix
- `gold/pdga/wind_effects/reports/published/`

### Path pattern
For each report table:
- `gold/pdga/wind_effects/reports/published/<table_name>/report.parquet`

### Published table purpose
Published tables are the dashboard-ready semantic layer.

They should be:
- filter-friendly
- stable
- directly usable by the dashboard
- consistently in mph / Fahrenheit for weather fields

### Published metrics
Published tables compute averages from additive intermediate metrics.

Typical averages include:
- `avg_observed_wind_mph`
- `avg_observed_temp_f`
- `avg_actual_round_strokes`
- `avg_predicted_round_strokes`
- `avg_predicted_round_strokes_wind_reference`
- `avg_estimated_wind_impact_strokes`
- `avg_estimated_temperature_impact_strokes`
- `avg_estimated_total_weather_impact_strokes`

---

## Published Table Summary

## 1. `weather_overview`
Purpose:
- KPI cards on Overview page

## 2. `weather_by_wind_bucket`
Purpose:
- Overview hero chart

## 3. `weather_by_temperature_band`
Purpose:
- Overview temperature chart

## 4. `weather_by_month`
Purpose:
- monthly trend chart

## 5. `weather_by_event_geo`
Purpose:
- Geography page event point map

## 6. `weather_by_state`
Purpose:
- Geography page state choropleth and top states table

## 7. `weather_by_division`
Purpose:
- Where It Matters division chart

## 8. `weather_by_rating_band`
Purpose:
- Where It Matters player skill chart

## 9. `weather_by_course_layout`
Purpose:
- Where It Matters venue ranking and sensitivity scatter

## 10. `weather_by_event`
Purpose:
- Event Explorer event summary cards and event table

## 11. `weather_by_event_round`
Purpose:
- Event Explorer round-level trends and comparisons

---

## DynamoDB Checkpoint Contract

## Checkpoint family
- `pk = PIPELINE#REPORT_ROUND_WEATHER_IMPACTS`
- `sk = EVENT#<event_id>#REPORT_POLICY#<report_policy_version>`

## Checkpoint purpose
This stores one checkpoint per:
- event
- report policy version

This allows:
- event-level incremental processing
- safe reruns
- clean recomputation when reporting logic changes

## Checkpoint fields
Typical fields include:
- `pipeline`
- `event_id`
- `report_policy_version`
- `status`
- `last_run_id`
- `updated_at`
- `event_year`
- `source_scored_key`
- `scored_input_fingerprint`
- `rows_input`
- `rows_retained`

### Status values
Typical statuses:
- `success`
- `failed`

## Run summary
A run summary item is written with:
- `pk = RUN#<run_id>`
- `sk = REPORT_ROUND_WEATHER_IMPACTS#SUMMARY`

---

## Incremental and Idempotent Behavior

The reporting step is incremental at the event level.

## Event input fingerprint
Each scored event file is fingerprinted using object metadata such as:
- key
- etag
- size
- last modified

## Skip behavior
An event is skipped when:
- a success checkpoint exists
- the current scored input fingerprint matches the stored fingerprint
- the report policy version matches
- `--force-events` is not used

## Idempotent writes
Intermediate contribution files are written to deterministic paths by:
- event year
- event ID
- report table name

Re-running for the same event overwrites the same logical file.

## Published table rebuild
Published tables are rebuilt from the complete set of intermediate contribution files.

This ensures:
- correctness
- deterministic published outputs
- compatibility with event-level incremental contribution updates

---

## Dashboard Relationship

This package is designed specifically to support the dashboard.

### Dashboard pages supported
- Overview
- Geography
- Where It Matters
- Event Explorer

### Dashboard-facing unit policy
All weather values in reporting outputs should be:
- wind in `mph`
- temperature in `Fahrenheit`

This is intentional because the dashboard is U.S.-oriented and portfolio-facing.

---

## Relationship to Upstream and Downstream Steps

## Upstream
- `score_round_wind_model`
  - produces row-level scored outputs

## Current step
- `report_round_weather_impacts`
  - produces dashboard-ready reporting tables

## Downstream
- dashboard UI
- dashboards/notebooks/visual exploration
- portfolio presentation layer

---

## Notes on Scope

This step intentionally does not:
- retrain the model
- rescore rounds
- make causal claims
- own the visualization layer directly

Its responsibility is:
- stable dashboard semantic tables
- stable reporting units
- incremental event-level contribution generation
- published aggregate tables for the dashboard