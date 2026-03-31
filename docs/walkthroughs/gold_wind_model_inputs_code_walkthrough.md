# Gold Wind Model Inputs Code Walkthrough

## Module Overview
Package: `gold_wind_model_inputs`

Core modules:
- `config.py`
  - runtime config loading
- `dynamo_io.py`
  - candidate discovery from `PIPELINE#GOLD_WIND_EFFECTS`
  - event checkpoint writes
  - run summary writes
- `gold_io.py`
  - reads Gold hole feature parquet from S3
- `transform.py`
  - aggregates Gold hole rows into round-level model-input rows
  - derives round structural/weather features
  - builds deterministic row hashes and event fingerprints
- `quality.py`
  - validates the round model-input contract
- `parquet_io.py`
  - builds S3 output/quarantine keys
  - writes round parquet outputs
- `runner.py`
  - orchestrates incremental event processing

---

## What This Layer Does

`gold_wind_model_inputs` is the canonical round-level feature-preparation layer for the production wind model.

It takes:
- Gold hole feature rows from `gold_wind_effects`

and produces:
- one round-level model-input parquet per event

This package is intentionally:
- round-only
- event-level
- incremental
- idempotent

It does not:
- train the model
- score the model
- produce notebook-only exploratory summaries

---

## Why This Layer Exists

This layer exists to stabilize the contract between:
- upstream enriched facts
- downstream model training and inference

Benefits:
- deterministic round-level feature engineering
- versioned model-input schema
- reproducible training inputs
- event-level incremental recompute
- easier debugging when upstream data changes

---

## End-to-End Flow

### 1. Candidate discovery
The runner loads candidate events from successful `gold_wind_effects` checkpoints.

This means `gold_wind_model_inputs` only processes events that already have valid Gold weather/scoring feature outputs.

### 2. Run-mode filtering
The runner selects candidate events based on:
- `pending_only`
- `full_check`

In `pending_only`, events with unchanged successful checkpoints are skipped unless:
- their checkpoint is missing
- their checkpoint is failed
- their checkpoint is `dq_failed` and the run opts into retrying those

### 3. Load source Gold hole rows
For each selected event, the runner reads the event’s Gold hole feature parquet from S3.

This is the only required input table now.

The step no longer depends on Gold round rows.

### 4. Compute source fingerprint
The runner computes a deterministic event fingerprint from the source Gold hole rows plus the model-input policy version.

This fingerprint powers incremental processing:
- unchanged event source => skip
- changed event source => recompute

### 5. Build round model-input rows
The transform groups hole rows by:
- `tourn_id`
- `round_number`
- `player_key`

Then it aggregates:
- scoring totals
- structural totals/averages
- weather means/maxes
- preserved metadata like `course_id`, `layout_id`, `division`, `player_rating`

The output is one row per player-round.

### 6. Run DQ validation
Before any write, the round output is validated.

Current validation checks:
- PK preservation from hole inputs to round outputs
- PK uniqueness
- required columns present
- required fields not null
- valid bucket values
- non-null row hash

### 7. Write outputs
If validation passes, the runner overwrites the event’s deterministic round output parquet:
- `gold/pdga/wind_effects/model_inputs_round/event_year=<YYYY>/tourn_id=<ID>/model_inputs_round.parquet`

### 8. Write checkpoint and run summary
Successful events get a success checkpoint containing:
- event fingerprint
- event year
- round row count
- round output S3 key

DQ failures get:
- a quarantine JSON report
- `dq_failed` checkpoint status

At the end of the run, the runner writes a run summary item to DynamoDB.

---

## Incremental and Idempotent Design

This package is productionized around event-level incremental recompute.

### Incremental
An event is skipped when:
- its last checkpoint status is `success`
- and the saved event fingerprint matches the current source fingerprint

This keeps the step from reprocessing the whole dataset every run.

### Idempotent
The event output key is deterministic.
A rerun writes to the same event partition and overwrites the prior file.

That gives safe reruns without duplicate outputs.

---

## Transform Logic Walkthrough

The most important code lives in `transform.py`.

### `build_round_model_inputs(...)`
This is the canonical round feature builder.

Inputs:
- Gold hole feature rows for one event
- `run_id`
- `processed_at_utc`
- `model_inputs_version`

Processing steps:
1. group hole rows by `(tourn_id, round_number, player_key)`
2. compute additive round totals:
   - `actual_round_strokes`
   - `round_strokes_over_par`
   - `round_total_hole_length`
   - `round_total_par`
   - `round_precip_mm_sum`
3. compute averages:
   - `round_avg_hole_length`
   - `round_avg_hole_par`
   - `round_wind_speed_mps_mean`
   - `round_wind_gust_mps_mean`
   - `round_temp_c_mean`
   - `round_precip_mm_mean`
   - `round_pressure_hpa_mean`
   - `round_humidity_pct_mean`
4. compute maxima:
   - `round_wind_speed_mps_max`
   - `round_wind_gust_mps_max`
5. preserve metadata:
   - `course_id`
   - `layout_id`
   - `division`
   - `player_rating`
6. derive buckets:
   - `round_wind_speed_bucket`
   - `round_wind_gust_bucket`
7. attach lineage:
   - `model_inputs_grain`
   - `model_inputs_version`
   - `model_inputs_run_id`
   - `model_inputs_processed_at_utc`
8. compute `row_hash_sha256`

### `compute_model_inputs_event_fingerprint(...)`
This function creates the event-level source fingerprint used for incremental processing.

It hashes a deterministic projection of the source hole rows and includes the policy version.

That means a change in:
- upstream Gold hole content
- or transform/business logic version

will trigger recomputation.

---

## Quality Logic Walkthrough

`quality.py` validates that the round output is structurally safe to use as a production model-input dataset.

### PK preservation
The output round PK set must match the round PK set implied by grouping the input hole rows.

### Uniqueness
No duplicate round PKs are allowed in the output.

### Required columns
Every row must contain the required round-level contract fields.

### Not-null fields
Critical modeling fields must not be null, including:
- `event_year`
- `actual_round_strokes`
- `round_strokes_over_par`
- `hole_count`
- `round_total_hole_length`
- `round_total_par`
- `player_rating`

### Accepted values
`model_inputs_grain` must equal `round`, and wind-speed bucket values must be valid.

### Quarantine behavior
If DQ fails:
- the event is not written as a success output
- a quarantine report is written to S3
- the event checkpoint is marked `dq_failed`

---

## Output Artifacts

### Round model-input parquet
One file per event:
- `gold/pdga/wind_effects/model_inputs_round/event_year=<YYYY>/tourn_id=<ID>/model_inputs_round.parquet`

### Quarantine report
If DQ fails:
- `gold/pdga/wind_effects/model_inputs_quarantine/event_year=<YYYY>/tourn_id=<ID>/run_id=<RUN_ID>/dq_errors.json`

### DynamoDB checkpoint
One item per event:
- `pk = PIPELINE#GOLD_WIND_MODEL_INPUTS`
- `sk = EVENT#<event_id>`

### DynamoDB run summary
One item per run with aggregate counts.

---

## Typical Commands

Run unit tests:
```powershell
python -m pytest tests/gold_wind_model_inputs -v
