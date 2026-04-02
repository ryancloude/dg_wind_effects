```md
# DG Wind Effects (`dg_wind_effects`)

An AWS-native, production-style data platform for estimating how weather, especially wind, affects disc golf scoring.

The project ingests PDGA event and live scoring data, aligns historical weather observations to round and hole timing, builds Bronze/Silver/Gold datasets in S3, prepares a canonical round-level modeling dataset, and trains a production one-stage CatBoost model for wind-impact analysis.

## Project Goal

Build a reproducible analytics pipeline that answers questions like:

- How many expected strokes does wind add?
- How does that effect change by course, layout, division, and player skill?
- Under what weather conditions is scoring most affected?

The intended end state is:

- replayable Bronze raw data in S3
- normalized and validated Silver datasets
- analytics-ready Gold datasets
- productionized model-input preparation
- productionized model training and inference
- notebook and reporting outputs that are understandable and reproducible

---

## Current Status

### Implemented pipeline layers

Bronze ingestion:
- `ingest_pdga_event_pages`
- `ingest_pdga_live_results`
- `ingest_weather_observations`

Silver processing:
- `silver_pdga_live_results`
- `silver_weather_observations`
- `silver_weather_enriched`

Gold processing:
- `gold_wind_effects`
- `gold_wind_model_inputs`

Modeling:
- `train_round_wind_model`

Analysis and prototyping:
- `wind_impact_analysis`

### Current production modeling direction

The current production modeling direction is:

- one-stage CatBoost
- round-level grain
- model-ready inputs produced by `gold_wind_model_inputs`
- model trained by `train_round_wind_model`
- future scoring step to generate predictions and wind-effect outputs

### What is still in progress

Planned or partially complete next steps:
- productionized model inference/scoring job
- scored output tables with:
  - predicted round strokes
  - calm counterfactual predictions
  - estimated wind-impact strokes
- broader documentation and reporting polish

---

## Repository Layout

```text
dg_wind_effects/
├── ingest_pdga_event_pages/
├── ingest_pdga_live_results/
├── ingest_weather_observations/
├── silver_pdga_live_results/
├── silver_weather_observations/
├── silver_weather_enriched/
├── gold_wind_effects/
├── gold_wind_model_inputs/
├── train_round_wind_model/
├── wind_impact_analysis/
├── docs/
├── docker/
├── infra/
└── tests/
```

### Package roles

- `ingest_pdga_event_pages`
  - ingests event metadata HTML and writes Bronze raw content plus event metadata state

- `ingest_pdga_live_results`
  - ingests PDGA live scoring JSON by event/division/round and writes Bronze raw content plus state

- `ingest_weather_observations`
  - ingests historical Open-Meteo weather observations aligned to event timing/location and writes Bronze raw payloads plus state

- `silver_pdga_live_results`
  - normalizes PDGA live results into stable round and hole parquet outputs

- `silver_weather_observations`
  - normalizes Bronze weather payloads into canonical hourly weather observations

- `silver_weather_enriched`
  - joins Silver weather observations onto Silver player rounds and player holes

- `gold_wind_effects`
  - produces reusable enriched scoring/weather fact tables from Silver weather-enriched outputs

- `gold_wind_model_inputs`
  - produces the canonical round-level model-input dataset used by the production wind model

- `train_round_wind_model`
  - trains the production round-level one-stage CatBoost model and writes versioned model artifacts

- `wind_impact_analysis`
  - notebook-based exploration, feature experiments, and model evaluation

---

## Pipeline Architecture

The pipeline follows a Bronze / Silver / Gold / Modeling pattern.

### Bronze
Raw, replayable source snapshots and state.

Examples:
- PDGA event HTML
- PDGA live-results JSON
- Open-Meteo archive weather JSON

### Silver
Normalized, typed, quality-checked datasets.

Examples:
- `player_rounds`
- `player_holes`
- hourly weather observations
- weather-enriched round/hole tables

### Gold
Analytics-ready, versioned outputs.

Examples:
- enriched round/hole scoring-weather facts
- canonical round-level model-input dataset

### Modeling
Versioned training and, later, scoring steps built on the Gold feature contracts.

Examples:
- round-level CatBoost training artifact bundle
- future scored round outputs with wind-effect estimates

---

## End-to-End Data Flow

### 1. Bronze Event Metadata: `ingest_pdga_event_pages`

Purpose:
- Discover and track PDGA event metadata and event structure.

Inputs:
- PDGA event HTML pages.

Outputs:
- Bronze raw HTML in S3
- event metadata in DynamoDB

Typical information captured:
- event dates
- status
- city/state/country
- lat/lon when available
- division and round structure
- parser hash and source metadata

Common commands:

```powershell
python -m ingest_pdga_event_pages.runner --incremental
python -m ingest_pdga_event_pages.runner --ids 90008,90009
python -m ingest_pdga_event_pages.runner --range 90000-90100
```

---

### 2. Bronze Live Results: `ingest_pdga_live_results`

Purpose:
- Pull per-event, per-division, per-round live scoring payloads.

Inputs:
- event metadata and division/round structure from DynamoDB

Outputs:
- Bronze raw live-results JSON in S3
- per-round state items in DynamoDB
- run summaries in DynamoDB
- event-level live-results ingested markers

Common commands:

```powershell
python -m ingest_pdga_live_results.runner --historical-backfill
python -m ingest_pdga_live_results.runner --event-ids 90008,90009
python -m ingest_pdga_live_results.runner --historical-backfill --dry-run
```

---

### 3. Silver Live Results: `silver_pdga_live_results`

Purpose:
- Normalize Bronze live-results payloads into stable round and hole tables.

Inputs:
- Bronze live-results raw payloads
- event metadata and state

Outputs:
- `silver/pdga/live_results/player_rounds/.../player_rounds.parquet`
- `silver/pdga/live_results/player_holes/.../player_holes.parquet`
- quarantine reports for DQ failures
- DynamoDB checkpoints and run summaries

Important downstream fields:
- normalized scoring values
- round/hole identifiers
- `round_date_interp`
- `tee_time_join_ts`

Common commands:

```powershell
python -m silver_pdga_live_results.runner --run-mode pending_only --progress-every 25
python -m silver_pdga_live_results.runner --run-mode full_check --force-events
python -m silver_pdga_live_results.runner --event-ids 90008,90009 --force-events
```

---

### 4. Bronze Weather Observations: `ingest_weather_observations`

Purpose:
- Pull historical weather observations aligned to event timing and location.

Inputs:
- Silver live-results output
- event location metadata
- tee-time-derived timing fields

Processing highlights:
- resolves coordinates from metadata, cache, or geocoding fallback
- builds event/round fetch windows
- fetches Open-Meteo archive weather data
- tracks request/content fingerprints for idempotency

Outputs:
- Bronze raw weather JSON in S3
- weather state and summaries in DynamoDB
- geocode cache in DynamoDB

Common commands:

```powershell
python -m ingest_weather_observations.runner --incremental --progress-every 25
python -m ingest_weather_observations.runner --event-ids 90008,90009 --dry-run
python -m ingest_weather_observations.runner --historical-backfill --progress-every 25
```

---

### 5. Silver Weather Observations: `silver_weather_observations`

Purpose:
- Normalize Bronze weather payloads into canonical hourly observations.

Inputs:
- Bronze weather raw payloads and event-level weather state

Outputs:
- Silver weather observation parquet
- quarantine reports for DQ failures
- DynamoDB checkpoints and run summaries

Common commands:

```powershell
python -m silver_weather_observations.runner --dry-run --log-level INFO
python -m silver_weather_observations.runner --run-mode pending_only --progress-every 25
python -m silver_weather_observations.runner --run-mode full_check --force-events --progress-every 25
python -m silver_weather_observations.runner --event-ids 90008,90009 --force-events
```

---

### 6. Silver Weather Enrichment: `silver_weather_enriched`

Purpose:
- Join Silver weather observations onto Silver player rounds and player holes.

Inputs:
- `silver_pdga_live_results`
- `silver_weather_observations`

Outputs:
- weather-enriched round parquet
- weather-enriched hole parquet
- checkpoint and run summary metadata
- quarantine outputs on DQ failures

This is the bridge between normalized gameplay data and analytics/modeling-ready weather-linked data.

Common commands:

```powershell
python -m silver_weather_enriched.runner --dry-run --log-level INFO
python -m silver_weather_enriched.runner --run-mode pending_only --progress-every 25
python -m silver_weather_enriched.runner --run-mode full_check --force-events --progress-every 25
python -m silver_weather_enriched.runner --event-ids 90008,90009 --force-events
```

---

### 7. Gold Wind Effects: `gold_wind_effects`

Purpose:
- Build reusable enriched fact tables for wind-effects analysis.

Inputs:
- Silver weather-enriched round and hole outputs

Outputs:
- `gold/pdga/wind_effects/player_rounds_features/.../player_rounds_features.parquet`
- `gold/pdga/wind_effects/player_holes_features/.../player_holes_features.parquet`
- quarantine reports for DQ failures
- DynamoDB checkpoints and run summaries

What this layer standardizes:
- `actual_strokes`
- `strokes_over_par`
- `weather_available_flag`
- `wind_speed_bucket`
- lineage metadata and row hashes

Design intent:
- keep this layer reusable and broadly useful
- avoid baking notebook-only model logic into it

Common commands:

```powershell
python -m gold_wind_effects.runner --dry-run --log-level INFO
python -m gold_wind_effects.runner --run-mode pending_only --progress-every 25
python -m gold_wind_effects.runner --run-mode full_check --force-events --progress-every 25
python -m gold_wind_effects.runner --event-ids 90008,90009 --force-events
```

---

### 8. Gold Wind Model Inputs: `gold_wind_model_inputs`

Purpose:
- Produce the canonical, versioned, round-level model-input dataset for the production wind model.

Inputs:
- Gold hole feature rows from `gold_wind_effects`

Output:
- `gold/pdga/wind_effects/model_inputs_round/event_year=<YYYY>/tourn_id=<ID>/model_inputs_round.parquet`

Current design:
- round-only output
- one row per `(tourn_id, round_number, player_key)`
- built by aggregating Gold hole rows
- incremental and idempotent at the event level

Important output fields include:
- keys:
  - `event_year`
  - `tourn_id`
  - `round_number`
  - `player_key`
- metadata:
  - `course_id`
  - `layout_id`
  - `division`
  - `player_rating`
- targets/context:
  - `actual_round_strokes`
  - `round_strokes_over_par`
- structural features:
  - `hole_count`
  - `round_total_hole_length`
  - `round_avg_hole_length`
  - `round_total_par`
  - `round_avg_hole_par`
  - `round_length_over_par`
- weather features:
  - `round_wind_speed_mps_mean`
  - `round_wind_speed_mps_max`
  - `round_wind_gust_mps_mean`
  - `round_wind_gust_mps_max`
  - `round_temp_c_mean`
  - `round_precip_mm_sum`
  - `round_precip_mm_mean`
  - `round_pressure_hpa_mean`
  - `round_humidity_pct_mean`
- buckets:
  - `round_wind_speed_bucket`
  - `round_wind_gust_bucket`
- lineage:
  - `model_inputs_version`
  - `model_inputs_run_id`
  - `model_inputs_processed_at_utc`
  - `row_hash_sha256`

Design intent:
- keep `gold_wind_effects` generic
- make `gold_wind_model_inputs` the first model-specific feature contract layer

Common commands:

```powershell
python -m gold_wind_model_inputs.runner --dry-run --log-level INFO
python -m gold_wind_model_inputs.runner --run-mode pending_only --progress-every 25
python -m gold_wind_model_inputs.runner --run-mode full_check --force-events --progress-every 25
python -m gold_wind_model_inputs.runner --event-ids 90008,90009 --force-events
```

---

### 9. Train Round Wind Model: `train_round_wind_model`

Purpose:
- Train the production round-level one-stage CatBoost wind model from the canonical round model-input dataset.

Inputs:
- `gold/pdga/wind_effects/model_inputs_round/...`

Outputs:
- versioned CatBoost model artifact bundle in S3
- training manifest and metrics
- feature importance export
- one DynamoDB checkpoint per training request fingerprint
- one training run summary per run

Current production model:
- one-stage CatBoost
- round-level grain
- target:
  - `actual_round_strokes`
- validation set used for early stopping
- test set used for final metrics

Artifact bundle includes:
- `model.cbm`
- `training_manifest.json`
- `metrics.json`
- `feature_columns.json`
- `categorical_feature_columns.json`
- `feature_importance.csv`

Checkpoint behavior:
- one canonical checkpoint per training request fingerprint
- successful identical training requests are skipped unless `--force-train`

Common commands:

```powershell
python -m train_round_wind_model.runner --dry-run --log-level INFO
python -m train_round_wind_model.runner --log-level INFO
python -m train_round_wind_model.runner --event-ids 90008,90009 --log-level INFO
python -m train_round_wind_model.runner --force-train --log-level INFO
```

---

## Incremental and Idempotent Design

A core design goal across the pipeline is safe reruns.

Patterns used throughout:
- deterministic event-level or request-level output identities
- source fingerprints to detect unchanged inputs
- checkpoints in DynamoDB
- run summaries for observability
- quarantine outputs for DQ failures

### Event-level incremental steps
These steps are incremental at the event level:
- `silver_pdga_live_results`
- `silver_weather_observations`
- `silver_weather_enriched`
- `gold_wind_effects`
- `gold_wind_model_inputs`

For these steps:
- unchanged event source data can be skipped
- reruns overwrite deterministic event output paths
- duplicate outputs are avoided

### Training-step incremental behavior
`train_round_wind_model` is incremental at the training-request level.

It computes a training request fingerprint from:
- source dataset fingerprint
- model identity/version
- feature contract
- split settings
- hyperparameters
- optional event subset

If the same request already succeeded, training is skipped unless forced.

---

## Modeling and Analysis

### Notebook work
Notebook-based exploration lives in:
- `wind_impact_analysis/`

This is where feature experiments, model comparisons, and interpretation work happen.

### Current preferred production model
The current preferred modeling direction is:
- one-stage CatBoost
- round-level grain
- trained from `gold_wind_model_inputs/model_inputs_round`
- artifactized by `train_round_wind_model`

### Why round level
The round-level model gives a good balance of:
- descriptive interpretability
- manageable feature engineering
- subgroup analysis by:
  - course
  - layout
  - division
  - player rating
- wind sensitivity analysis through counterfactual scoring

### Current production boundary
At the moment:
- feature preparation is productionized in `gold_wind_model_inputs`
- training is productionized in `train_round_wind_model`
- scoring/inference is the next modeling step to formalize

---

## Documentation

Project-specific documentation lives under:
- `docs/schemas/`
- `docs/walkthroughs/`

Relevant current docs include:
- `docs/schemas/gold_wind_model_inputs_schema.md`
- `docs/walkthroughs/gold_wind_model_inputs_code_walkthrough.md`
- `docs/schemas/train_round_wind_model_schema.md`
- `docs/walkthroughs/train_round_wind_model_code_walkthrough.md`

These are intended to document:
- schema contracts
- transform/training behavior
- incremental/idempotent runner logic
- artifact and checkpoint contracts

---

## Local Development

### Python environment
A virtual environment is recommended.

Example:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
```

If you prefer using pinned dependencies, use your normal lockfile-driven workflow.

### Environment variables
Runtime configuration is loaded from package-specific config modules and environment variables.

Typical examples include:
- S3 bucket name
- DynamoDB table name
- AWS region

See:
- `.env.example`
- package `config.py` files

---

## Docker

The repo includes Docker support under:
- `docker/dockerfiles/`
- `docker/entrypoints/`

Example relevant images:
- `docker/dockerfiles/Dockerfile.gold_wind_model_inputs`
- `docker/dockerfiles/Dockerfile.train_round_wind_model`

This supports running pipeline steps in containerized environments consistent with the AWS-native design.

---

## Typical End-to-End Run Sequence

A representative historical or incremental flow looks like:

```powershell
python -m ingest_pdga_event_pages.runner --incremental
python -m ingest_pdga_live_results.runner --historical-backfill
python -m silver_pdga_live_results.runner --run-mode pending_only --progress-every 25
python -m ingest_weather_observations.runner --incremental --progress-every 25
python -m silver_weather_observations.runner --run-mode pending_only --progress-every 25
python -m silver_weather_enriched.runner --run-mode pending_only --progress-every 25
python -m gold_wind_effects.runner --run-mode pending_only --progress-every 25
python -m gold_wind_model_inputs.runner --run-mode pending_only --progress-every 25
python -m train_round_wind_model.runner --log-level INFO
```

For targeted rebuilds, use:
- `--event-ids`
- `--force-events`
- `--run-mode full_check`
- `--force-train`

---

## Testing

Run the full test suite:

```powershell
python -m pytest -v
```

Run tests for a specific package:

```powershell
python -m pytest tests/ingest_pdga_event_pages -v
python -m pytest tests/ingest_pdga_live_results -v
python -m pytest tests/ingest_weather_observations -v
python -m pytest tests/silver_pdga_live_results -v
python -m pytest tests/silver_weather_observations -v
python -m pytest tests/silver_weather_enriched -v
python -m pytest tests/gold_wind_effects -v
python -m pytest tests/gold_wind_model_inputs -v
python -m pytest tests/train_round_wind_model -v
```

Fast focused loops:

```powershell
python -m pytest tests/gold_wind_model_inputs -q
python -m pytest tests/train_round_wind_model -q
```

---

## Design Principles

This project is intentionally built around a few production-style principles:

- idempotent event-level reruns
- deterministic outputs and hashes
- versioned transforms and model artifacts
- quarantine on data-quality failure
- clear Bronze / Silver / Gold / Modeling separation
- reusable fact layers before model-specific feature layers
- notebook experimentation feeding back into production contracts only after validation

---

## Near-Term Roadmap

The next major production steps are:

1. stabilize `gold_wind_model_inputs` round-only schema and DQ behavior
2. stabilize `train_round_wind_model` training artifact and checkpoint contract
3. build a production `score_round_wind_model` step
4. write scored output tables for downstream analysis and reporting
5. keep tightening docs so the pipeline remains understandable and reproducible

---

## Notes

This repo is intentionally being developed as both:
- a portfolio-grade analytics project
- a production-style data engineering and modeling system

That means notebook-based exploration still exists, but the direction of travel is:

- move stable logic into versioned pipeline code
- keep exploration in notebooks
- keep production data contracts explicit and testable
- keep training and, later, scoring as first-class pipeline steps
```