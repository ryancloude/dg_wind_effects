# Score Round Wind Model Code Walkthrough

## Module Overview

Package:
- `score_round_wind_model`

Core modules:
- `config.py`
  - loads S3 bucket, DynamoDB table, and AWS region config
- `models.py`
  - defines pipeline constants, scoring policy version, output prefixes, and unit constants
- `gold_io.py`
  - lists canonical round model-input parquet files and loads event-level dataframes
- `model_io.py`
  - loads the trained CatBoost model artifact and training metadata
- `scoring.py`
  - prepares the scoring dataframe
  - generates reference counterfactual dataframes
  - produces observed and counterfactual predictions
  - computes impact fields
- `parquet_io.py`
  - writes deterministic scored parquet outputs
- `dynamo_io.py`
  - reads/writes event-level scoring checkpoints and run summaries
- `runner.py`
  - orchestrates end-to-end scoring

---

## Why This Step Exists

`gold_wind_model_inputs` gives the project a stable round-level feature dataset.
`train_round_wind_model` gives the project a stable trained model artifact.

`score_round_wind_model` exists to connect those two pieces and generate row-level production outputs that quantify weather impact.

This package is the bridge between:
- model artifact creation
and
- analytics-ready, interpretable weather-impact outputs

It is the step that turns a trained model into usable per-round impact estimates.

---

## End-to-End Flow

## 1. Load config
The runner reads:
- S3 bucket
- DynamoDB table
- AWS region

from environment-driven config.

## 2. Load the model artifact bundle
The runner requires:
- `--training-request-fingerprint`

Using that fingerprint, `model_io.py` loads:
- `model.cbm`
- `training_manifest.json`
- `feature_columns.json`
- `categorical_feature_columns.json`

This ensures scoring is tied to one explicit training artifact.

## 3. List candidate event inputs
`gold_io.py` lists round model-input parquet files from:

- `gold/pdga/wind_effects/model_inputs_round/...`

The listing can be restricted with:
- `--event-ids`

Each event parquet file becomes one incremental scoring unit.

## 4. Compute a scoring request fingerprint
For each event, the scoring step computes a deterministic scoring request fingerprint from:
- event parquet object metadata
- selected training request fingerprint
- scoring policy version

This fingerprint answers:
- has the scoring input changed for this event/model combination?

## 5. Check DynamoDB for an existing success
The runner checks DynamoDB for a checkpoint with:
- `pk = PIPELINE#SCORE_ROUND_WIND_MODEL`
- `sk = EVENT#<event_id>#MODEL#<training_request_fingerprint>`

If the checkpoint already exists, the event is skipped unless:
- `--force-events`
or
- the scoring request fingerprint changed

This keeps scoring incremental and idempotent.

## 6. Load the event dataframe
For events that need scoring, `gold_io.py` loads one event’s model-input parquet rows into a pandas dataframe.

This keeps memory usage aligned with the event-level incremental model rather than loading the entire history at once.

## 7. Prepare the scoring dataframe
`scoring.py` verifies that:
- required scoring columns exist
- required model feature columns exist

It then:
- coerces numeric features
- drops rows with nulls in required numeric model feature columns
- standardizes categorical columns to string
- fills missing categorical values with `__MISSING__`

This logic mirrors the training contract so scoring uses the model consistently.

## 8. Predict observed round scores
The scoring step builds a CatBoost `Pool` using:
- feature columns from the training artifact
- categorical feature columns from the training artifact

It then predicts:
- `predicted_round_strokes`

under observed conditions.

## 9. Build reference counterfactuals
The scoring step builds three modified copies of the event dataframe:

### Wind reference dataframe
Changes only:
- wind speed / gust fields
- wind bucket fields

### Temperature reference dataframe
Changes only:
- temperature field

### Total weather reference dataframe
Changes:
- wind
- gust
- wind buckets
- temperature
- precip
- pressure
- humidity

This produces the counterfactual states needed for the three impact measures.

## 10. Predict counterfactual scores
The model predicts:
- `predicted_round_strokes_wind_reference`
- `predicted_round_strokes_temperature_reference`
- `predicted_round_strokes_total_weather_reference`

These predictions are then compared against the observed prediction.

## 11. Compute impact fields
The scoring step computes:
- `estimated_wind_impact_strokes`
- `estimated_temperature_impact_strokes`
- `estimated_total_weather_impact_strokes`

Each is a delta between the observed prediction and the relevant reference prediction.

## 12. Write scored parquet output
`parquet_io.py` writes the scored event output to:

- `gold/pdga/wind_effects/scored_rounds/event_year=<YYYY>/tourn_id=<ID>/scored_rounds.parquet`

This is a deterministic overwrite path.

## 13. Write checkpoint and run summary
After a successful event scoring write, the runner records a success checkpoint in DynamoDB.

At the end of the run, it writes a run summary with:
- events attempted
- events processed
- events skipped
- events failed
- total rows scored

---

## Core Design Choices

## Explicit model selection
A major design choice is that scoring requires an explicit training fingerprint.

The step does not try to discover “latest” or “best” automatically.

Why this is good:
- fully reproducible
- easier debugging
- safer in production
- easier to compare outputs from different model versions later

## Event-level scoring
Scoring is incremental at the event level.

Why:
- aligns with the rest of the pipeline
- makes checkpointing clean
- avoids scanning the full history on every run
- keeps reruns targeted and idempotent

## Counterfactual-based interpretation
The step estimates weather impact by comparing predictions under:
- observed weather
and
- reference weather states

This was chosen because it gives directly interpretable row-level outputs that downstream reporting can use.

## Separate wind, temperature, and total-weather impacts
The scoring step intentionally produces three distinct impact families:
- wind
- temperature
- total weather

This makes the downstream reporting and dashboard much more informative than a wind-only design.

---

## Module Walkthrough

## `models.py`
Defines:
- pipeline name
- checkpoint PK
- scoring policy version
- scored output prefix
- model-input prefix
- conversion constants

This is where scoring policy identity is anchored.

## `gold_io.py`
Responsible for:
- listing round model-input parquet objects
- filtering by event IDs
- loading one event’s model-input dataframe

This isolates S3 data access from scoring logic.

## `model_io.py`
Responsible for:
- locating the artifact prefix from the training fingerprint
- loading the CatBoost model binary
- loading training manifest and feature manifests

This isolates artifact access and keeps scoring artifact-driven.

## `scoring.py`
This is the core logic of the package.

Responsibilities:
- `compute_scoring_request_fingerprint(...)`
- `prepare_scoring_dataframe(...)`
- build observed and reference dataframes
- run model predictions
- compute impact fields
- return a structured `ScoringResult`

This module owns the counterfactual scoring policy.

## `parquet_io.py`
Responsible for:
- deterministic scored output key construction
- parquet serialization
- event-level overwrite behavior

## `dynamo_io.py`
Responsible for:
- `get_score_checkpoint(...)`
- `put_score_checkpoint(...)`
- `put_score_run_summary(...)`

This module keeps scoring checkpoint concerns separate from runner logic.

## `runner.py`
The orchestration entrypoint.

Responsibilities:
- parse CLI args
- load config
- load model bundle
- list event input files
- check incremental skip state
- load and score each event
- write scored parquet outputs
- write checkpoints and run summary
- emit timing logs

---

## Counterfactual Policy

## Wind reference
Purpose:
- isolate wind contribution while leaving other weather fixed

Reference:
- `2 mph` wind
- `3 mph` gust

## Temperature reference
Purpose:
- isolate temperature contribution while leaving other weather fixed

Reference:
- `12 C`

## Total weather reference
Purpose:
- estimate the overall contribution of weather-related conditions

Reference:
- low wind
- low gust
- `12 C`
- zero precip
- pressure median from training
- humidity median from training

This policy reflects the project’s chosen interpretation framework and is embedded in scoring.

---

## Output Contract

Scored outputs are written to:

- `gold/pdga/wind_effects/scored_rounds/...`

These outputs are intended to be the source of truth for:
- detailed event exploration
- aggregate reporting
- dashboard drill-downs

They are not yet the dashboard-ready semantic layer.
That layer is built later by `report_round_weather_impacts`.

---

## Incremental Behavior

This step is incremental at the event + model level.

### Skip rule
If an event was already scored successfully for the same model and the scored input fingerprint has not changed, the event is skipped.

### Force rescoring
`--force-events` bypasses the skip rule.

### Why this works
The event-level scored parquet file is deterministic.
So rescoring is safe to repeat and does not create duplicate logical outputs.

---

## Typical Commands

Run unit tests:
```powershell
python -m pytest tests/score_round_wind_model -v
