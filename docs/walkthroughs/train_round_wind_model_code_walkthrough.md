# Train Round Wind Model Code Walkthrough

## Module Overview
Package: `train_round_wind_model`

Core modules:
- `config.py`
  - loads S3 bucket, DynamoDB table, and AWS region config
- `models.py`
  - defines model identity, feature contract, split settings, and CatBoost parameters
- `gold_io.py`
  - loads round model-input parquet rows from S3
- `training.py`
  - prepares training data
  - computes fingerprints
  - fits the CatBoost model
  - computes metrics and feature importance
- `artifact_io.py`
  - writes model and metadata artifacts to S3
- `dynamo_io.py`
  - reads/writes training checkpoints and run summaries in DynamoDB
- `runner.py`
  - orchestrates end-to-end training

---

## Why This Step Exists

`gold_wind_model_inputs` gives the project a stable, canonical round-level feature table.

`train_round_wind_model` exists to convert that feature table into a production model artifact bundle.

This separation is intentional:

- `gold_wind_model_inputs`
  - owns the feature contract
- `train_round_wind_model`
  - owns the training recipe and artifact outputs

That keeps the training logic flexible without constantly changing upstream data contracts.

---

## End-to-End Flow

### 1. Load config
The runner reads:
- S3 bucket
- DynamoDB table
- AWS region

from environment-driven config.

### 2. Load round model-input rows
The runner loads round rows from:

- `gold/pdga/wind_effects/model_inputs_round/...`

This is done through `gold_io.py`.

The loader:
- lists relevant parquet keys under the model-input round prefix
- optionally filters by `--event-ids`
- reads parquet rows into memory
- returns both:
  - loaded rows
  - source key list

The source key list is included in the training manifest for lineage.

### 3. Compute dataset fingerprint
The training logic computes a deterministic dataset fingerprint from the sorted set of `row_hash_sha256` values present in the loaded round rows.

This fingerprint answers:
- did the effective training dataset change?

### 4. Compute training request fingerprint
The training logic then combines:
- dataset fingerprint
- model version
- feature lists
- split settings
- CatBoost hyperparameters
- optional event subset

into a deterministic training request fingerprint.

This fingerprint answers:
- is this training request meaningfully different from one that already ran?

### 5. Check DynamoDB for existing success
The runner checks DynamoDB for a checkpoint with:

- `pk = PIPELINE#TRAIN_ROUND_WIND_MODEL`
- `sk = TRAINING#<training_request_fingerprint>`

If that checkpoint already exists with `status = success`, the run is skipped unless:
- `--force-train`

This keeps training incremental and avoids redundant artifact writes.

### 6. Prepare the training dataframe
`training.py` converts the loaded row list into a pandas dataframe and applies training-time preparation.

Current steps:
- verify required input columns exist
- optionally require `weather_available_flag = True`
- coerce numeric features and target to numeric
- drop rows with nulls in numeric features or target
- standardize categorical columns and replace unknown levels with `__MISSING__`

This is where model-specific eligibility filtering lives.

### 7. Split train / validation / test
The training step performs a row-level split:
- train / test
- then train / validation inside the training subset

Validation is used for:
- CatBoost early stopping

Test is used for:
- final evaluation metrics

### 8. Train CatBoost
`training.py` fits the production one-stage round CatBoost model using:
- the fixed numeric feature set
- the fixed categorical feature set
- the configured CatBoost parameters from `models.py`

The current production model is the one-stage round model without explicit wind interaction features.

### 9. Evaluate the model
After training, the step computes:
- `mae`
- `rmse`
- `r2`
- `best_iteration`

These metrics are computed on the held-out test set.

### 10. Compute feature importance
Feature importance is computed from the trained model and exported as a simple table:
- `feature`
- `importance`

This gives a basic model-interpretation artifact for debugging and documentation.

### 11. Write artifact bundle to S3
`artifact_io.py` writes the training outputs under a deterministic artifact prefix.

Artifacts include:
- `model.cbm`
- `training_manifest.json`
- `metrics.json`
- `feature_columns.json`
- `categorical_feature_columns.json`
- `feature_importance.csv`

The prefix is derived from:
- model name
- model version
- training request fingerprint

### 12. Write training checkpoint and run summary
On success, the runner writes:
- one training checkpoint keyed by the training request fingerprint
- one run summary keyed by the run ID

This preserves:
- incremental skip behavior
- traceability
- operational observability

---

## Core Design Choices

## One checkpoint per training request
A major design choice in this package is that it does not write multiple logical checkpoints for the same effective model request.

The checkpoint identity is:
- the training request fingerprint

That fingerprint represents:
- source dataset
- feature contract
- training recipe
- model version

So a repeated training request should map to one stable checkpoint item.

This is different from an event-processing pipeline step, but it is the right fit for model training.

## `training.py` instead of `transform.py`
The package uses `training.py` because this step’s primary job is model fitting, not dataset transformation.

`training.py` owns:
- training dataframe prep
- fingerprints
- CatBoost fitting
- metrics
- feature importance

That naming makes the step easier to understand than a generic `transform.py`.

## Training from canonical S3 model-input data
The training step does not rebuild notebook exports or recompute round features itself.
It trusts `gold_wind_model_inputs` as the source of truth.

This gives a clean production boundary.

## Fixed recipe, not experiment framework
The training step is intentionally narrow.
It trains the current production model recipe only.

That means:
- one target column
- one feature contract
- one CatBoost recipe
- one artifact bundle format

This is deliberate.
The goal is to productionize the chosen model, not to recreate notebook experimentation infrastructure.

---

## Module Walkthrough

## `models.py`
Defines the stable contract for training:
- model identity
- artifact base prefix
- target column
- numeric feature list
- categorical feature list
- required input columns
- split settings
- CatBoost hyperparameters

This is the file that makes the training recipe explicit and versionable.

## `gold_io.py`
Loads round model-input parquet rows from S3.

Key responsibilities:
- list parquet keys under the model-input round prefix
- optionally filter by `event_ids`
- read parquet rows into Python dictionaries

This module keeps S3-loading concerns out of the training logic.

## `training.py`
This is the core of the package.

Responsibilities:
- `compute_dataset_fingerprint(...)`
- `compute_training_request_fingerprint(...)`
- `prepare_training_dataframe(...)`
- `train_round_model(...)`

The output of `train_round_model(...)` is a structured `TrainingResult` containing:
- trained model object
- metrics
- training manifest
- feature importance rows

## `artifact_io.py`
Writes training outputs to S3.

Responsibilities:
- build deterministic artifact prefix
- save CatBoost binary model
- save JSON metadata files
- save feature importance CSV

This keeps artifact persistence separate from training logic.

## `dynamo_io.py`
Handles DynamoDB interactions.

Responsibilities:
- `get_training_checkpoint(...)`
- `put_training_checkpoint(...)`
- `put_training_run_summary(...)`

This module enforces the single-checkpoint-per-training-request pattern.

## `runner.py`
Top-level orchestration.

Responsibilities:
- parse CLI args
- load config
- load input rows
- compute fingerprints
- perform incremental skip check
- call training
- write artifacts
- write checkpoint and run summary
- emit logs and printed summaries

---

## Artifact Contract

The artifact prefix is deterministic and based on:
- `model_name`
- `model_version`
- `training_request_fingerprint`

A typical artifact prefix looks like:

- `artifacts/pdga/wind_effects/models/round_one_stage_catboost/model_name=round_one_stage_catboost/model_version=v1/training_fingerprint=<FINGERPRINT>/`

Files under that prefix:
- `model.cbm`
- `training_manifest.json`
- `metrics.json`
- `feature_columns.json`
- `categorical_feature_columns.json`
- `feature_importance.csv`

This artifact contract is meant to be consumed later by:
- `score_round_wind_model`

---

## Incremental Behavior

This step is incremental at the training-request level.

### Training skip rule
If the same training request fingerprint already has a `success` checkpoint, the runner skips retraining.

### Force retrain
`--force-train` bypasses the skip rule and retrains anyway.

### Why this works
The training request fingerprint captures:
- data state
- feature contract
- model recipe

So if none of those changed, a new training run would be logically redundant.

---

## Typical Commands

Run unit tests:
```powershell
python -m pytest tests/train_round_wind_model -v
