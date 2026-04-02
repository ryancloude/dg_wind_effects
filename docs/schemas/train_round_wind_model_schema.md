# Train Round Wind Model Schema

## Purpose
`train_round_wind_model` is the production training step for the round-level one-stage CatBoost wind model.

It consumes the canonical round-level model-input dataset from:
- `gold_wind_model_inputs`

and produces:
- a trained CatBoost model artifact
- training metadata
- evaluation metrics
- feature manifests
- feature importance output
- a single DynamoDB checkpoint per training request fingerprint

This step does not score new rows. It only trains and persists the production model artifact bundle.

---

## Input Contract

### Source dataset
The training step reads round-level parquet rows from:

- `gold/pdga/wind_effects/model_inputs_round/event_year=<YYYY>/tourn_id=<ID>/model_inputs_round.parquet`

The input contract assumes the schema produced by `gold_wind_model_inputs`.

### Required input columns
The training step expects these fields to exist on the round model-input rows:

- `event_year`
- `tourn_id`
- `round_number`
- `player_key`
- `course_id`
- `layout_id`
- `division`
- `player_rating`
- `actual_round_strokes`
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
- `row_hash_sha256`

### Training-time filtering
The training step applies additional model-specific filtering after load.

Current filtering behavior:
- require `weather_available_flag = True`
- coerce numeric feature columns to numeric
- drop rows with nulls in required numeric features or target

This filtering is part of the training recipe, not the upstream data contract.

---

## Model Contract

### Model identity
- `model_name = round_one_stage_catboost`
- `model_version = v1`

### Target column
- `actual_round_strokes`

### Numeric feature columns
- `player_rating`
- `round_number`
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

### Categorical feature columns
- `course_id`
- `round_wind_speed_bucket`
- `round_wind_gust_bucket`
- `division`
- `layout_id`

### Split strategy
Current production training split:
- row-level random split
- train / validation / test
- validation set used for CatBoost early stopping
- test set used for final evaluation metrics

### Current CatBoost training recipe
High-level settings:
- `loss_function = RMSE`
- `eval_metric = RMSE`
- `iterations = 20000`
- `depth = 6`
- `learning_rate = 0.05`
- `bootstrap_type = Bernoulli`
- `subsample = 0.8`
- `early_stopping_rounds = 500`

The full effective recipe is encoded in the training request fingerprint.

---

## Dataset Fingerprint

The training step computes a dataset fingerprint from the loaded round rows.

Current method:
- collect `row_hash_sha256` values from input rows
- sort them deterministically
- hash the resulting payload

This allows the training step to know whether the effective training dataset changed.

---

## Training Request Fingerprint

The training step computes a deterministic training request fingerprint from:

- `model_name`
- `model_version`
- `dataset_fingerprint`
- target column
- numeric feature columns
- categorical feature columns
- weather-availability requirement
- train/validation/test split settings
- random seed
- CatBoost hyperparameters
- optional `event_ids` subset

This fingerprint is the canonical identity of one training request.

It is used for:
- incremental skip behavior
- artifact pathing
- DynamoDB checkpoint identity

---

## S3 Artifact Contract

Artifacts are written under:

- `artifacts/pdga/wind_effects/models/round_one_stage_catboost/model_name=<MODEL_NAME>/model_version=<MODEL_VERSION>/training_fingerprint=<FINGERPRINT>/`

### Artifact files
Expected files include:
- `model.cbm`
- `training_manifest.json`
- `metrics.json`
- `feature_columns.json`
- `categorical_feature_columns.json`
- `feature_importance.csv`

### `model.cbm`
Binary CatBoost model artifact.

### `training_manifest.json`
Full training metadata and recipe manifest.

Expected contents include:
- `model_name`
- `model_version`
- `target_col`
- feature column lists
- `dataset_fingerprint`
- `training_request_fingerprint`
- source key count
- optional event subset
- split settings
- CatBoost hyperparameters
- row counts
- evaluation metrics
- best iteration

### `metrics.json`
Compact evaluation metrics for the trained model.

Expected fields include:
- `mae`
- `rmse`
- `r2`
- `best_iteration`
- row counts for:
  - input rows
  - filtered rows
  - train rows
  - validation rows
  - test rows

### `feature_columns.json`
Ordered full feature list used during training.

### `categorical_feature_columns.json`
Ordered categorical feature list used during training.

### `feature_importance.csv`
Feature importance export with columns:
- `feature`
- `importance`

---

## DynamoDB Checkpoint Contract

### Checkpoint family
- `pk = PIPELINE#TRAIN_ROUND_WIND_MODEL`
- `sk = TRAINING#<training_request_fingerprint>`

### Checkpoint purpose
This stores one canonical checkpoint item per unique training request.

That means the same effective training request should not produce multiple checkpoint records.

### Checkpoint fields
Expected fields include:
- `pipeline`
- `training_request_fingerprint`
- `status`
- `last_run_id`
- `updated_at`
- `model_name`
- `model_version`
- `dataset_fingerprint`
- `artifact_prefix`
- `model_key`
- `metrics_key`
- `manifest_key`
- key metrics such as:
  - `mae`
  - `rmse`
  - `r2`
  - `best_iteration`

### Status values
Typical statuses:
- `success`
- `failed`

### Run summary
A per-run summary item is also written with:
- `pk = RUN#<run_id>`
- `sk = TRAIN_ROUND_WIND_MODEL#SUMMARY`

---

## Incremental and Idempotent Behavior

The training step is incremental at the training-request level.

### Skip behavior
If a checkpoint already exists for the same training request fingerprint and its status is `success`, training is skipped unless:
- `--force-train` is provided

### Idempotent behavior
The artifact prefix is deterministic from:
- model name
- model version
- training request fingerprint

So rerunning the same training request writes to the same logical artifact location and checkpoint identity.

This keeps the step safe to rerun without creating multiple logically duplicate training records.

---

## Evaluation Outputs

The current training step evaluates on the held-out test split and records:

- `mae`
- `rmse`
- `r2`
- `best_iteration`

These are descriptive training outputs and are stored in:
- `metrics.json`
- `training_manifest.json`
- DynamoDB checkpoint metadata

---

## Relationship to Upstream and Downstream Steps

### Upstream
- `gold_wind_model_inputs`
  - produces the canonical round-level feature table

### Current step
- `train_round_wind_model`
  - trains the production one-stage round CatBoost model

### Downstream
Planned downstream step:
- `score_round_wind_model`
  - load trained model artifact
  - score round model-input rows
  - write predictions and wind-effect outputs

---

## Notes on Scope

This step intentionally does not:
- rebuild model inputs
- perform notebook experimentation
- compare multiple candidate model recipes
- score calm counterfactuals
- publish charts

Its responsibility is:
- stable training
- stable artifact creation
- stable metadata/checkpointing
