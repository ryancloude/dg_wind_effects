from __future__ import annotations

PIPELINE_NAME = "train_round_wind_model"
TRAINING_CHECKPOINT_PK = "PIPELINE#TRAIN_ROUND_WIND_MODEL"

MODEL_NAME = "round_one_stage_catboost_monotone"
MODEL_VERSION = "v4"

MODEL_INPUTS_ROUND_PREFIX = "gold/pdga/wind_effects/model_inputs_round/"
ARTIFACT_BASE_PREFIX = "artifacts/pdga/wind_effects/models/round_one_stage_catboost_monotone/"

TARGET_COL = "actual_round_strokes"

NUMERIC_FEATURES = (
    "player_rating",
    "round_number",
    "hole_count",
    "round_total_hole_length",
    "round_avg_hole_length",
    "round_total_par",
    "round_avg_hole_par",
    "round_length_over_par",
    "round_wind_speed_mps_mean",
    "round_wind_gust_mps_mean",
    "round_temp_c_mean",
    "precip_during_round_flag",
)

CATEGORICAL_FEATURES = (
    "course_id",
    "division",
)

FEATURE_COLUMNS = NUMERIC_FEATURES + CATEGORICAL_FEATURES

REQUIRED_INPUT_COLS = (
    "event_year",
    "tourn_id",
    "round_number",
    "player_key",
    "course_id",
    "layout_id",
    "division",
    "player_rating",
    "actual_round_strokes",
    "weather_available_flag",
    "hole_count",
    "round_total_hole_length",
    "round_avg_hole_length",
    "round_total_par",
    "round_avg_hole_par",
    "round_length_over_par",
    "round_wind_speed_mps_mean",
    "round_wind_gust_mps_mean",
    "round_temp_c_mean",
    "round_precip_mm_sum",
    "row_hash_sha256",
)

REQUIRE_WEATHER_AVAILABLE = True
MIN_HOLES_PLAYED = 18

TEST_SIZE = 0.20
VALID_SIZE_WITHIN_TRAIN = 0.20
RANDOM_STATE = 42
EARLY_STOPPING_ROUNDS = 300

REFERENCE_WIND_SPEED_MPH = 0.0
REFERENCE_WIND_GUST_MPH = 1.0
REFERENCE_TEMPERATURE_F = 80.0
REFERENCE_TEMPERATURE_C = (REFERENCE_TEMPERATURE_F - 32.0) * 5.0 / 9.0
REFERENCE_PRECIP_FLAG = 0

MONOTONE_CONSTRAINT_MAP = {
    "player_rating": 0,
    "round_number": 0,
    "hole_count": 0,
    "round_total_hole_length": 0,
    "round_avg_hole_length": 0,
    "round_total_par": 0,
    "round_avg_hole_par": 0,
    "round_length_over_par": 0,
    "round_wind_speed_mps_mean": 1,
    "round_wind_gust_mps_mean": 1,
    "round_temp_c_mean": 0,
    "precip_during_round_flag": 0,
    "course_id": 0,
    "division": 0,
}

# CatBoost requires a list (or dict/string), not a tuple.
MONOTONE_CONSTRAINTS = [MONOTONE_CONSTRAINT_MAP[feature] for feature in FEATURE_COLUMNS]

CATBOOST_PARAMS = {
    "loss_function": "RMSE",
    "eval_metric": "RMSE",
    "iterations": 10000,
    "depth": 6,
    "learning_rate": 0.03,
    "l2_leaf_reg": 10.0,
    "random_seed": RANDOM_STATE,
    "verbose": 250,
    "bootstrap_type": "Bernoulli",
    "subsample": 0.8,
    "max_ctr_complexity": 1,
    "store_all_simple_ctr": False,
    "task_type": "CPU",
    "monotone_constraints": MONOTONE_CONSTRAINTS,
}
