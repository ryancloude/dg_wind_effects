from __future__ import annotations

PIPELINE_NAME = "train_round_wind_model"
TRAINING_CHECKPOINT_PK = "PIPELINE#TRAIN_ROUND_WIND_MODEL"

MODEL_NAME = "round_one_stage_catboost"
MODEL_VERSION = "v1"

MODEL_INPUTS_ROUND_PREFIX = "gold/pdga/wind_effects/model_inputs_round/"
ARTIFACT_BASE_PREFIX = "artifacts/pdga/wind_effects/models/round_one_stage_catboost/"

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
    "round_wind_speed_mps_max",
    "round_wind_gust_mps_mean",
    "round_wind_gust_mps_max",
    "round_temp_c_mean",
    "round_precip_mm_sum",
    "round_precip_mm_mean",
    "round_pressure_hpa_mean",
    "round_humidity_pct_mean",
)

CATEGORICAL_FEATURES = (
    "course_id",
    "round_wind_speed_bucket",
    "round_wind_gust_bucket",
    "division",
    "layout_id",
)

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
    "round_wind_speed_mps_max",
    "round_wind_gust_mps_mean",
    "round_wind_gust_mps_max",
    "round_temp_c_mean",
    "round_precip_mm_sum",
    "round_precip_mm_mean",
    "round_pressure_hpa_mean",
    "round_humidity_pct_mean",
    "round_wind_speed_bucket",
    "round_wind_gust_bucket",
    "row_hash_sha256",
)

REQUIRE_WEATHER_AVAILABLE = True

TEST_SIZE = 0.20
VALID_SIZE_WITHIN_TRAIN = 0.20
RANDOM_STATE = 42
EARLY_STOPPING_ROUNDS = 500

CATBOOST_PARAMS = {
    "loss_function": "RMSE",
    "eval_metric": "RMSE",
    "iterations": 20000,
    "depth": 6,
    "learning_rate": 0.05,
    "l2_leaf_reg": 5.0,
    "random_seed": RANDOM_STATE,
    "verbose": 250,
    "bootstrap_type": "Bernoulli",
    "subsample": 0.8,
    "max_ctr_complexity": 1,
    "store_all_simple_ctr": False,
    "task_type": "CPU",
}
