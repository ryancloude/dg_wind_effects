from __future__ import annotations

PIPELINE_NAME = "score_round_wind_model"
SCORE_CHECKPOINT_PK = "PIPELINE#SCORE_ROUND_WIND_MODEL"
SCORE_POLICY_VERSION = "v1"

MODEL_INPUTS_ROUND_PREFIX = "gold/pdga/wind_effects/model_inputs_round/"
SCORED_ROUNDS_PREFIX = "gold/pdga/wind_effects/scored_rounds/"

REQUIRED_SCORE_INPUT_COLS = (
    "event_year",
    "tourn_id",
    "round_number",
    "player_key",
    "course_id",
    "layout_id",
    "division",
    "player_rating",
    "actual_round_strokes",
    "round_strokes_over_par",
    "hole_count",
    "round_total_hole_length",
    "round_total_par",
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
)

MPS_TO_MPH = 2.23694
MPH_TO_MPS = 1.0 / MPS_TO_MPH
