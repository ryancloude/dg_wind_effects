from __future__ import annotations

PIPELINE_NAME = "score_round_wind_model"
SCORE_CHECKPOINT_PK = "PIPELINE#SCORE_ROUND_WIND_MODEL"
SCORE_POLICY_VERSION = "v2"

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
)

MPS_TO_MPH = 2.23694
MPH_TO_MPS = 1.0 / MPS_TO_MPH
