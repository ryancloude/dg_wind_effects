from __future__ import annotations

PIPELINE_NAME = "gold_wind_model_inputs"
GOLD_MODEL_INPUTS_CHECKPOINT_PK = "PIPELINE#GOLD_WIND_MODEL_INPUTS"

# Bump when transform/business logic changes and should force recompute.
MODEL_INPUTS_POLICY_VERSION = "v2"

ROUND_PK_COLS = ("tourn_id", "round_number", "player_key")

MODEL_INPUTS_ROUND_REQUIRED_COLS = (
    "event_year",
    "tourn_id",
    "round_number",
    "player_key",
    "model_inputs_grain",
    "model_inputs_version",
    "model_inputs_run_id",
    "model_inputs_processed_at_utc",
    "row_hash_sha256",
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
