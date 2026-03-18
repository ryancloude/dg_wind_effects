from __future__ import annotations

PIPELINE_NAME = "gold_wind_model_inputs"
GOLD_MODEL_INPUTS_CHECKPOINT_PK = "PIPELINE#GOLD_WIND_MODEL_INPUTS"

# Bump when transform/business logic changes and should force recompute.
MODEL_INPUTS_POLICY_VERSION = "v1"

MODEL_INPUTS_ROUND_REQUIRED_COLS = (
    "event_year",
    "tourn_id",
    "round_number",
    "player_key",
    "model_inputs_grain",
    "model_inputs_version",
    "target_strokes_over_par",
    "weather_available_flag",
    "row_hash_sha256",
)

MODEL_INPUTS_HOLE_REQUIRED_COLS = (
    "event_year",
    "tourn_id",
    "round_number",
    "hole_number",
    "player_key",
    "model_inputs_grain",
    "model_inputs_version",
    "target_strokes_over_par",
    "weather_available_flag",
    "row_hash_sha256",
)