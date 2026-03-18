from __future__ import annotations

PIPELINE_NAME = "gold_wind_effects"
GOLD_CHECKPOINT_PK = "PIPELINE#GOLD_WIND_EFFECTS"

# Bump whenever transform logic changes and should force recompute.
GOLD_POLICY_VERSION = "v1"

ROUND_PK_COLS = ("tourn_id", "round_number", "player_key")
HOLE_PK_COLS = ("tourn_id", "round_number", "hole_number", "player_key")

REQUIRED_GOLD_ROUND_COLS = (
    "gold_grain",
    "gold_model_version",
    "gold_run_id",
    "gold_processed_at_utc",
    "actual_strokes",
    "strokes_over_par",
    "weather_available_flag",
    "wind_speed_bucket",
    "row_hash_sha256",
)

REQUIRED_GOLD_HOLE_COLS = (
    "gold_grain",
    "gold_model_version",
    "gold_run_id",
    "gold_processed_at_utc",
    "actual_strokes",
    "strokes_over_par",
    "weather_available_flag",
    "wind_speed_bucket",
    "row_hash_sha256",
)