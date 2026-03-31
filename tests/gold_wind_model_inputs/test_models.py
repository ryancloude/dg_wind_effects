from gold_wind_model_inputs.models import (
    GOLD_MODEL_INPUTS_CHECKPOINT_PK,
    MODEL_INPUTS_POLICY_VERSION,
    MODEL_INPUTS_ROUND_REQUIRED_COLS,
    PIPELINE_NAME,
    ROUND_PK_COLS,
)


def test_model_constants_exist():
    assert PIPELINE_NAME == "gold_wind_model_inputs"
    assert GOLD_MODEL_INPUTS_CHECKPOINT_PK == "PIPELINE#GOLD_WIND_MODEL_INPUTS"
    assert MODEL_INPUTS_POLICY_VERSION == "v2"


def test_required_columns_include_core_fields():
    assert ROUND_PK_COLS == ("tourn_id", "round_number", "player_key")

    assert "actual_round_strokes" in MODEL_INPUTS_ROUND_REQUIRED_COLS
    assert "round_strokes_over_par" in MODEL_INPUTS_ROUND_REQUIRED_COLS
    assert "weather_available_flag" in MODEL_INPUTS_ROUND_REQUIRED_COLS
    assert "row_hash_sha256" in MODEL_INPUTS_ROUND_REQUIRED_COLS
    assert "round_total_hole_length" in MODEL_INPUTS_ROUND_REQUIRED_COLS
    assert "round_total_par" in MODEL_INPUTS_ROUND_REQUIRED_COLS
    assert "round_wind_speed_mps_mean" in MODEL_INPUTS_ROUND_REQUIRED_COLS
    assert "round_wind_gust_mps_mean" in MODEL_INPUTS_ROUND_REQUIRED_COLS
    assert "round_temp_c_mean" in MODEL_INPUTS_ROUND_REQUIRED_COLS
    assert "round_wind_speed_bucket" in MODEL_INPUTS_ROUND_REQUIRED_COLS