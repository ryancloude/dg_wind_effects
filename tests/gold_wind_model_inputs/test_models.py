from gold_wind_model_inputs.models import (
    GOLD_MODEL_INPUTS_CHECKPOINT_PK,
    MODEL_INPUTS_HOLE_REQUIRED_COLS,
    MODEL_INPUTS_POLICY_VERSION,
    MODEL_INPUTS_ROUND_REQUIRED_COLS,
    PIPELINE_NAME,
)


def test_model_constants_exist():
    assert PIPELINE_NAME == "gold_wind_model_inputs"
    assert GOLD_MODEL_INPUTS_CHECKPOINT_PK == "PIPELINE#GOLD_WIND_MODEL_INPUTS"
    assert MODEL_INPUTS_POLICY_VERSION == "v1"


def test_required_columns_include_core_fields():
    assert "target_strokes_over_par" in MODEL_INPUTS_HOLE_REQUIRED_COLS
    assert "weather_available_flag" in MODEL_INPUTS_HOLE_REQUIRED_COLS
    assert "row_hash_sha256" in MODEL_INPUTS_HOLE_REQUIRED_COLS

    assert "target_strokes_over_par" in MODEL_INPUTS_ROUND_REQUIRED_COLS
    assert "weather_available_flag" in MODEL_INPUTS_ROUND_REQUIRED_COLS
    assert "row_hash_sha256" in MODEL_INPUTS_ROUND_REQUIRED_COLS