from train_round_wind_model.models import (
    ARTIFACT_BASE_PREFIX,
    CATEGORICAL_FEATURES,
    MODEL_INPUTS_ROUND_PREFIX,
    MODEL_NAME,
    MODEL_VERSION,
    NUMERIC_FEATURES,
    PIPELINE_NAME,
    TARGET_COL,
    TRAINING_CHECKPOINT_PK,
)


def test_model_constants_exist():
    assert PIPELINE_NAME == "train_round_wind_model"
    assert TRAINING_CHECKPOINT_PK == "PIPELINE#TRAIN_ROUND_WIND_MODEL"
    assert MODEL_NAME == "round_one_stage_catboost"
    assert MODEL_VERSION == "v1"
    assert TARGET_COL == "actual_round_strokes"
    assert MODEL_INPUTS_ROUND_PREFIX == "gold/pdga/wind_effects/model_inputs_round/"
    assert ARTIFACT_BASE_PREFIX.startswith("artifacts/pdga/wind_effects/models/")


def test_feature_sets_are_not_empty():
    assert "player_rating" in NUMERIC_FEATURES
    assert "course_id" in CATEGORICAL_FEATURES
