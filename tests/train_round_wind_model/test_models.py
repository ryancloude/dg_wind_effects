from train_round_wind_model.models import (
    ARTIFACT_BASE_PREFIX,
    CATEGORICAL_FEATURES,
    FEATURE_COLUMNS,
    MIN_HOLES_PLAYED,
    MODEL_INPUTS_ROUND_PREFIX,
    MODEL_NAME,
    MODEL_VERSION,
    MONOTONE_CONSTRAINTS,
    NUMERIC_FEATURES,
    PIPELINE_NAME,
    REFERENCE_PRECIP_FLAG,
    REFERENCE_TEMPERATURE_F,
    REFERENCE_WIND_GUST_MPH,
    REFERENCE_WIND_SPEED_MPH,
    TARGET_COL,
    TRAINING_CHECKPOINT_PK,
)


def test_model_constants_exist():
    assert PIPELINE_NAME == "train_round_wind_model"
    assert TRAINING_CHECKPOINT_PK == "PIPELINE#TRAIN_ROUND_WIND_MODEL"
    assert MODEL_NAME == "round_one_stage_catboost_monotone"
    assert MODEL_VERSION == "v4"
    assert TARGET_COL == "actual_round_strokes"
    assert MODEL_INPUTS_ROUND_PREFIX == "gold/pdga/wind_effects/model_inputs_round/"
    assert ARTIFACT_BASE_PREFIX.startswith("artifacts/pdga/wind_effects/models/")


def test_feature_sets_match_one_stage_v4_model():
    assert NUMERIC_FEATURES == (
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

    assert CATEGORICAL_FEATURES == (
        "course_id",
        "division",
    )

    assert FEATURE_COLUMNS == NUMERIC_FEATURES + CATEGORICAL_FEATURES


def test_reference_weather_constants_match_notebook():
    assert REFERENCE_WIND_SPEED_MPH == 0.0
    assert REFERENCE_WIND_GUST_MPH == 1.0
    assert REFERENCE_TEMPERATURE_F == 80.0
    assert REFERENCE_PRECIP_FLAG == 0
    assert MIN_HOLES_PLAYED == 18


def test_monotone_constraints_only_apply_to_wind_speed_and_gust():
    assert isinstance(MONOTONE_CONSTRAINTS, list)
    assert len(MONOTONE_CONSTRAINTS) == len(FEATURE_COLUMNS)

    constraint_by_feature = dict(zip(FEATURE_COLUMNS, MONOTONE_CONSTRAINTS))

    assert constraint_by_feature["round_wind_speed_mps_mean"] == 1
    assert constraint_by_feature["round_wind_gust_mps_mean"] == 1

    for feature_name, constraint in constraint_by_feature.items():
        if feature_name not in {"round_wind_speed_mps_mean", "round_wind_gust_mps_mean"}:
            assert constraint == 0
