import numpy as np
import pandas as pd

from score_round_wind_model.scoring import (
    compute_scoring_request_fingerprint,
    prepare_scoring_dataframe,
    score_round_rows,
)


class FakeModel:
    def __init__(self):
        self.call_index = 0

    def predict(self, pool):
        self.call_index += 1
        row_count = pool.num_row()

        if self.call_index == 1:
            return np.full(row_count, 60.0)  # observed prediction
        if self.call_index == 2:
            return np.full(row_count, 58.5)  # wind reference
        if self.call_index == 3:
            return np.full(row_count, 59.25)  # temperature reference
        if self.call_index == 4:
            return np.full(row_count, 59.75)  # precip reference
        if self.call_index == 5:
            return np.full(row_count, 57.75)  # total weather reference
        raise AssertionError("FakeModel.predict called more times than expected")


def _row(**overrides):
    row = {
        "event_year": 2026,
        "tourn_id": 90008,
        "round_number": 1,
        "player_key": "P1",
        "course_id": 101,
        "layout_id": 201,
        "division": "MA3",
        "player_rating": 915,
        "actual_round_strokes": 57,
        "round_strokes_over_par": -3,
        "weather_available_flag": True,
        "hole_count": 18,
        "round_total_hole_length": 9000.0,
        "round_avg_hole_length": 500.0,
        "round_total_par": 60,
        "round_avg_hole_par": 3.33,
        "round_length_over_par": 150.0,
        "round_wind_speed_mps_mean": 4.0,
        "round_wind_gust_mps_mean": 6.0,
        "round_temp_c_mean": 18.0,
        "round_precip_mm_sum": 0.0,
    }
    row.update(overrides)
    return row


def test_compute_scoring_request_fingerprint_deterministic():
    event_object = {"key": "k", "etag": "e", "size": 1, "last_modified": "x"}
    fp1 = compute_scoring_request_fingerprint(event_object=event_object, training_request_fingerprint="train-1")
    fp2 = compute_scoring_request_fingerprint(event_object=event_object, training_request_fingerprint="train-1")
    assert fp1 == fp2


def test_prepare_scoring_dataframe_filters_weather_holes_and_derives_precip_flag():
    df = pd.DataFrame(
        [
            _row(player_key="P1", round_precip_mm_sum=0.0),
            _row(player_key="P2", round_precip_mm_sum=1.2),
            _row(player_key="P3", weather_available_flag=False),
            _row(player_key="P4", hole_count=9),
            _row(player_key="P5", player_rating=None),
        ]
    )

    out_df = prepare_scoring_dataframe(
        df=df,
        feature_columns=[
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
            "course_id",
            "division",
        ],
        categorical_feature_columns=["course_id", "division"],
        require_weather_available=True,
        min_holes_played=18,
    )

    assert len(out_df) == 2

    by_player = out_df.set_index("player_key")
    assert by_player.loc["P1", "precip_during_round_flag"] == 0
    assert by_player.loc["P2", "precip_during_round_flag"] == 1
    assert out_df["course_id"].map(type).eq(str).all()
    assert out_df["division"].map(type).eq(str).all()


def test_score_round_rows_adds_impact_columns_and_normalizes_output_string_columns():
    df = pd.DataFrame([_row()])

    manifest = {
        "model_name": "round_one_stage_catboost_monotone",
        "model_version": "v4",
        "wind_speed_reference_mph": 0.0,
        "wind_gust_reference_mph": 1.0,
        "temperature_reference_c": (80.0 - 32.0) * 5.0 / 9.0,
        "precip_reference_flag": 0,
        "require_weather_available": True,
        "min_holes_played": 18,
    }

    feature_columns = [
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
        "course_id",
        "division",
    ]
    categorical_feature_columns = [
        "course_id",
        "division",
    ]

    result = score_round_rows(
        df=df,
        model=FakeModel(),
        training_manifest=manifest,
        feature_columns=feature_columns,
        categorical_feature_columns=categorical_feature_columns,
        training_request_fingerprint="train-fp",
        scoring_run_id="score-run-1",
        scored_at_utc="2026-04-02T12:00:00Z",
        scoring_request_fingerprint="score-fp",
        model_artifact_prefix="artifacts/prefix/",
    )

    scored_df = result.scored_df
    assert "predicted_round_strokes" in scored_df.columns
    assert "predicted_round_strokes_wind_reference" in scored_df.columns
    assert "predicted_round_strokes_temperature_reference" in scored_df.columns
    assert "predicted_round_strokes_precip_reference" in scored_df.columns
    assert "predicted_round_strokes_total_weather_reference" in scored_df.columns
    assert "estimated_wind_impact_strokes" in scored_df.columns
    assert "estimated_temperature_impact_strokes" in scored_df.columns
    assert "estimated_precip_impact_strokes" in scored_df.columns
    assert "estimated_total_weather_impact_strokes" in scored_df.columns

    row = scored_df.iloc[0]
    assert row["predicted_round_strokes"] == 60.0
    assert row["predicted_round_strokes_wind_reference"] == 58.5
    assert row["predicted_round_strokes_temperature_reference"] == 59.25
    assert row["predicted_round_strokes_precip_reference"] == 59.75
    assert row["predicted_round_strokes_total_weather_reference"] == 57.75
    assert row["estimated_wind_impact_strokes"] == 1.5
    assert row["estimated_temperature_impact_strokes"] == 0.75
    assert row["estimated_precip_impact_strokes"] == 0.25
    assert row["estimated_total_weather_impact_strokes"] == 2.25

    # Regression guard for the Athena schema issue.
    assert row["course_id"] == "101"
    assert row["layout_id"] == "201"
    assert isinstance(row["course_id"], str)
    assert isinstance(row["layout_id"], str)
