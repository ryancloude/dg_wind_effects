import numpy as np
import pandas as pd

from score_round_wind_model.scoring import compute_scoring_request_fingerprint, score_round_rows


class FakeModel:
    def __init__(self):
        self.call_index = 0

    def predict(self, pool):
        self.call_index += 1
        row_count = pool.num_row()

        # Return deterministic predictions per scoring pass without trying to
        # inspect Pool feature values. This keeps the test independent of
        # CatBoost's internal handling of categorical features.
        if self.call_index == 1:
            return np.full(row_count, 60.0)  # observed prediction
        if self.call_index == 2:
            return np.full(row_count, 58.5)  # wind reference
        if self.call_index == 3:
            return np.full(row_count, 59.25)  # temperature reference
        if self.call_index == 4:
            return np.full(row_count, 57.75)  # total weather reference
        raise AssertionError("FakeModel.predict called more times than expected")


def test_compute_scoring_request_fingerprint_deterministic():
    event_object = {"key": "k", "etag": "e", "size": 1, "last_modified": "x"}
    fp1 = compute_scoring_request_fingerprint(event_object=event_object, training_request_fingerprint="train-1")
    fp2 = compute_scoring_request_fingerprint(event_object=event_object, training_request_fingerprint="train-1")
    assert fp1 == fp2


def test_score_round_rows_adds_impact_columns():
    df = pd.DataFrame(
        [
            {
                "event_year": 2026,
                "tourn_id": 90008,
                "round_number": 1,
                "player_key": "P1",
                "course_id": "101",
                "layout_id": "201",
                "division": "MA3",
                "player_rating": 915,
                "actual_round_strokes": 57,
                "round_strokes_over_par": -3,
                "hole_count": 18,
                "round_total_hole_length": 9000.0,
                "round_total_par": 60,
                "round_wind_speed_mps_mean": 4.0,
                "round_wind_speed_mps_max": 5.0,
                "round_wind_gust_mps_mean": 6.0,
                "round_wind_gust_mps_max": 7.0,
                "round_temp_c_mean": 18.0,
                "round_precip_mm_sum": 0.0,
                "round_precip_mm_mean": 0.0,
                "round_pressure_hpa_mean": 1012.0,
                "round_humidity_pct_mean": 60.0,
                "round_wind_speed_bucket": "light",
                "round_wind_gust_bucket": "mild",
            }
        ]
    )

    manifest = {
        "model_name": "round_one_stage_catboost",
        "model_version": "v1",
        "wind_speed_reference_mph": 2.0,
        "wind_gust_reference_mph": 3.0,
        "temperature_reference_c": 12.0,
        "precip_reference_mm": 0.0,
        "pressure_reference_hpa": 1010.0,
        "humidity_reference_pct": 55.0,
    }

    result = score_round_rows(
        df=df,
        model=FakeModel(),
        training_manifest=manifest,
        feature_columns=[
            "player_rating",
            "course_id",
            "round_wind_speed_bucket",
            "round_wind_gust_bucket",
            "division",
            "layout_id",
        ],
        categorical_feature_columns=[
            "course_id",
            "round_wind_speed_bucket",
            "round_wind_gust_bucket",
            "division",
            "layout_id",
        ],
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
    assert "predicted_round_strokes_total_weather_reference" in scored_df.columns
    assert "estimated_wind_impact_strokes" in scored_df.columns
    assert "estimated_temperature_impact_strokes" in scored_df.columns
    assert "estimated_total_weather_impact_strokes" in scored_df.columns

    row = scored_df.iloc[0]
    assert row["predicted_round_strokes"] == 60.0
    assert row["predicted_round_strokes_wind_reference"] == 58.5
    assert row["predicted_round_strokes_temperature_reference"] == 59.25
    assert row["predicted_round_strokes_total_weather_reference"] == 57.75
    assert row["estimated_wind_impact_strokes"] == 1.5
    assert row["estimated_temperature_impact_strokes"] == 0.75
    assert row["estimated_total_weather_impact_strokes"] == 2.25