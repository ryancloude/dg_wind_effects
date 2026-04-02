import pandas as pd

from train_round_wind_model.training import (
    compute_dataset_fingerprint,
    compute_training_request_fingerprint,
    prepare_training_dataframe,
)


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
        "weather_available_flag": True,
        "hole_count": 18,
        "round_total_hole_length": 9000.0,
        "round_avg_hole_length": 500.0,
        "round_total_par": 60,
        "round_avg_hole_par": 3.33,
        "round_length_over_par": 150.0,
        "round_wind_speed_mps_mean": 4.2,
        "round_wind_speed_mps_max": 5.1,
        "round_wind_gust_mps_mean": 6.0,
        "round_wind_gust_mps_max": 7.4,
        "round_temp_c_mean": 18.0,
        "round_precip_mm_sum": 0.0,
        "round_precip_mm_mean": 0.0,
        "round_pressure_hpa_mean": 1012.0,
        "round_humidity_pct_mean": 61.0,
        "round_wind_speed_bucket": "light",
        "round_wind_gust_bucket": "mild",
        "row_hash_sha256": "abc",
    }
    row.update(overrides)
    return row


def test_compute_dataset_fingerprint_deterministic():
    objects1 = [
        {"key": "a", "etag": "e1", "size": 10, "last_modified": "2026-04-02T00:00:00Z"},
        {"key": "b", "etag": "e2", "size": 20, "last_modified": "2026-04-02T00:00:01Z"},
    ]
    objects2 = list(reversed(objects1))

    fp1 = compute_dataset_fingerprint(objects1)
    fp2 = compute_dataset_fingerprint(objects2)

    assert fp1 == fp2


def test_compute_training_request_fingerprint_deterministic():
    fp1 = compute_training_request_fingerprint(dataset_fingerprint="dataset-1", event_ids=[2, 1])
    fp2 = compute_training_request_fingerprint(dataset_fingerprint="dataset-1", event_ids=[1, 2])

    assert fp1 == fp2


def test_prepare_training_dataframe_filters_missing_numeric_rows():
    df = pd.DataFrame(
        [
            _row(player_key="P1"),
            _row(player_key="P2", player_rating=None),
            _row(player_key="P3", weather_available_flag=False),
        ]
    )

    out_df, stats = prepare_training_dataframe(df)

    assert len(out_df) == 1
    assert stats["input_rows"] == 3
    assert stats["rows_after_weather_filter"] == 2
    assert stats["rows_after_numeric_not_null_filter"] == 1

