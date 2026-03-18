from gold_wind_model_inputs.transform import (
    build_hole_model_inputs,
    build_round_model_inputs,
    compute_model_inputs_event_fingerprint,
)


def test_build_hole_model_inputs_happy_path():
    rows = [
        {
            "event_year": 2026,
            "tourn_id": 90008,
            "round_number": 1,
            "hole_number": 2,
            "player_key": "P1",
            "hole_score": 4,
            "hole_par": 3,
            "wx_wind_speed_mps": 4.2,
            "wx_wind_gust_mps": 6.1,
            "wx_wind_dir_deg": 120.0,
            "wx_temperature_c": 19.0,
            "wx_precip_mm": 0.0,
            "wx_pressure_hpa": 1011.0,
            "wx_relative_humidity_pct": 65.0,
            "wx_weather_missing_flag": False,
            "source_content_sha256": "abc",
        }
    ]

    out = build_hole_model_inputs(rows, run_id="run-1", processed_at_utc="2026-03-18T12:00:00Z")
    assert len(out) == 1
    row = out[0]

    assert row["model_inputs_grain"] == "hole"
    assert row["model_inputs_version"] == "v1"
    assert row["target_strokes_over_par"] == 1
    assert row["weather_available_flag"] is True
    assert row["wind_speed_bucket"] == "light"
    assert row["feature_wind_speed_mps"] == 4.2
    assert row["row_hash_sha256"]


def test_build_round_model_inputs_happy_path():
    rows = [
        {
            "event_year": 2026,
            "tourn_id": 90008,
            "round_number": 1,
            "player_key": "P1",
            "round_score": 57,
            "layout_par": 60,
            "wx_wind_speed_mps": 8.4,
            "wx_wind_gust_mps": 11.0,
            "wx_wind_dir_deg": 200.0,
            "wx_temperature_c": 18.0,
            "wx_precip_mm": 0.2,
            "wx_weather_missing_flag": False,
            "source_content_sha256": "def",
        }
    ]

    out = build_round_model_inputs(rows, run_id="run-1", processed_at_utc="2026-03-18T12:00:00Z")
    assert len(out) == 1
    row = out[0]

    assert row["model_inputs_grain"] == "round"
    assert row["target_strokes_over_par"] == -3
    assert row["wind_speed_bucket"] == "strong"
    assert row["feature_wind_speed_mps"] == 8.4
    assert row["row_hash_sha256"]


def test_fingerprint_deterministic():
    hole_rows = [
        {"tourn_id": 1, "round_number": 1, "hole_number": 1, "player_key": "P1", "hole_score": 3, "hole_par": 3}
    ]
    round_rows = [{"tourn_id": 1, "round_number": 1, "player_key": "P1", "round_score": 54, "layout_par": 54}]

    fp1 = compute_model_inputs_event_fingerprint(hole_rows=hole_rows, round_rows=round_rows)
    fp2 = compute_model_inputs_event_fingerprint(
        hole_rows=list(reversed(hole_rows)),
        round_rows=list(reversed(round_rows)),
    )

    assert fp1 == fp2