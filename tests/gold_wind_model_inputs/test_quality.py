from gold_wind_model_inputs.quality import validate_model_inputs_quality


def _hole_input():
    return {
        "event_year": 2026,
        "tourn_id": 90008,
        "round_number": 1,
        "hole_number": 1,
        "player_key": "P1",
    }


def _round_output():
    return {
        "event_year": 2026,
        "tourn_id": 90008,
        "round_number": 1,
        "player_key": "P1",
        "player_name": "Pat One",
        "event_name": "Test Open",
        "event_city": "Austin",
        "event_state": "TX",
        "event_start": "2026-04-10",
        "event_end": "2026-04-12",
        "round_date": "2026-04-10",
        "course_name": "Live Oak DGC",
        "layout_name": "Blue Layout",
        "lat": 30.2672,
        "lon": -97.7431,
        "model_inputs_grain": "round",
        "model_inputs_version": "v3",
        "model_inputs_run_id": "run-1",
        "model_inputs_processed_at_utc": "2026-03-31T12:00:00Z",
        "row_hash_sha256": "abc",
        "actual_round_strokes": 57,
        "round_strokes_over_par": -3,
        "weather_available_flag": True,
        "hole_count": 18,
        "round_total_hole_length": 9000.0,
        "round_avg_hole_length": 500.0,
        "round_total_par": 60,
        "round_avg_hole_par": 3.33,
        "round_length_over_par": 150.0,
        "round_wind_speed_mps_mean": 4.2,
        "round_wind_speed_mps_max": 6.0,
        "round_wind_gust_mps_mean": 5.8,
        "round_wind_gust_mps_max": 7.1,
        "round_temp_c_mean": 18.0,
        "round_precip_mm_sum": 0.1,
        "round_precip_mm_mean": 0.01,
        "round_pressure_hpa_mean": 1012.0,
        "round_humidity_pct_mean": 61.0,
        "round_wind_speed_bucket": "light",
        "round_wind_gust_bucket": "mild",
        "course_id": 101,
        "layout_id": 201,
        "division": "MA3",
        "player_rating": 915.0,
    }


def test_validate_model_inputs_quality_happy_path():
    errors = validate_model_inputs_quality(
        hole_input_rows=[_hole_input()],
        round_output_rows=[_round_output()],
    )
    assert errors == []


def test_validate_model_inputs_quality_missing_required_column():
    bad = _round_output()
    bad.pop("row_hash_sha256")

    errors = validate_model_inputs_quality(
        hole_input_rows=[_hole_input()],
        round_output_rows=[bad],
    )
    assert errors
    assert any(e["rule"] in ("columns:round_required", "not_null:row_hash_sha256:round") for e in errors)


def test_validate_model_inputs_quality_duplicate_round_pk():
    inp = [_hole_input(), dict(_hole_input(), hole_number=2)]
    out = [_round_output(), _round_output()]

    errors = validate_model_inputs_quality(
        hole_input_rows=inp,
        round_output_rows=out,
    )
    assert any(e["rule"] == "uniqueness:round_pk" for e in errors)

