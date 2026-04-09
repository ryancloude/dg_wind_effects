from gold_wind_model_inputs.transform import (
    build_round_model_inputs,
    compute_model_inputs_event_fingerprint,
)


def test_build_round_model_inputs_aggregates_holes():
    rows = [
        {
            "event_year": 2026,
            "tourn_id": 90008,
            "round_number": 1,
            "hole_number": 1,
            "player_key": "P1",
            "player_name": "Pat One",
            "event_name": "Test Open",
            "event_city": "Austin",
            "event_state": "TX",
            "event_start": "2026-04-10",
            "event_end": "2026-04-12",
            "round_date": "2026-04-10",
            "course_id": 101,
            "course_name": "Live Oak DGC",
            "layout_id": 201,
            "layout_name": "Blue Layout",
            "lat": 30.2672,
            "lon": -97.7431,
            "division": "MA3",
            "player_rating": 915,
            "actual_strokes": 3,
            "strokes_over_par": 0,
            "hole_length": 280.0,
            "hole_par": 3,
            "wx_wind_speed_mps": 4.0,
            "wx_wind_gust_mps": 6.0,
            "wx_temperature_c": 18.0,
            "wx_precip_mm": 0.0,
            "wx_pressure_hpa": 1012.0,
            "wx_relative_humidity_pct": 55.0,
            "weather_available_flag": True,
            "source_content_sha256": "abc",
        },
        {
            "event_year": 2026,
            "tourn_id": 90008,
            "round_number": 1,
            "hole_number": 2,
            "player_key": "P1",
            "player_name": "Pat One",
            "event_name": "Test Open",
            "event_city": "Austin",
            "event_state": "TX",
            "event_start": "2026-04-10",
            "event_end": "2026-04-12",
            "round_date": "2026-04-10",
            "course_id": 101,
            "course_name": "Live Oak DGC",
            "layout_id": 201,
            "layout_name": "Blue Layout",
            "lat": 30.2672,
            "lon": -97.7431,
            "division": "MA3",
            "player_rating": 915,
            "actual_strokes": 4,
            "strokes_over_par": 1,
            "hole_length": 350.0,
            "hole_par": 3,
            "wx_wind_speed_mps": 6.0,
            "wx_wind_gust_mps": 8.0,
            "wx_temperature_c": 20.0,
            "wx_precip_mm": 0.2,
            "wx_pressure_hpa": 1010.0,
            "wx_relative_humidity_pct": 65.0,
            "weather_available_flag": True,
            "source_content_sha256": "abc",
        },
    ]

    out = build_round_model_inputs(rows, run_id="run-1", processed_at_utc="2026-03-31T12:00:00Z")
    assert len(out) == 1

    row = out[0]
    assert row["model_inputs_grain"] == "round"
    assert row["player_name"] == "Pat One"
    assert row["event_name"] == "Test Open"
    assert row["event_city"] == "Austin"
    assert row["event_state"] == "TX"
    assert row["event_start"] == "2026-04-10"
    assert row["event_end"] == "2026-04-12"
    assert row["round_date"] == "2026-04-10"
    assert row["course_name"] == "Live Oak DGC"
    assert row["layout_name"] == "Blue Layout"
    assert row["lat"] == 30.2672
    assert row["lon"] == -97.7431
    assert row["actual_round_strokes"] == 7
    assert row["round_strokes_over_par"] == 1
    assert row["hole_count"] == 2
    assert row["round_total_hole_length"] == 630.0
    assert row["round_avg_hole_length"] == 315.0
    assert row["round_total_par"] == 6
    assert row["round_avg_hole_par"] == 3.0
    assert row["round_length_over_par"] == 105.0
    assert row["round_wind_speed_mps_mean"] == 5.0
    assert row["round_wind_speed_mps_max"] == 6.0
    assert row["round_wind_gust_mps_mean"] == 7.0
    assert row["round_temp_c_mean"] == 19.0
    assert row["round_precip_mm_sum"] == 0.2
    assert row["round_precip_mm_mean"] == 0.1
    assert row["round_wind_speed_bucket"] == "moderate"
    assert row["row_hash_sha256"]


def test_build_round_model_inputs_supports_metadata_aliases():
    rows = [
        {
            "event_year": 2026,
            "tourn_id": 90008,
            "round_number": 1,
            "hole_number": 1,
            "player_key": "P1",
            "player_name": "Pat One",
            "tournament_name": "Alias Open",
            "city": "Dallas",
            "state": "TX",
            "start_date": "2026-05-01",
            "end_date": "2026-05-03",
            "round_date": "2026-05-01",
            "course_id": 101,
            "course_name": "Alias Course",
            "layout_id": 201,
            "layout_name": "Alias Layout",
            "latitude": 32.7767,
            "longitude": -96.797,
            "division": "MA3",
            "player_rating": 915,
            "actual_strokes": 3,
            "strokes_over_par": 0,
            "hole_length": 280.0,
            "hole_par": 3,
            "wx_wind_speed_mps": 4.0,
            "wx_wind_gust_mps": 6.0,
            "wx_temperature_c": 18.0,
            "wx_precip_mm": 0.0,
            "wx_pressure_hpa": 1012.0,
            "wx_relative_humidity_pct": 55.0,
            "weather_available_flag": True,
            "source_content_sha256": "abc",
        }
    ]

    out = build_round_model_inputs(rows, run_id="run-1", processed_at_utc="2026-03-31T12:00:00Z")
    row = out[0]

    assert row["event_name"] == "Alias Open"
    assert row["event_city"] == "Dallas"
    assert row["event_state"] == "TX"
    assert row["event_start"] == "2026-05-01"
    assert row["event_end"] == "2026-05-03"
    assert row["lat"] == 32.7767
    assert row["lon"] == -96.797


def test_fingerprint_deterministic():
    hole_rows = [
        {
            "tourn_id": 1,
            "round_number": 1,
            "hole_number": 1,
            "player_key": "P1",
            "player_name": "Pat One",
            "event_name": "Test Open",
            "event_city": "Austin",
            "event_state": "TX",
            "event_start": "2026-04-10",
            "round_date": "2026-04-10",
            "course_name": "Live Oak DGC",
            "layout_name": "Blue Layout",
            "lat": 30.2672,
            "lon": -97.7431,
            "actual_strokes": 3,
            "strokes_over_par": 0,
        }
    ]

    fp1 = compute_model_inputs_event_fingerprint(hole_rows=hole_rows)
    fp2 = compute_model_inputs_event_fingerprint(hole_rows=list(reversed(hole_rows)))

    assert fp1 == fp2
