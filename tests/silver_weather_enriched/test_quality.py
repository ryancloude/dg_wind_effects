from silver_weather_enriched.quality import validate_enriched_quality


def _round_row():
    return {
        "tourn_id": 90008,
        "round_number": 1,
        "player_key": "P1",
        "wx_observation_hour_utc": "2026-03-17T08:00:00Z",
        "wx_wind_speed_mps": 4.2,
        "wx_wind_gust_mps": 6.0,
        "wx_wind_dir_deg": 120.0,
        "wx_temperature_c": 19.5,
        "wx_pressure_hpa": 1012.0,
        "wx_relative_humidity_pct": 70.0,
        "wx_precip_mm": 0.0,
        "wx_provider": "open_meteo_archive",
        "wx_source_id": "GRID#A",
        "wx_source_json_key": "silver/weather/obs.parquet",
        "wx_source_content_sha256": "wx-hash-1",
        "wx_weather_obs_pk": "wxpk-1",
        "wx_weather_missing_flag": False,
    }


def _hole_row():
    return {
        "tourn_id": 90008,
        "round_number": 1,
        "hole_number": 1,
        "player_key": "P1",
        "wx_observation_hour_utc": "2026-03-17T08:00:00Z",
        "wx_wind_speed_mps": 4.2,
        "wx_wind_gust_mps": 6.0,
        "wx_wind_dir_deg": 120.0,
        "wx_temperature_c": 19.5,
        "wx_pressure_hpa": 1012.0,
        "wx_relative_humidity_pct": 70.0,
        "wx_precip_mm": 0.0,
        "wx_provider": "open_meteo_archive",
        "wx_source_id": "GRID#A",
        "wx_source_json_key": "silver/weather/obs.parquet",
        "wx_source_content_sha256": "wx-hash-1",
        "wx_weather_obs_pk": "wxpk-1",
        "wx_weather_missing_flag": False,
    }


def test_validate_enriched_quality_happy_path():
    errors = validate_enriched_quality(
        round_input_rows=[{"tourn_id": 90008, "round_number": 1, "player_key": "P1"}],
        hole_input_rows=[{"tourn_id": 90008, "round_number": 1, "hole_number": 1, "player_key": "P1"}],
        round_output_rows=[_round_row()],
        hole_output_rows=[_hole_row()],
    )
    assert errors == []


def test_validate_enriched_quality_detects_count_mismatch():
    errors = validate_enriched_quality(
        round_input_rows=[{"tourn_id": 90008, "round_number": 1, "player_key": "P1"}],
        hole_input_rows=[],
        round_output_rows=[],
        hole_output_rows=[],
    )
    assert any(e["rule"] == "row_count:round" for e in errors)