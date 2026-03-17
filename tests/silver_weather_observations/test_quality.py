from silver_weather_observations.quality import validate_quality


def _base_row() -> dict:
    return {
        "weather_obs_pk": "pk-1",
        "event_id": 90008,
        "round_number": 1,
        "provider": "open_meteo_archive",
        "source_id": "GRID#A",
        "observation_hour_utc": "2026-03-10T08:00:00Z",
        "wind_speed_mps": 4.2,
        "wind_gust_mps": 6.0,
        "wind_dir_deg": 120.0,
        "temperature_c": 19.5,
        "pressure_hpa": 1012.0,
        "relative_humidity_pct": 72.0,
        "precip_mm": 0.0,
        "source_json_key": "k1",
        "source_content_sha256": "h1",
        "source_fetched_at_utc": "2026-03-16T12:00:00Z",
        "silver_run_id": "run-1",
    }


def test_validate_quality_happy_path():
    errors = validate_quality([_base_row()])
    assert errors == []


def test_validate_quality_flags_missing_required():
    row = _base_row()
    row["provider"] = ""
    errors = validate_quality([row])
    assert any(e["rule"] == "not_null:provider" for e in errors)


def test_validate_quality_flags_duplicate_pk():
    row1 = _base_row()
    row2 = _base_row()
    errors = validate_quality([row1, row2])
    assert any(e["rule"] == "unique:weather_obs_pk" for e in errors)


def test_validate_quality_flags_out_of_range_values():
    row = _base_row()
    row["relative_humidity_pct"] = 130
    row["wind_dir_deg"] = 500
    errors = validate_quality([row])
    assert any(e["rule"] == "range:relative_humidity_pct" for e in errors)
    assert any(e["rule"] == "range:wind_dir_deg" for e in errors)