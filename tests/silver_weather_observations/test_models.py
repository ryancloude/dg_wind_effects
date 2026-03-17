from silver_weather_observations.models import LINEAGE_REQUIRED_COLS, OBS_PK_COLS, OBS_TIEBREAK_COLS


def test_model_constants_are_stable():
    assert OBS_PK_COLS == ("event_id", "round_number", "provider", "source_id", "observation_hour_utc")
    assert OBS_TIEBREAK_COLS == ("source_fetched_at_utc", "source_json_key")
    assert "silver_run_id" in LINEAGE_REQUIRED_COLS