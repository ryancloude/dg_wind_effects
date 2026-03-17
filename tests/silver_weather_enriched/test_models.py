from silver_weather_enriched.models import (
    ENRICHED_HOLE_WEATHER_COLS,
    ENRICHED_ROUND_WEATHER_COLS,
    JOIN_POLICY_VERSION,
    PIPELINE_NAME,
    SILVER_ENRICHED_CHECKPOINT_PK,
)


def test_pipeline_constants():
    assert PIPELINE_NAME == "silver_weather_enriched"
    assert SILVER_ENRICHED_CHECKPOINT_PK == "PIPELINE#SILVER_WEATHER_ENRICHED"
    assert JOIN_POLICY_VERSION == "v1"


def test_weather_column_sets_present():
    assert "wx_wind_speed_mps" in ENRICHED_ROUND_WEATHER_COLS
    assert "wx_weather_missing_flag" in ENRICHED_ROUND_WEATHER_COLS

    assert "wx_wind_speed_mps" in ENRICHED_HOLE_WEATHER_COLS
    assert "wx_weather_missing_flag" in ENRICHED_HOLE_WEATHER_COLS