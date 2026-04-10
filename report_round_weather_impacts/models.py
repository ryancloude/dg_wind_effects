from __future__ import annotations

PIPELINE_NAME = "report_round_weather_impacts"
REPORT_CHECKPOINT_PK = "PIPELINE#REPORT_ROUND_WEATHER_IMPACTS"
REPORT_POLICY_VERSION = "v2"

ATHENA_BASE_PREFIX = "gold/pdga/wind_effects/reports/athena/base/"
PUBLISHED_BASE_PREFIX = "gold/pdga/wind_effects/reports/published/"

MPS_TO_MPH = 2.23694

REPORT_TABLES = (
    "weather_overview",
    "weather_by_wind_bucket",
    "weather_by_temperature_band",
    "weather_by_month",
    "weather_by_event_geo",
    "weather_by_state",
    "weather_by_division",
    "weather_by_rating_band",
    "weather_by_course_layout",
    "weather_by_event",
    "weather_by_event_round",
)

