from __future__ import annotations

PIPELINE_NAME = "report_round_weather_impacts"
REPORT_CHECKPOINT_PK = "PIPELINE#REPORT_ROUND_WEATHER_IMPACTS"
REPORT_POLICY_VERSION = "v1"

SCORED_ROUNDS_PREFIX = "gold/pdga/wind_effects/scored_rounds/"
INTERMEDIATE_BASE_PREFIX = "gold/pdga/wind_effects/reports/intermediate/"
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

REQUIRED_SCORED_COLS = (
    "event_year",
    "tourn_id",
    "round_number",
    "player_key",
    "course_id",
    "layout_id",
    "division",
    "player_rating",
    "actual_round_strokes",
    "predicted_round_strokes",
    "predicted_round_strokes_wind_reference",
    "estimated_wind_impact_strokes",
    "estimated_temperature_impact_strokes",
    "estimated_total_weather_impact_strokes",
    "round_wind_speed_mps_mean",
    "round_temp_c_mean",
)

SUM_METRIC_COLS = (
    "sum_observed_wind_mph",
    "sum_observed_temp_f",
    "sum_actual_round_strokes",
    "sum_predicted_round_strokes",
    "sum_predicted_round_strokes_wind_reference",
    "sum_estimated_wind_impact_strokes",
    "sum_estimated_temperature_impact_strokes",
    "sum_estimated_total_weather_impact_strokes",
)
