from __future__ import annotations

PIPELINE_NAME = "silver_weather_enriched"
SILVER_ENRICHED_CHECKPOINT_PK = "PIPELINE#SILVER_WEATHER_ENRICHED"

# Bump this whenever join policy logic changes in a way that should force recompute.
JOIN_POLICY_VERSION = "v1"

# Weather columns appended to player_rounds
ENRICHED_ROUND_WEATHER_COLS = (
    "wx_observation_hour_utc",
    "wx_wind_speed_mps",
    "wx_wind_gust_mps",
    "wx_wind_dir_deg",
    "wx_temperature_c",
    "wx_pressure_hpa",
    "wx_relative_humidity_pct",
    "wx_precip_mm",
    "wx_provider",
    "wx_source_id",
    "wx_source_json_key",
    "wx_source_content_sha256",
    "wx_weather_obs_pk",
    "wx_weather_missing_flag",
)

# Weather columns appended to player_holes
ENRICHED_HOLE_WEATHER_COLS = (
    "wx_observation_hour_utc",
    "wx_wind_speed_mps",
    "wx_wind_gust_mps",
    "wx_wind_dir_deg",
    "wx_temperature_c",
    "wx_pressure_hpa",
    "wx_relative_humidity_pct",
    "wx_precip_mm",
    "wx_provider",
    "wx_source_id",
    "wx_source_json_key",
    "wx_source_content_sha256",
    "wx_weather_obs_pk",
    "wx_weather_missing_flag",
)