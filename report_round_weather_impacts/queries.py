from __future__ import annotations

from report_round_weather_impacts.models import REPORT_TABLES


def build_drop_table_sql(*, database: str, table_name: str) -> str:
    return f"DROP TABLE IF EXISTS {database}.{table_name}"


def _month_label_sql(expr: str) -> str:
    return f"""
    CASE {expr}
      WHEN 1 THEN 'Jan'
      WHEN 2 THEN 'Feb'
      WHEN 3 THEN 'Mar'
      WHEN 4 THEN 'Apr'
      WHEN 5 THEN 'May'
      WHEN 6 THEN 'Jun'
      WHEN 7 THEN 'Jul'
      WHEN 8 THEN 'Aug'
      WHEN 9 THEN 'Sep'
      WHEN 10 THEN 'Oct'
      WHEN 11 THEN 'Nov'
      WHEN 12 THEN 'Dec'
      ELSE 'Unknown'
    END
    """.strip()


def build_reporting_base_ctas_sql(
    *,
    database: str,
    source_table: str,
    base_table_name: str,
    external_location: str,
) -> str:
    return f"""
CREATE TABLE {database}.{base_table_name}
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = '{external_location}'
) AS
WITH scored_source AS (
  SELECT
    event_year,
    CAST(tourn_id AS bigint) AS tourn_id,
    CAST(round_number AS integer) AS round_number,
    CAST(player_key AS varchar) AS player_key,
    CAST(player_name AS varchar) AS player_name,
    CAST(division AS varchar) AS division,
    CAST(player_rating AS double) AS player_rating,
    CAST(event_name AS varchar) AS event_name,
    CAST(event_city AS varchar) AS event_city,
    CAST(event_state AS varchar) AS event_state,
    CAST(event_start_date AS varchar) AS event_start_date,
    CAST(event_end_date AS varchar) AS event_end_date,
    CAST(round_date AS varchar) AS round_date,
    CAST(course_id AS varchar) AS course_id,
    CAST(course_name AS varchar) AS course_name,
    CAST(layout_id AS varchar) AS layout_id,
    CAST(layout_name AS varchar) AS layout_name,
    CAST(lat AS double) AS lat,
    CAST(lon AS double) AS lon,
    CAST(actual_round_strokes AS bigint) AS actual_round_strokes,
    CAST(predicted_round_strokes AS double) AS predicted_round_strokes,
    CAST(predicted_round_strokes_wind_reference AS double) AS predicted_round_strokes_wind_reference,
    CAST(predicted_round_strokes_temperature_reference AS double) AS predicted_round_strokes_temperature_reference,
    CAST(predicted_round_strokes_total_weather_reference AS double) AS predicted_round_strokes_total_weather_reference,
    CAST(estimated_wind_impact_strokes AS double) AS estimated_wind_impact_strokes,
    CAST(estimated_temperature_impact_strokes AS double) AS estimated_temperature_impact_strokes,
    CAST(estimated_total_weather_impact_strokes AS double) AS estimated_total_weather_impact_strokes,
    CAST(round_wind_speed_bucket AS varchar) AS round_wind_speed_bucket,
    CAST(round_wind_gust_bucket AS varchar) AS round_wind_gust_bucket,
    CAST(round_wind_speed_mps_mean AS double) AS round_wind_speed_mps_mean,
    CAST(round_temp_c_mean AS double) AS round_temp_c_mean,
    CAST(round_precip_mm_sum AS double) AS round_precip_mm_sum,
    try(date_parse(CAST(round_date AS varchar), '%Y-%m-%d')) AS round_dt
  FROM {database}.{source_table}
  WHERE estimated_total_weather_impact_strokes IS NOT NULL
)
SELECT
  event_year,
  tourn_id,
  round_number,
  player_key,
  player_name,
  COALESCE(NULLIF(TRIM(division), ''), 'Unknown') AS division,
  player_rating,
  COALESCE(NULLIF(TRIM(event_name), ''), 'Unknown Event') AS event_name,
  NULLIF(TRIM(event_city), '') AS event_city,
  NULLIF(TRIM(event_state), '') AS event_state,
  UPPER(COALESCE(NULLIF(TRIM(event_state), ''), 'UNKNOWN')) AS state,
  event_start_date,
  event_end_date,
  round_date,
  course_id,
  COALESCE(NULLIF(TRIM(course_name), ''), 'Unknown Course') AS course_name,
  layout_id,
  COALESCE(NULLIF(TRIM(layout_name), ''), 'Unknown Layout') AS layout_name,
  lat,
  lon,
  actual_round_strokes,
  predicted_round_strokes,
  predicted_round_strokes_wind_reference,
  predicted_round_strokes_temperature_reference,
  predicted_round_strokes_total_weather_reference,
  estimated_wind_impact_strokes,
  estimated_temperature_impact_strokes,
  estimated_total_weather_impact_strokes,
  round_wind_speed_bucket,
  round_wind_gust_bucket,
  round_wind_speed_mps_mean * 2.23694 AS observed_wind_mph,
  (round_temp_c_mean * 9.0 / 5.0) + 32.0 AS observed_temp_f,
  year(round_dt) AS round_year,
  month(round_dt) AS round_month,
  {_month_label_sql("month(round_dt)")} AS round_month_label,
  CASE
    WHEN player_rating IS NULL THEN 'Unknown'
    WHEN player_rating < 900 THEN '<900'
    WHEN player_rating < 940 THEN '900-939'
    WHEN player_rating < 970 THEN '940-969'
    ELSE '970+'
  END AS rating_band,
  CASE
    WHEN ((round_temp_c_mean * 9.0 / 5.0) + 32.0) IS NULL THEN 'Unknown'
    WHEN ((round_temp_c_mean * 9.0 / 5.0) + 32.0) < 41 THEN '<41F'
    WHEN ((round_temp_c_mean * 9.0 / 5.0) + 32.0) < 50 THEN '41-49F'
    WHEN ((round_temp_c_mean * 9.0 / 5.0) + 32.0) < 60 THEN '50-59F'
    WHEN ((round_temp_c_mean * 9.0 / 5.0) + 32.0) < 70 THEN '60-69F'
    WHEN ((round_temp_c_mean * 9.0 / 5.0) + 32.0) < 80 THEN '70-79F'
    ELSE '80F+'
  END AS temperature_band_f,
  CASE
    WHEN COALESCE(round_precip_mm_sum, 0.0) > 0.0 THEN 'Precip'
    ELSE 'No Precip'
  END AS precip_flag
FROM scored_source
""".strip()


def _ctas_sql(*, database: str, table_name: str, external_location: str, select_sql: str) -> str:
    return f"""
CREATE TABLE {database}.{table_name}
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = '{external_location}'
) AS
{select_sql}
""".strip()


def build_report_ctas_sql(
    *,
    database: str,
    base_table_name: str,
    report_table_name: str,
    external_location: str,
) -> str:
    if report_table_name not in REPORT_TABLES:
        raise ValueError(f"unsupported report table: {report_table_name}")

    source = f"{database}.{base_table_name}"

    if report_table_name == "weather_overview":
        select_sql = f"""
SELECT
  COUNT(*) AS rounds_scored,
  COUNT(DISTINCT tourn_id) AS events_scored,
  COUNT(DISTINCT player_key) AS players_scored,
  COUNT(DISTINCT state) AS states_covered,
  AVG(observed_wind_mph) AS avg_observed_wind_mph,
  AVG(observed_temp_f) AS avg_observed_temp_f,
  AVG(actual_round_strokes) AS avg_actual_round_strokes,
  AVG(predicted_round_strokes) AS avg_predicted_round_strokes,
  AVG(predicted_round_strokes_wind_reference) AS avg_predicted_round_strokes_wind_reference,
  AVG(estimated_wind_impact_strokes) AS avg_estimated_wind_impact_strokes,
  AVG(estimated_temperature_impact_strokes) AS avg_estimated_temperature_impact_strokes,
  AVG(estimated_total_weather_impact_strokes) AS avg_estimated_total_weather_impact_strokes
FROM {source}
""".strip()
        return _ctas_sql(
            database=database,
            table_name=report_table_name,
            external_location=external_location,
            select_sql=select_sql,
        )

    if report_table_name == "weather_impact_distribution":
        select_sql = f"""
WITH bins AS (
  SELECT *
  FROM (
    VALUES
      ('total_weather', '< -3.0', CAST(NULL AS double), CAST(-3.0 AS double), 0),
      ('total_weather', '-3.0 to -2.5', CAST(-3.0 AS double), CAST(-2.5 AS double), 1),
      ('total_weather', '-2.5 to -2.0', CAST(-2.5 AS double), CAST(-2.0 AS double), 2),
      ('total_weather', '-2.0 to -1.5', CAST(-2.0 AS double), CAST(-1.5 AS double), 3),
      ('total_weather', '-1.5 to -1.0', CAST(-1.5 AS double), CAST(-1.0 AS double), 4),
      ('total_weather', '-1.0 to -0.5', CAST(-1.0 AS double), CAST(-0.5 AS double), 5),
      ('total_weather', '-0.5 to 0.0', CAST(-0.5 AS double), CAST(0.0 AS double), 6),
      ('total_weather', '0.0 to 0.5', CAST(0.0 AS double), CAST(0.5 AS double), 7),
      ('total_weather', '0.5 to 1.0', CAST(0.5 AS double), CAST(1.0 AS double), 8),
      ('total_weather', '1.0 to 1.5', CAST(1.0 AS double), CAST(1.5 AS double), 9),
      ('total_weather', '1.5 to 2.0', CAST(1.5 AS double), CAST(2.0 AS double), 10),
      ('total_weather', '2.0 to 2.5', CAST(2.0 AS double), CAST(2.5 AS double), 11),
      ('total_weather', '2.5 to 3.0', CAST(2.5 AS double), CAST(3.0 AS double), 12),
      ('total_weather', '3.0 to 3.5', CAST(3.0 AS double), CAST(3.5 AS double), 13),
      ('total_weather', '3.5 to 4.0', CAST(3.5 AS double), CAST(4.0 AS double), 14),
      ('total_weather', '4.0 to 4.5', CAST(4.0 AS double), CAST(4.5 AS double), 15),
      ('total_weather', '4.5 to 5.0', CAST(4.5 AS double), CAST(5.0 AS double), 16),
      ('total_weather', '5.0 to 5.5', CAST(5.0 AS double), CAST(5.5 AS double), 17),
      ('total_weather', '5.5 to 6.0', CAST(5.5 AS double), CAST(6.0 AS double), 18),
      ('total_weather', '>= 6.0', CAST(6.0 AS double), CAST(NULL AS double), 19)
  ) AS t (impact_metric, impact_bin_label, impact_bin_start, impact_bin_end, sort_order)
),
source_rows AS (
  SELECT estimated_total_weather_impact_strokes
  FROM {source}
  WHERE estimated_total_weather_impact_strokes IS NOT NULL
)
SELECT
  b.impact_metric,
  b.impact_bin_label,
  b.impact_bin_start,
  b.impact_bin_end,
  COALESCE(COUNT(s.estimated_total_weather_impact_strokes), 0) AS rounds_scored,
  b.sort_order
FROM bins b
LEFT JOIN source_rows s
  ON (
    (b.impact_bin_start IS NULL AND s.estimated_total_weather_impact_strokes < b.impact_bin_end)
    OR
    (b.impact_bin_end IS NULL AND s.estimated_total_weather_impact_strokes >= b.impact_bin_start)
    OR
    (
      b.impact_bin_start IS NOT NULL
      AND b.impact_bin_end IS NOT NULL
      AND s.estimated_total_weather_impact_strokes >= b.impact_bin_start
      AND s.estimated_total_weather_impact_strokes < b.impact_bin_end
    )
  )
GROUP BY 1,2,3,4,6
ORDER BY sort_order
""".strip()
        return _ctas_sql(database=database, table_name=report_table_name, external_location=external_location, select_sql=select_sql)

    if report_table_name == "weather_by_wind_bucket":
        select_sql = f"""
SELECT
  round_wind_speed_bucket,
  COUNT(*) AS rounds_scored,
  COUNT(DISTINCT tourn_id) AS events_scored,
  AVG(observed_wind_mph) AS avg_observed_wind_mph,
  AVG(estimated_wind_impact_strokes) AS avg_estimated_wind_impact_strokes,
  AVG(estimated_total_weather_impact_strokes) AS avg_estimated_total_weather_impact_strokes
FROM {source}
GROUP BY 1
ORDER BY 1
""".strip()
        return _ctas_sql(database=database, table_name=report_table_name, external_location=external_location, select_sql=select_sql)

    if report_table_name == "weather_by_temperature_band":
        select_sql = f"""
SELECT
  temperature_band_f,
  COUNT(*) AS rounds_scored,
  COUNT(DISTINCT tourn_id) AS events_scored,
  AVG(observed_temp_f) AS avg_observed_temp_f,
  AVG(estimated_temperature_impact_strokes) AS avg_estimated_temperature_impact_strokes,
  AVG(estimated_total_weather_impact_strokes) AS avg_estimated_total_weather_impact_strokes
FROM {source}
GROUP BY 1
ORDER BY 1
""".strip()
        return _ctas_sql(database=database, table_name=report_table_name, external_location=external_location, select_sql=select_sql)

    if report_table_name == "weather_by_month":
        select_sql = f"""
SELECT
  round_year,
  round_month,
  round_month_label,
  COUNT(*) AS rounds_scored,
  COUNT(DISTINCT tourn_id) AS events_scored,
  AVG(observed_wind_mph) AS avg_observed_wind_mph,
  AVG(observed_temp_f) AS avg_observed_temp_f,
  AVG(estimated_wind_impact_strokes) AS avg_estimated_wind_impact_strokes,
  AVG(estimated_temperature_impact_strokes) AS avg_estimated_temperature_impact_strokes,
  AVG(estimated_total_weather_impact_strokes) AS avg_estimated_total_weather_impact_strokes
FROM {source}
GROUP BY 1,2,3
ORDER BY 1,2
""".strip()
        return _ctas_sql(database=database, table_name=report_table_name, external_location=external_location, select_sql=select_sql)

    if report_table_name == "weather_by_event_geo":
        select_sql = f"""
SELECT
  event_year,
  tourn_id,
  event_name,
  event_city,
  state,
  event_start_date,
  lat,
  lon,
  COUNT(*) AS rounds_scored,
  COUNT(DISTINCT player_key) AS players_scored,
  AVG(observed_wind_mph) AS avg_observed_wind_mph,
  AVG(observed_temp_f) AS avg_observed_temp_f,
  AVG(estimated_wind_impact_strokes) AS avg_estimated_wind_impact_strokes,
  AVG(estimated_temperature_impact_strokes) AS avg_estimated_temperature_impact_strokes,
  AVG(estimated_total_weather_impact_strokes) AS avg_estimated_total_weather_impact_strokes
FROM {source}
GROUP BY 1,2,3,4,5,6,7,8
""".strip()
        return _ctas_sql(database=database, table_name=report_table_name, external_location=external_location, select_sql=select_sql)

    if report_table_name == "weather_by_state":
        select_sql = f"""
SELECT
  state,
  COUNT(*) AS rounds_scored,
  COUNT(DISTINCT tourn_id) AS events_scored,
  COUNT(DISTINCT player_key) AS players_scored,
  AVG(observed_wind_mph) AS avg_observed_wind_mph,
  AVG(observed_temp_f) AS avg_observed_temp_f,
  AVG(estimated_wind_impact_strokes) AS avg_estimated_wind_impact_strokes,
  AVG(estimated_temperature_impact_strokes) AS avg_estimated_temperature_impact_strokes,
  AVG(estimated_total_weather_impact_strokes) AS avg_estimated_total_weather_impact_strokes
FROM {source}
GROUP BY 1
ORDER BY avg_estimated_wind_impact_strokes DESC
""".strip()
        return _ctas_sql(database=database, table_name=report_table_name, external_location=external_location, select_sql=select_sql)

    if report_table_name == "weather_by_division":
        select_sql = f"""
SELECT
  division,
  COUNT(*) AS rounds_scored,
  COUNT(DISTINCT tourn_id) AS events_scored,
  AVG(observed_wind_mph) AS avg_observed_wind_mph,
  AVG(observed_temp_f) AS avg_observed_temp_f,
  AVG(estimated_wind_impact_strokes) AS avg_estimated_wind_impact_strokes,
  AVG(estimated_temperature_impact_strokes) AS avg_estimated_temperature_impact_strokes,
  AVG(estimated_total_weather_impact_strokes) AS avg_estimated_total_weather_impact_strokes
FROM {source}
GROUP BY 1
ORDER BY avg_estimated_wind_impact_strokes DESC
""".strip()
        return _ctas_sql(database=database, table_name=report_table_name, external_location=external_location, select_sql=select_sql)

    if report_table_name == "weather_by_rating_band":
        select_sql = f"""
SELECT
  rating_band,
  COUNT(*) AS rounds_scored,
  COUNT(DISTINCT tourn_id) AS events_scored,
  AVG(observed_wind_mph) AS avg_observed_wind_mph,
  AVG(observed_temp_f) AS avg_observed_temp_f,
  AVG(estimated_wind_impact_strokes) AS avg_estimated_wind_impact_strokes,
  AVG(estimated_temperature_impact_strokes) AS avg_estimated_temperature_impact_strokes,
  AVG(estimated_total_weather_impact_strokes) AS avg_estimated_total_weather_impact_strokes
FROM {source}
GROUP BY 1
ORDER BY 1
""".strip()
        return _ctas_sql(database=database, table_name=report_table_name, external_location=external_location, select_sql=select_sql)

    if report_table_name == "weather_by_course_layout":
        select_sql = f"""
SELECT
  course_id,
  course_name,
  layout_id,
  layout_name,
  state,
  COUNT(*) AS rounds_scored,
  COUNT(DISTINCT tourn_id) AS events_scored,
  AVG(observed_wind_mph) AS avg_observed_wind_mph,
  AVG(observed_temp_f) AS avg_observed_temp_f,
  AVG(estimated_wind_impact_strokes) AS avg_estimated_wind_impact_strokes,
  AVG(estimated_temperature_impact_strokes) AS avg_estimated_temperature_impact_strokes,
  AVG(estimated_total_weather_impact_strokes) AS avg_estimated_total_weather_impact_strokes
FROM {source}
GROUP BY 1,2,3,4,5
""".strip()
        return _ctas_sql(database=database, table_name=report_table_name, external_location=external_location, select_sql=select_sql)

    if report_table_name == "weather_by_event":
        select_sql = f"""
SELECT
  event_year,
  tourn_id,
  event_name,
  event_city,
  state,
  event_start_date,
  COUNT(*) AS rounds_scored,
  COUNT(DISTINCT player_key) AS players_scored,
  AVG(observed_wind_mph) AS avg_observed_wind_mph,
  AVG(observed_temp_f) AS avg_observed_temp_f,
  AVG(actual_round_strokes) AS avg_actual_round_strokes,
  AVG(predicted_round_strokes) AS avg_predicted_round_strokes,
  AVG(predicted_round_strokes_wind_reference) AS avg_predicted_round_strokes_wind_reference,
  AVG(estimated_wind_impact_strokes) AS avg_estimated_wind_impact_strokes,
  AVG(estimated_temperature_impact_strokes) AS avg_estimated_temperature_impact_strokes,
  AVG(estimated_total_weather_impact_strokes) AS avg_estimated_total_weather_impact_strokes
FROM {source}
GROUP BY 1,2,3,4,5,6
""".strip()
        return _ctas_sql(database=database, table_name=report_table_name, external_location=external_location, select_sql=select_sql)

    select_sql = f"""
SELECT
  event_year,
  tourn_id,
  event_name,
  round_number,
  division,
  round_date,
  COUNT(*) AS rounds_scored,
  COUNT(DISTINCT player_key) AS players_scored,
  AVG(observed_wind_mph) AS avg_observed_wind_mph,
  AVG(observed_temp_f) AS avg_observed_temp_f,
  AVG(actual_round_strokes) AS avg_actual_round_strokes,
  AVG(predicted_round_strokes) AS avg_predicted_round_strokes,
  AVG(predicted_round_strokes_wind_reference) AS avg_predicted_round_strokes_wind_reference,
  AVG(estimated_wind_impact_strokes) AS avg_estimated_wind_impact_strokes,
  AVG(estimated_temperature_impact_strokes) AS avg_estimated_temperature_impact_strokes,
  AVG(estimated_total_weather_impact_strokes) AS avg_estimated_total_weather_impact_strokes
FROM {source}
GROUP BY 1,2,3,4,5,6
""".strip()
    return _ctas_sql(database=database, table_name=report_table_name, external_location=external_location, select_sql=select_sql)
