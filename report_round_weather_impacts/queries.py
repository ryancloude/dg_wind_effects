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
    CAST(round_wind_gust_mps_mean AS double) AS round_wind_gust_mps_mean,
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
  round_wind_gust_mps_mean * 2.23694 AS observed_wind_gust_mph,
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
    WHEN COALESCE(round_precip_mm_sum, 0.0) > 0.0 THEN 'Precipitation'
    ELSE 'No Precipitation'
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
  CAST(0.0 AS double) AS reference_wind_mph,
  CAST(1.0 AS double) AS reference_gust_mph,
  CAST(80.0 AS double) AS reference_temp_f,
  CAST('No Precipitation' AS varchar) AS reference_precipitation,
  COUNT(*) AS rounds_tracked,
  COUNT(DISTINCT tourn_id) AS events_tracked,
  AVG(estimated_total_weather_impact_strokes) AS avg_added_strokes_weather,
  AVG(estimated_wind_impact_strokes) AS avg_added_strokes_wind,
  AVG(observed_wind_mph) AS avg_observed_wind_mph,
  AVG(observed_wind_gust_mph) AS avg_observed_wind_gust_mph,
  AVG(observed_temp_f) AS avg_observed_temp_f
FROM {source}
""".strip()
        return _ctas_sql(
            database=database,
            table_name=report_table_name,
            external_location=external_location,
            select_sql=select_sql,
        )

    if report_table_name == "weather_by_wind_bucket":
        select_sql = f"""
SELECT
  round_wind_speed_bucket,
  COUNT(*) AS rounds_scored,
  COUNT(DISTINCT tourn_id) AS events_scored,
  AVG(observed_wind_mph) AS avg_observed_wind_mph,
  AVG(observed_wind_gust_mph) AS avg_observed_wind_gust_mph,
  AVG(estimated_wind_impact_strokes) AS avg_estimated_wind_impact_strokes,
  AVG(estimated_total_weather_impact_strokes) AS avg_estimated_total_weather_impact_strokes
FROM {source}
GROUP BY 1
ORDER BY 1
""".strip()
        return _ctas_sql(database=database, table_name=report_table_name, external_location=external_location, select_sql=select_sql)

    if report_table_name == "weather_impact_distribution":
        select_sql = f"""
WITH bins AS (
  SELECT *
  FROM (
    VALUES
      ('total_added_strokes_weather', 'Total Added Strokes from Weather', '< -1.0', CAST(NULL AS double), CAST(-1.0 AS double), CAST(NULL AS varchar), 0),
      ('total_added_strokes_weather', 'Total Added Strokes from Weather', '-1.0 to -0.5', CAST(-1.0 AS double), CAST(-0.5 AS double), CAST(NULL AS varchar), 1),
      ('total_added_strokes_weather', 'Total Added Strokes from Weather', '-0.5 to 0.0', CAST(-0.5 AS double), CAST(0.0 AS double), CAST(NULL AS varchar), 2),
      ('total_added_strokes_weather', 'Total Added Strokes from Weather', '0.0 to 0.5', CAST(0.0 AS double), CAST(0.5 AS double), CAST(NULL AS varchar), 3),
      ('total_added_strokes_weather', 'Total Added Strokes from Weather', '0.5 to 1.0', CAST(0.5 AS double), CAST(1.0 AS double), CAST(NULL AS varchar), 4),
      ('total_added_strokes_weather', 'Total Added Strokes from Weather', '1.0 to 1.5', CAST(1.0 AS double), CAST(1.5 AS double), CAST(NULL AS varchar), 5),
      ('total_added_strokes_weather', 'Total Added Strokes from Weather', '1.5 to 2.0', CAST(1.5 AS double), CAST(2.0 AS double), CAST(NULL AS varchar), 6),
      ('total_added_strokes_weather', 'Total Added Strokes from Weather', '2.0 to 2.5', CAST(2.0 AS double), CAST(2.5 AS double), CAST(NULL AS varchar), 7),
      ('total_added_strokes_weather', 'Total Added Strokes from Weather', '2.5 to 3.0', CAST(2.5 AS double), CAST(3.0 AS double), CAST(NULL AS varchar), 8),
      ('total_added_strokes_weather', 'Total Added Strokes from Weather', '3.0 to 3.5', CAST(3.0 AS double), CAST(3.5 AS double), CAST(NULL AS varchar), 9),
      ('total_added_strokes_weather', 'Total Added Strokes from Weather', '3.5 to 4.0', CAST(3.5 AS double), CAST(4.0 AS double), CAST(NULL AS varchar), 10),
      ('total_added_strokes_weather', 'Total Added Strokes from Weather', '>= 4.0', CAST(4.0 AS double), CAST(NULL AS double), CAST(NULL AS varchar), 11),

      ('added_strokes_wind', 'Added Strokes from Wind', '< -1.0', CAST(NULL AS double), CAST(-1.0 AS double), CAST(NULL AS varchar), 100),
      ('added_strokes_wind', 'Added Strokes from Wind', '-1.0 to -0.5', CAST(-1.0 AS double), CAST(-0.5 AS double), CAST(NULL AS varchar), 101),
      ('added_strokes_wind', 'Added Strokes from Wind', '-0.5 to 0.0', CAST(-0.5 AS double), CAST(0.0 AS double), CAST(NULL AS varchar), 102),
      ('added_strokes_wind', 'Added Strokes from Wind', '0.0 to 0.5', CAST(0.0 AS double), CAST(0.5 AS double), CAST(NULL AS varchar), 103),
      ('added_strokes_wind', 'Added Strokes from Wind', '0.5 to 1.0', CAST(0.5 AS double), CAST(1.0 AS double), CAST(NULL AS varchar), 104),
      ('added_strokes_wind', 'Added Strokes from Wind', '1.0 to 1.5', CAST(1.0 AS double), CAST(1.5 AS double), CAST(NULL AS varchar), 105),
      ('added_strokes_wind', 'Added Strokes from Wind', '1.5 to 2.0', CAST(1.5 AS double), CAST(2.0 AS double), CAST(NULL AS varchar), 106),
      ('added_strokes_wind', 'Added Strokes from Wind', '2.0 to 2.5', CAST(2.0 AS double), CAST(2.5 AS double), CAST(NULL AS varchar), 107),
      ('added_strokes_wind', 'Added Strokes from Wind', '2.5 to 3.0', CAST(2.5 AS double), CAST(3.0 AS double), CAST(NULL AS varchar), 108),
      ('added_strokes_wind', 'Added Strokes from Wind', '3.0 to 3.5', CAST(3.0 AS double), CAST(3.5 AS double), CAST(NULL AS varchar), 109),
      ('added_strokes_wind', 'Added Strokes from Wind', '3.5 to 4.0', CAST(3.5 AS double), CAST(4.0 AS double), CAST(NULL AS varchar), 110),
      ('added_strokes_wind', 'Added Strokes from Wind', '>= 4.0', CAST(4.0 AS double), CAST(NULL AS double), CAST(NULL AS varchar), 111),

      ('observed_average_wind_speed', 'Observed Average Wind Speed', '0 to 3 mph', CAST(0.0 AS double), CAST(3.0 AS double), CAST(NULL AS varchar), 200),
      ('observed_average_wind_speed', 'Observed Average Wind Speed', '3 to 6 mph', CAST(3.0 AS double), CAST(6.0 AS double), CAST(NULL AS varchar), 201),
      ('observed_average_wind_speed', 'Observed Average Wind Speed', '6 to 9 mph', CAST(6.0 AS double), CAST(9.0 AS double), CAST(NULL AS varchar), 202),
      ('observed_average_wind_speed', 'Observed Average Wind Speed', '9 to 12 mph', CAST(9.0 AS double), CAST(12.0 AS double), CAST(NULL AS varchar), 203),
      ('observed_average_wind_speed', 'Observed Average Wind Speed', '12 to 15 mph', CAST(12.0 AS double), CAST(15.0 AS double), CAST(NULL AS varchar), 204),
      ('observed_average_wind_speed', 'Observed Average Wind Speed', '15+ mph', CAST(15.0 AS double), CAST(NULL AS double), CAST(NULL AS varchar), 205),

      ('observed_average_wind_gust_speed', 'Observed Average Wind Gust Speed', '0 to 5 mph', CAST(0.0 AS double), CAST(5.0 AS double), CAST(NULL AS varchar), 300),
      ('observed_average_wind_gust_speed', 'Observed Average Wind Gust Speed', '5 to 10 mph', CAST(5.0 AS double), CAST(10.0 AS double), CAST(NULL AS varchar), 301),
      ('observed_average_wind_gust_speed', 'Observed Average Wind Gust Speed', '10 to 15 mph', CAST(10.0 AS double), CAST(15.0 AS double), CAST(NULL AS varchar), 302),
      ('observed_average_wind_gust_speed', 'Observed Average Wind Gust Speed', '15 to 20 mph', CAST(15.0 AS double), CAST(20.0 AS double), CAST(NULL AS varchar), 303),
      ('observed_average_wind_gust_speed', 'Observed Average Wind Gust Speed', '20 to 25 mph', CAST(20.0 AS double), CAST(25.0 AS double), CAST(NULL AS varchar), 304),
      ('observed_average_wind_gust_speed', 'Observed Average Wind Gust Speed', '25+ mph', CAST(25.0 AS double), CAST(NULL AS double), CAST(NULL AS varchar), 305),

      ('observed_temperature', 'Observed Temperature', '< 32 F', CAST(NULL AS double), CAST(32.0 AS double), CAST(NULL AS varchar), 400),
      ('observed_temperature', 'Observed Temperature', '32 to 40 F', CAST(32.0 AS double), CAST(40.0 AS double), CAST(NULL AS varchar), 401),
      ('observed_temperature', 'Observed Temperature', '40 to 50 F', CAST(40.0 AS double), CAST(50.0 AS double), CAST(NULL AS varchar), 402),
      ('observed_temperature', 'Observed Temperature', '50 to 60 F', CAST(50.0 AS double), CAST(60.0 AS double), CAST(NULL AS varchar), 403),
      ('observed_temperature', 'Observed Temperature', '60 to 70 F', CAST(60.0 AS double), CAST(70.0 AS double), CAST(NULL AS varchar), 404),
      ('observed_temperature', 'Observed Temperature', '70 to 80 F', CAST(70.0 AS double), CAST(80.0 AS double), CAST(NULL AS varchar), 405),
      ('observed_temperature', 'Observed Temperature', '80 to 90 F', CAST(80.0 AS double), CAST(90.0 AS double), CAST(NULL AS varchar), 406),
      ('observed_temperature', 'Observed Temperature', '>= 90 F', CAST(90.0 AS double), CAST(NULL AS double), CAST(NULL AS varchar), 407),

      ('observed_precipitation', 'Observed Precipitation', 'No Precipitation', CAST(NULL AS double), CAST(NULL AS double), 'No Precipitation', 500),
      ('observed_precipitation', 'Observed Precipitation', 'Precipitation', CAST(NULL AS double), CAST(NULL AS double), 'Precipitation', 501)
  ) AS t (metric_name, metric_label, bin_label, bin_start, bin_end, category_value, sort_order)
),
source_rows AS (
  SELECT
    'total_added_strokes_weather' AS metric_name,
    estimated_total_weather_impact_strokes AS metric_value,
    CAST(NULL AS varchar) AS category_value
  FROM {source}
  WHERE estimated_total_weather_impact_strokes IS NOT NULL

  UNION ALL

  SELECT
    'added_strokes_wind' AS metric_name,
    estimated_wind_impact_strokes AS metric_value,
    CAST(NULL AS varchar) AS category_value
  FROM {source}
  WHERE estimated_wind_impact_strokes IS NOT NULL

  UNION ALL

  SELECT
    'observed_average_wind_speed' AS metric_name,
    observed_wind_mph AS metric_value,
    CAST(NULL AS varchar) AS category_value
  FROM {source}
  WHERE observed_wind_mph IS NOT NULL

  UNION ALL

  SELECT
    'observed_average_wind_gust_speed' AS metric_name,
    observed_wind_gust_mph AS metric_value,
    CAST(NULL AS varchar) AS category_value
  FROM {source}
  WHERE observed_wind_gust_mph IS NOT NULL

  UNION ALL

  SELECT
    'observed_temperature' AS metric_name,
    observed_temp_f AS metric_value,
    CAST(NULL AS varchar) AS category_value
  FROM {source}
  WHERE observed_temp_f IS NOT NULL

  UNION ALL

  SELECT
    'observed_precipitation' AS metric_name,
    CAST(NULL AS double) AS metric_value,
    precip_flag AS category_value
  FROM {source}
  WHERE precip_flag IS NOT NULL
),
bin_counts AS (
  SELECT
    b.metric_name,
    b.metric_label,
    b.bin_label,
    b.bin_start,
    b.bin_end,
    b.sort_order,
    COUNT(s.metric_name) AS rounds_tracked
  FROM bins b
  LEFT JOIN source_rows s
    ON b.metric_name = s.metric_name
   AND (
      (
        b.category_value IS NOT NULL
        AND s.category_value = b.category_value
      )
      OR
      (
        b.category_value IS NULL
        AND (
          (b.bin_start IS NULL AND s.metric_value < b.bin_end)
          OR
          (b.bin_end IS NULL AND s.metric_value >= b.bin_start)
          OR
          (
            b.bin_start IS NOT NULL
            AND b.bin_end IS NOT NULL
            AND s.metric_value >= b.bin_start
            AND s.metric_value < b.bin_end
          )
        )
      )
   )
  GROUP BY 1,2,3,4,5,6
)
SELECT
  metric_name,
  metric_label,
  bin_label,
  bin_start,
  bin_end,
  rounds_tracked,
  CAST(rounds_tracked AS double) / NULLIF(SUM(rounds_tracked) OVER (PARTITION BY metric_name), 0) AS share_of_rounds,
  sort_order
FROM bin_counts
ORDER BY metric_name, sort_order
""".strip()
        return _ctas_sql(
            database=database,
            table_name=report_table_name,
            external_location=external_location,
            select_sql=select_sql,
        )

    if report_table_name == "weather_wind_impact_points":
        select_sql = f"""
WITH source_rows AS (
  SELECT
    'wind_speed' AS bucket_metric,
    CASE
      WHEN observed_wind_mph < 3 THEN '0-3 mph'
      WHEN observed_wind_mph < 6 THEN '3-6 mph'
      WHEN observed_wind_mph < 9 THEN '6-9 mph'
      WHEN observed_wind_mph < 12 THEN '9-12 mph'
      WHEN observed_wind_mph < 15 THEN '12-15 mph'
      ELSE '15+ mph'
    END AS bucket_label,
    CASE
      WHEN observed_wind_mph < 3 THEN 0
      WHEN observed_wind_mph < 6 THEN 1
      WHEN observed_wind_mph < 9 THEN 2
      WHEN observed_wind_mph < 12 THEN 3
      WHEN observed_wind_mph < 15 THEN 4
      ELSE 5
    END AS sort_order,
    estimated_wind_impact_strokes AS added_strokes_from_wind
  FROM {source}
  WHERE observed_wind_mph IS NOT NULL
    AND estimated_wind_impact_strokes IS NOT NULL

  UNION ALL

  SELECT
    'wind_gust' AS bucket_metric,
    CASE
      WHEN observed_wind_gust_mph < 5 THEN '0-5 mph'
      WHEN observed_wind_gust_mph < 10 THEN '5-10 mph'
      WHEN observed_wind_gust_mph < 15 THEN '10-15 mph'
      WHEN observed_wind_gust_mph < 20 THEN '15-20 mph'
      WHEN observed_wind_gust_mph < 25 THEN '20-25 mph'
      ELSE '25+ mph'
    END AS bucket_label,
    CASE
      WHEN observed_wind_gust_mph < 5 THEN 100
      WHEN observed_wind_gust_mph < 10 THEN 101
      WHEN observed_wind_gust_mph < 15 THEN 102
      WHEN observed_wind_gust_mph < 20 THEN 103
      WHEN observed_wind_gust_mph < 25 THEN 104
      ELSE 105
    END AS sort_order,
    estimated_wind_impact_strokes AS added_strokes_from_wind
  FROM {source}
  WHERE observed_wind_gust_mph IS NOT NULL
    AND estimated_wind_impact_strokes IS NOT NULL
)
SELECT
  bucket_metric,
  bucket_label,
  sort_order,
  COUNT(*) AS rounds_tracked,
  AVG(added_strokes_from_wind) AS avg_added_strokes_from_wind
FROM source_rows
GROUP BY 1,2,3
ORDER BY sort_order
""".strip()
        return _ctas_sql(
            database=database,
            table_name=report_table_name,
            external_location=external_location,
            select_sql=select_sql,
        )

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
  round_month,
  round_month_label,
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
GROUP BY 1,2,3
ORDER BY 1,3
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
  AVG(observed_wind_gust_mph) AS avg_observed_wind_gust_mph,
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
  AVG(observed_wind_gust_mph) AS avg_observed_wind_gust_mph,
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
