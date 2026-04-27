from report_round_weather_impacts.queries import (
    build_drop_table_sql,
    build_report_ctas_sql,
    build_reporting_base_ctas_sql,
)


def test_build_drop_table_sql():
    sql = build_drop_table_sql(database="pdga_analytics", table_name="weather_by_state")
    assert sql == "DROP TABLE IF EXISTS pdga_analytics.weather_by_state"


def test_build_reporting_base_ctas_sql_uses_source_table_and_location():
    sql = build_reporting_base_ctas_sql(
        database="pdga_analytics",
        source_table="scored_rounds",
        base_table_name="reporting_base_rounds",
        external_location="s3://bucket/gold/pdga/wind_effects/reports/athena/base/reporting_base_rounds/",
    )

    assert "CREATE TABLE pdga_analytics.reporting_base_rounds" in sql
    assert "FROM pdga_analytics.scored_rounds" in sql
    assert "external_location = 's3://bucket/gold/pdga/wind_effects/reports/athena/base/reporting_base_rounds/'" in sql
    assert "rating_band" in sql
    assert "temperature_band_f" in sql
    assert "state" in sql
    assert "observed_wind_gust_mph" in sql
    assert "'No Precipitation'" in sql


def test_build_report_ctas_sql_for_weather_overview_matches_new_overview_page():
    sql = build_report_ctas_sql(
        database="pdga_analytics",
        base_table_name="reporting_base_rounds",
        report_table_name="weather_overview",
        external_location="s3://bucket/gold/pdga/wind_effects/reports/published/weather_overview/",
    )

    assert "CREATE TABLE pdga_analytics.weather_overview" in sql
    assert "reference_wind_mph" in sql
    assert "reference_gust_mph" in sql
    assert "reference_temp_f" in sql
    assert "reference_precipitation" in sql
    assert "rounds_tracked" in sql
    assert "events_tracked" in sql
    assert "avg_added_strokes_weather" in sql
    assert "avg_added_strokes_wind" in sql
    assert "avg_observed_wind_mph" in sql
    assert "avg_observed_wind_gust_mph" in sql


def test_build_report_ctas_sql_for_distribution_table_has_expected_bins():
    sql = build_report_ctas_sql(
        database="pdga_analytics",
        base_table_name="reporting_base_rounds",
        report_table_name="weather_impact_distribution",
        external_location="s3://bucket/gold/pdga/wind_effects/reports/published/weather_impact_distribution/",
    )

    assert "CREATE TABLE pdga_analytics.weather_impact_distribution" in sql
    assert "'total_added_strokes_weather'" in sql
    assert "'added_strokes_wind'" in sql
    assert "'observed_average_wind_speed'" in sql
    assert "'observed_average_wind_gust_speed'" in sql
    assert "'observed_temperature'" in sql
    assert "'observed_precipitation'" in sql
    assert "'< -1.0'" in sql
    assert "'>= 4.0'" in sql
    assert "'15+ mph'" in sql
    assert "'25+ mph'" in sql
    assert "share_of_rounds" in sql
    assert "rounds_tracked" in sql


def test_build_report_ctas_sql_for_weather_wind_impact_points_supports_speed_and_gust():
    sql = build_report_ctas_sql(
        database="pdga_analytics",
        base_table_name="reporting_base_rounds",
        report_table_name="weather_wind_impact_points",
        external_location="s3://bucket/gold/pdga/wind_effects/reports/published/weather_wind_impact_points/",
    )

    assert "CREATE TABLE pdga_analytics.weather_wind_impact_points" in sql
    assert "'wind_speed' AS bucket_metric" in sql
    assert "'wind_gust' AS bucket_metric" in sql
    assert "'0-3 mph'" in sql
    assert "'15+ mph'" in sql
    assert "'0-5 mph'" in sql
    assert "'25+ mph'" in sql
    assert "AVG(added_strokes_from_wind) AS avg_added_strokes_from_wind" in sql
    assert "COUNT(*) AS rounds_tracked" in sql


def test_build_report_ctas_sql_for_weather_by_state_uses_month_grain():
    sql = build_report_ctas_sql(
        database="pdga_analytics",
        base_table_name="reporting_base_rounds",
        report_table_name="weather_by_state",
        external_location="s3://bucket/gold/pdga/wind_effects/reports/published/weather_by_state/",
    )

    assert "CREATE TABLE pdga_analytics.weather_by_state" in sql
    assert "FROM pdga_analytics.reporting_base_rounds" in sql
    assert "round_month" in sql
    assert "round_month_label" in sql
    assert "GROUP BY 1,2,3" in sql
    assert "avg_estimated_wind_impact_strokes" in sql.lower()


def test_build_report_ctas_sql_for_weather_by_event_includes_gust_metric():
    sql = build_report_ctas_sql(
        database="pdga_analytics",
        base_table_name="reporting_base_rounds",
        report_table_name="weather_by_event",
        external_location="s3://bucket/gold/pdga/wind_effects/reports/published/weather_by_event/",
    )

    assert "CREATE TABLE pdga_analytics.weather_by_event" in sql
    assert "AVG(observed_wind_gust_mph) AS avg_observed_wind_gust_mph" in sql
    assert "AVG(estimated_total_weather_impact_strokes)" in sql


def test_build_report_ctas_sql_for_event_round_includes_round_grain_and_gust():
    sql = build_report_ctas_sql(
        database="pdga_analytics",
        base_table_name="reporting_base_rounds",
        report_table_name="weather_by_event_round",
        external_location="s3://bucket/gold/pdga/wind_effects/reports/published/weather_by_event_round/",
    )

    assert "round_number" in sql
    assert "round_date" in sql
    assert "event_name" in sql
    assert "AVG(observed_wind_gust_mph) AS avg_observed_wind_gust_mph" in sql
