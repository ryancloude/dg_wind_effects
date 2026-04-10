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


def test_build_report_ctas_sql_for_weather_by_state_uses_base_table():
    sql = build_report_ctas_sql(
        database="pdga_analytics",
        base_table_name="reporting_base_rounds",
        report_table_name="weather_by_state",
        external_location="s3://bucket/gold/pdga/wind_effects/reports/published/weather_by_state/",
    )

    assert "CREATE TABLE pdga_analytics.weather_by_state" in sql
    assert "FROM pdga_analytics.reporting_base_rounds" in sql
    assert "GROUP BY 1" in sql
    assert "avg_estimated_wind_impact_strokes" in sql.lower()


def test_build_report_ctas_sql_for_event_round_includes_round_grain():
    sql = build_report_ctas_sql(
        database="pdga_analytics",
        base_table_name="reporting_base_rounds",
        report_table_name="weather_by_event_round",
        external_location="s3://bucket/gold/pdga/wind_effects/reports/published/weather_by_event_round/",
    )

    assert "round_number" in sql
    assert "round_date" in sql
    assert "event_name" in sql
