import score_round_wind_model.athena_io as athena_io


def test_build_add_partition_sql():
    sql = athena_io.build_add_partition_sql(
        database="pdga_analytics",
        table_name="scored_rounds",
        event_year=2026,
        event_id=90008,
        partition_location="s3://bucket/gold/pdga/wind_effects/scored_rounds/event_year=2026/tourn_id=90008/",
    )

    assert "ALTER TABLE pdga_analytics.scored_rounds" in sql
    assert "ADD IF NOT EXISTS" in sql
    assert "event_year = 2026" in sql
    assert "tourn_id = 90008" in sql
    assert "LOCATION 's3://bucket/gold/pdga/wind_effects/scored_rounds/event_year=2026/tourn_id=90008/'" in sql


def test_register_scored_round_partition_executes_athena_query(monkeypatch):
    execute_calls = []

    def fake_execute_athena_query(**kwargs):
        execute_calls.append(kwargs)
        return {
            "query_execution_id": "qe-123",
            "state": "SUCCEEDED",
            "sql": kwargs["sql"],
        }

    monkeypatch.setattr(athena_io, "execute_athena_query", fake_execute_athena_query)

    result = athena_io.register_scored_round_partition(
        database="pdga_analytics",
        table_name="scored_rounds",
        workgroup="pdga-analytics",
        output_location="s3://athena-results/query-results/",
        aws_region="us-east-2",
        event_year=2026,
        event_id=90008,
        partition_location="s3://bucket/gold/pdga/wind_effects/scored_rounds/event_year=2026/tourn_id=90008/",
    )

    assert len(execute_calls) == 1
    call = execute_calls[0]
    assert call["database"] == "pdga_analytics"
    assert call["workgroup"] == "pdga-analytics"
    assert call["output_location"] == "s3://athena-results/query-results/"
    assert call["aws_region"] == "us-east-2"
    assert "ALTER TABLE pdga_analytics.scored_rounds" in call["sql"]

    assert result["query_execution_id"] == "qe-123"
    assert result["event_year"] == 2026
    assert result["event_id"] == 90008
    assert result["table_name"] == "scored_rounds"
    assert result["partition_location"] == "s3://bucket/gold/pdga/wind_effects/scored_rounds/event_year=2026/tourn_id=90008/"


def test_execute_athena_query_returns_wait_result(monkeypatch):
    monkeypatch.setattr(
        athena_io,
        "start_athena_query",
        lambda **kwargs: "qe-456",
    )
    monkeypatch.setattr(
        athena_io,
        "wait_for_query",
        lambda **kwargs: {
            "query_execution_id": kwargs["query_execution_id"],
            "state": "SUCCEEDED",
            "scanned_bytes": 0,
            "engine_execution_time_ms": 12,
            "total_execution_time_ms": 20,
            "output_location": "s3://athena-results/query-results/qe-456.csv",
        },
    )

    result = athena_io.execute_athena_query(
        sql="SELECT 1",
        database="pdga_analytics",
        workgroup="pdga-analytics",
        output_location="s3://athena-results/query-results/",
        aws_region="us-east-2",
    )

    assert result["query_execution_id"] == "qe-456"
    assert result["state"] == "SUCCEEDED"
    assert result["sql"] == "SELECT 1"
