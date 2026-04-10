from types import SimpleNamespace

import report_round_weather_impacts.runner as runner


def test_runner_dry_run_prints_plan(monkeypatch, capsys):
    args = SimpleNamespace(
        tables="weather_by_state,weather_by_event",
        bucket=None,
        ddb_table=None,
        athena_database=None,
        athena_workgroup=None,
        athena_results_s3_uri=None,
        source_table=None,
        base_table_name=None,
        dry_run=True,
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(
            s3_bucket="bucket",
            ddb_table="table",
            athena_database="pdga_analytics",
            athena_workgroup="pdga-analytics",
            athena_results_s3_uri="s3://athena-results/query-results/",
            athena_source_scored_table="scored_rounds",
            athena_reporting_base_table="reporting_base_rounds",
            aws_region="us-east-1",
        ),
    )

    assert runner.main() == 0
    out = capsys.readouterr().out
    assert "report_round_weather_impacts_plan" in out
    assert "weather_by_state" in out
    assert "weather_by_event" in out
    assert "CREATE TABLE pdga_analytics.reporting_base_rounds" in out


def test_runner_rebuilds_base_and_selected_tables(monkeypatch):
    args = SimpleNamespace(
        tables="weather_by_state,weather_by_event",
        bucket=None,
        ddb_table=None,
        athena_database=None,
        athena_workgroup=None,
        athena_results_s3_uri=None,
        source_table=None,
        base_table_name=None,
        dry_run=False,
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(
            s3_bucket="bucket",
            ddb_table="table",
            athena_database="pdga_analytics",
            athena_workgroup="pdga-analytics",
            athena_results_s3_uri="s3://athena-results/query-results/",
            athena_source_scored_table="scored_rounds",
            athena_reporting_base_table="reporting_base_rounds",
            aws_region="us-east-1",
        ),
    )

    delete_calls = []
    query_calls = []
    checkpoint_calls = []
    summary_calls = []

    monkeypatch.setattr(
        runner,
        "delete_s3_prefix",
        lambda **kwargs: delete_calls.append(kwargs) or 0,
    )
    monkeypatch.setattr(
        runner,
        "execute_athena_query",
        lambda **kwargs: query_calls.append(kwargs)
        or {
            "query_execution_id": f"q{len(query_calls)}",
            "state": "SUCCEEDED",
            "scanned_bytes": 1024,
            "engine_execution_time_ms": 250,
            "total_execution_time_ms": 350,
            "output_location": kwargs["output_location"],
            "sql": kwargs["sql"],
        },
    )
    monkeypatch.setattr(
        runner,
        "put_report_table_checkpoint",
        lambda **kwargs: checkpoint_calls.append(kwargs),
    )
    monkeypatch.setattr(
        runner,
        "put_report_run_summary",
        lambda **kwargs: summary_calls.append(kwargs),
    )

    assert runner.main() == 0

    assert len(delete_calls) == 3
    assert len(query_calls) == 6
    assert len(checkpoint_calls) == 2
    assert len(summary_calls) == 1
    assert checkpoint_calls[0]["report_table"] == "weather_by_state"
    assert checkpoint_calls[1]["report_table"] == "weather_by_event"

