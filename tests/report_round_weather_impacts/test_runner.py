from types import SimpleNamespace

import pandas as pd

import report_round_weather_impacts.runner as runner


def test_runner_skips_existing_success(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        bucket=None,
        ddb_table=None,
        dry_run=True,
        force_events=False,
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(s3_bucket="bucket", ddb_table="table", aws_region="us-east-1"),
    )
    monkeypatch.setattr(
        runner,
        "list_scored_event_objects",
        lambda **kwargs: [{"key": "gold/pdga/wind_effects/scored_rounds/event_year=2026/tourn_id=90008/scored_rounds.parquet", "etag": "e1", "size": 1, "last_modified": "x"}],
    )
    monkeypatch.setattr(
        runner,
        "get_report_checkpoint",
        lambda **kwargs: {"status": "success", "scored_input_fingerprint": runner._fingerprint_event_object({"key": "gold/pdga/wind_effects/scored_rounds/event_year=2026/tourn_id=90008/scored_rounds.parquet", "etag": "e1", "size": 1, "last_modified": "x"})},
    )

    assert runner.main() == 0


def test_runner_processes_and_publishes(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        bucket=None,
        ddb_table=None,
        dry_run=False,
        force_events=False,
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(s3_bucket="bucket", ddb_table="table", aws_region="us-east-1"),
    )
    monkeypatch.setattr(
        runner,
        "list_scored_event_objects",
        lambda **kwargs: [{"key": "gold/pdga/wind_effects/scored_rounds/event_year=2026/tourn_id=90008/scored_rounds.parquet", "etag": "e1", "size": 1, "last_modified": "x"}],
    )
    monkeypatch.setattr(runner, "get_report_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(
        runner,
        "load_scored_event_dataframe",
        lambda **kwargs: pd.DataFrame(
            [
                {
                    "event_year": 2026,
                    "tourn_id": 90008,
                    "round_number": 1,
                    "player_key": "P1",
                    "course_id": "101",
                    "layout_id": "201",
                    "division": "MA3",
                    "player_rating": 915,
                    "actual_round_strokes": 57,
                    "predicted_round_strokes": 58.0,
                    "predicted_round_strokes_wind_reference": 56.5,
                    "estimated_wind_impact_strokes": 1.5,
                    "estimated_temperature_impact_strokes": 0.5,
                    "estimated_total_weather_impact_strokes": 2.0,
                    "round_wind_speed_mps_mean": 4.0,
                    "round_temp_c_mean": 20.0,
                    "state": "TX",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        runner,
        "prepare_reporting_dataframe",
        lambda df: pd.DataFrame(
            [
                {
                    "event_year": 2026,
                    "tourn_id": 90008,
                    "event_name": "Test Event",
                    "state": "TX",
                    "city": "Austin",
                    "lat": 30.0,
                    "lon": -97.0,
                    "round_year": 2026,
                    "round_month": 4,
                    "round_month_label": "Apr",
                    "round_number": 1,
                    "player_key": "P1",
                    "division": "MA3",
                    "rating_band": "900-939",
                    "temperature_band_f": "60-69F",
                    "round_wind_speed_bucket": "light",
                    "course_id": "101",
                    "layout_id": "201",
                    "observed_wind_mph": 9.0,
                    "observed_temp_f": 68.0,
                    "actual_round_strokes": 57.0,
                    "predicted_round_strokes": 58.0,
                    "predicted_round_strokes_wind_reference": 56.5,
                    "estimated_wind_impact_strokes": 1.5,
                    "estimated_temperature_impact_strokes": 0.5,
                    "estimated_total_weather_impact_strokes": 2.0,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        runner,
        "build_event_contributions",
        lambda df: {"weather_overview": pd.DataFrame([{"event_year": 2026, "tourn_id": 90008, "rounds_scored": 1}])},
    )
    monkeypatch.setattr(runner, "write_intermediate_table", lambda **kwargs: "intermediate.parquet")
    monkeypatch.setattr(runner, "build_published_table", lambda **kwargs: pd.DataFrame([{"rounds_scored": 1}]))
    monkeypatch.setattr(runner, "write_published_table", lambda **kwargs: "published.parquet")

    checkpoint_calls = []
    summary_calls = []

    monkeypatch.setattr(runner, "put_report_checkpoint", lambda **kwargs: checkpoint_calls.append(kwargs))
    monkeypatch.setattr(runner, "put_report_run_summary", lambda **kwargs: summary_calls.append(kwargs))

    assert runner.main() == 0
    assert len(checkpoint_calls) == 1
    assert len(summary_calls) == 1
