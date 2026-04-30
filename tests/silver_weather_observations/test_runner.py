from types import SimpleNamespace

import silver_weather_observations.runner as runner
from silver_weather_observations.models import BronzeWeatherRoundSource


def _source() -> BronzeWeatherRoundSource:
    return BronzeWeatherRoundSource(
        event_id=90008,
        round_number=1,
        provider="open_meteo_archive",
        source_id="GRID#A",
        source_json_key="bronze/weather/a.json",
        source_meta_key="bronze/weather/a.meta.json",
        source_content_sha256="h1",
        source_fetched_at_utc="2026-03-16T12:00:00Z",
        request_fingerprint="req1",
        tee_time_source_fingerprint="tee1",
        payload={"hourly": {"time": ["2026-03-10T08:00"], "wind_speed_10m": [4.2]}},
    )


def _row() -> dict:
    return {
        "weather_obs_pk": "pk1",
        "event_id": 90008,
        "event_year": 2026,
        "round_number": 1,
        "provider": "open_meteo_archive",
        "source_id": "GRID#A",
        "observation_ts_utc": "2026-03-10T08:00:00Z",
        "observation_hour_utc": "2026-03-10T08:00:00Z",
        "wind_speed_mps": 4.2,
        "wind_gust_mps": 6.0,
        "wind_dir_deg": 120.0,
        "temperature_c": 19.0,
        "pressure_hpa": 1012.0,
        "relative_humidity_pct": 70.0,
        "precip_mm": 0.0,
        "source_json_key": "bronze/weather/a.json",
        "source_content_sha256": "h1",
        "source_fetched_at_utc": "2026-03-16T12:00:00Z",
        "silver_run_id": "run1",
    }


def test_main_pending_only_skips_success_with_fingerprint(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        bucket=None,
        ddb_table=None,
        dry_run=True,
        force_events=False,
        run_mode="pending_only",
        include_dq_failed_in_pending=False,
        progress_every=10,
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
        "load_weather_event_summaries",
        lambda **kwargs: [{"event_id": 90008}],
    )
    monkeypatch.setattr(
        runner,
        "load_silver_weather_event_checkpoints",
        lambda **kwargs: {90008: {"status": "success", "event_source_fingerprint": "fp-ok"}},
    )

    printed = []
    monkeypatch.setattr(runner, "print", lambda obj: printed.append(obj), raising=False)

    exit_code = runner.main()
    assert exit_code == 0
    assert any("silver_weather_summary" in p for p in printed)


def test_main_full_check_processes_success(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        bucket=None,
        ddb_table=None,
        dry_run=False,
        force_events=False,
        run_mode="full_check",
        include_dq_failed_in_pending=False,
        progress_every=10,
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(s3_bucket="bucket", ddb_table="table", aws_region="us-east-1"),
    )
    monkeypatch.setattr(runner, "load_weather_event_summaries", lambda **kwargs: [{"event_id": 90008}])
    monkeypatch.setattr(runner, "get_event_metadata", lambda **kwargs: {"event_id": 90008, "start_date": "2026-03-10"})
    monkeypatch.setattr(runner, "load_weather_state_items", lambda **kwargs: [{"sk": "WEATHER_OBS#ROUND#1#PROV#open_meteo_archive#SRC#GRID#A"}])
    monkeypatch.setattr(runner, "build_weather_round_sources", lambda **kwargs: [_source()])
    monkeypatch.setattr(runner, "compute_event_source_fingerprint", lambda sources: "fp-new")
    monkeypatch.setattr(runner, "get_silver_weather_event_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(runner, "normalize_event_records", lambda **kwargs: [_row()])
    monkeypatch.setattr(runner, "validate_quality", lambda rows: [])
    monkeypatch.setattr(runner, "overwrite_event_table", lambda **kwargs: {"observations_key": "silver/weather/x.parquet"})

    checkpoint_calls = []
    run_summary_calls = []

    monkeypatch.setattr(
        runner,
        "put_silver_weather_event_checkpoint",
        lambda **kwargs: checkpoint_calls.append(kwargs),
    )
    monkeypatch.setattr(
        runner,
        "put_silver_weather_run_summary",
        lambda **kwargs: run_summary_calls.append(kwargs),
    )

    exit_code = runner.main()
    assert exit_code == 0
    assert len(checkpoint_calls) == 1
    assert checkpoint_calls[0]["status"] == "success"
    assert len(run_summary_calls) == 1


def test_should_exit_nonzero_when_failure_rate_at_threshold():
    stats = runner.RunStats(attempted_events=10, failed_events=5)
    assert runner._should_exit_nonzero(stats=stats, max_failure_rate=0.5) is True


def test_should_not_exit_nonzero_when_failure_rate_below_threshold():
    stats = runner.RunStats(attempted_events=10, failed_events=4)
    assert runner._should_exit_nonzero(stats=stats, max_failure_rate=0.5) is False
