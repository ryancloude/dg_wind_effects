from types import SimpleNamespace

import silver_weather_enriched.runner as runner
from silver_weather_enriched.dynamo_io import EnrichedEventCandidate


def _candidate():
    return EnrichedEventCandidate(
        event_id=90008,
        event_year=2026,
        round_s3_key="r.parquet",
        hole_s3_key="h.parquet",
        weather_s3_key="w.parquet",
    )


def test_is_pending_event_skips_failed_by_default():
    pending = runner._is_pending_event(
        90008,
        {90008: {"status": "failed", "event_source_fingerprint": "fp-1"}},
        include_failed=False,
        include_dq_failed=False,
    )
    assert pending is False


def test_is_pending_event_includes_failed_when_enabled():
    pending = runner._is_pending_event(
        90008,
        {90008: {"status": "failed", "event_source_fingerprint": "fp-1"}},
        include_failed=True,
        include_dq_failed=False,
    )
    assert pending is True


def test_main_pending_only_skips_success_with_same_fingerprint(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        bucket=None,
        ddb_table=None,
        dry_run=True,
        force_events=False,
        run_mode="pending_only",
        include_failed_events=False,
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
    monkeypatch.setattr(runner, "load_enriched_event_candidates", lambda **kwargs: [_candidate()])
    monkeypatch.setattr(
        runner,
        "load_enriched_event_checkpoints",
        lambda **kwargs: {90008: {"status": "success", "event_source_fingerprint": "fp-1"}},
    )
    monkeypatch.setattr(
        runner,
        "load_event_input_tables",
        lambda **kwargs: (
            [{"tourn_id": 90008, "round_number": 1, "player_key": "P1"}],
            [{"tourn_id": 90008, "round_number": 1, "hole_number": 1, "player_key": "P1"}],
            [{"event_id": 90008, "round_number": 1, "observation_hour_utc": "2026-03-17T08:00:00Z"}],
        ),
    )
    monkeypatch.setattr(runner, "compute_enriched_event_fingerprint", lambda **kwargs: "fp-1")
    monkeypatch.setattr(
        runner,
        "get_enriched_event_checkpoint",
        lambda **kwargs: {"status": "success", "event_source_fingerprint": "fp-1"},
    )

    exit_code = runner.main()
    assert exit_code == 0


def test_main_full_check_processes_success(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        bucket=None,
        ddb_table=None,
        dry_run=False,
        force_events=False,
        run_mode="full_check",
        include_failed_events=False,
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
    monkeypatch.setattr(runner, "load_enriched_event_candidates", lambda **kwargs: [_candidate()])
    monkeypatch.setattr(
        runner,
        "load_event_input_tables",
        lambda **kwargs: (
            [{"tourn_id": 90008, "round_number": 1, "player_key": "P1", "tee_time_join_ts": "2026-03-17T08:22:00Z"}],
            [{"tourn_id": 90008, "round_number": 1, "hole_number": 1, "player_key": "P1", "hole_time_est_ts": "2026-03-17T08:05:00Z"}],
            [{"event_id": 90008, "round_number": 1, "observation_hour_utc": "2026-03-17T08:00:00Z", "provider": "open_meteo_archive", "source_id": "GRID#A"}],
        ),
    )
    monkeypatch.setattr(runner, "compute_enriched_event_fingerprint", lambda **kwargs: "fp-new")
    monkeypatch.setattr(runner, "get_enriched_event_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(runner, "validate_enriched_quality", lambda **kwargs: [])
    monkeypatch.setattr(runner, "overwrite_event_tables", lambda **kwargs: {"round_key": "rk", "hole_key": "hk"})

    checkpoint_calls = []
    run_summary_calls = []

    monkeypatch.setattr(runner, "put_enriched_event_checkpoint", lambda **kwargs: checkpoint_calls.append(kwargs))
    monkeypatch.setattr(runner, "put_enriched_run_summary", lambda **kwargs: run_summary_calls.append(kwargs))

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
