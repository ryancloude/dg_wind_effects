from types import SimpleNamespace

import ingest_weather_observations.runner as runner
from ingest_weather_observations.dynamo_reader import WeatherEventCandidate


def _candidate(
    event_id: int,
    silver_updated: str = "2026-03-16T00:00:00Z",
    include_coords: bool = True,
):
    metadata = {
        "event_id": event_id,
        "name": f"Event {event_id}",
        "start_date": "2026-03-10",
        "end_date": "2026-03-11",
        "division_rounds": {"MPO": 2},
        "city": "Austin",
        "state": "Texas",
        "country": "United States",
    }
    if include_coords:
        metadata["latitude"] = 30.2672
        metadata["longitude"] = -97.7431

    return WeatherEventCandidate(
        event_id=event_id,
        event_metadata=metadata,
        silver_checkpoint={
            "event_id": event_id,
            "status": "success",
            "round_s3_key": f"silver/pdga/live_results/player_rounds/event_year=2026/tourn_id={event_id}/player_rounds.parquet",
            "updated_at": silver_updated,
        },
    )


def test_incremental_skip_when_silver_checkpoint_unchanged():
    candidate = _candidate(90008, "2026-03-16T00:00:00Z")
    summary = {"last_silver_checkpoint_updated_at": "2026-03-16T00:00:00Z"}
    assert runner._is_incremental_skip(candidate=candidate, summary_item=summary) is True


def test_incremental_process_when_silver_checkpoint_changed():
    candidate = _candidate(90008, "2026-03-17T00:00:00Z")
    summary = {"last_silver_checkpoint_updated_at": "2026-03-16T00:00:00Z"}
    assert runner._is_incremental_skip(candidate=candidate, summary_item=summary) is False


def test_is_failed_summary_true_for_failed_status():
    assert runner._is_failed_summary({"status": "failed"}) is True


def test_select_candidates_for_run_skips_failed_by_default():
    candidates = [
        _candidate(90008, "2026-03-16T00:00:00Z"),
        _candidate(90009, "2026-03-17T00:00:00Z"),
        _candidate(90010, "2026-03-18T00:00:00Z"),
    ]
    summaries = {
        90008: {"status": "failed", "last_silver_checkpoint_updated_at": "2026-03-15T00:00:00Z"},
        90009: {"status": "success", "last_silver_checkpoint_updated_at": "2026-03-17T00:00:00Z"},
        90010: None,
    }

    selected, skipped_incremental, skipped_failed = runner._select_candidates_for_run(
        candidates=candidates,
        summaries_by_event_id=summaries,
        mode_incremental=True,
        force_events=False,
        include_failed_events=False,
    )

    assert [c.event_id for c in selected] == [90010]
    assert skipped_incremental == 1
    assert skipped_failed == 1


def test_select_candidates_for_run_includes_failed_when_flag_set():
    candidates = [_candidate(90008)]
    summaries = {
        90008: {"status": "failed", "last_silver_checkpoint_updated_at": "2026-03-15T00:00:00Z"},
    }

    selected, skipped_incremental, skipped_failed = runner._select_candidates_for_run(
        candidates=candidates,
        summaries_by_event_id=summaries,
        mode_incremental=True,
        force_events=False,
        include_failed_events=True,
    )

    assert [c.event_id for c in selected] == [90008]
    assert skipped_incremental == 0
    assert skipped_failed == 0


def test_should_exit_nonzero_when_failure_rate_at_threshold():
    stats = runner.RunStats(attempted_events=10, failed_events=5)
    assert runner._should_exit_nonzero(stats=stats, max_failure_rate=0.5) is True


def test_should_not_exit_nonzero_when_failure_rate_below_threshold():
    stats = runner.RunStats(attempted_events=10, failed_events=4)
    assert runner._should_exit_nonzero(stats=stats, max_failure_rate=0.5) is False


def test_resolve_event_geopoint_uses_metadata_first(monkeypatch):
    candidate = _candidate(90008, include_coords=True)

    point, source = runner._resolve_event_geopoint(
        candidate=candidate,
        table_name="tbl",
        aws_region="us-east-1",
        session=object(),
        http_cfg=SimpleNamespace(timeout_s=30),
        dry_run=True,
        run_id="run-1",
    )

    assert source == "metadata"
    assert round(point.latitude, 4) == 30.2672


def test_resolve_event_geopoint_uses_cache_when_metadata_missing(monkeypatch):
    candidate = _candidate(90008, include_coords=False)

    monkeypatch.setattr(
        runner,
        "get_cached_geocode",
        lambda **kwargs: {
            "latitude": 30.2672,
            "longitude": -97.7431,
        },
    )

    point, source = runner._resolve_event_geopoint(
        candidate=candidate,
        table_name="tbl",
        aws_region="us-east-1",
        session=object(),
        http_cfg=SimpleNamespace(timeout_s=30),
        dry_run=True,
        run_id="run-1",
    )

    assert source == "cache"
    assert round(point.latitude, 4) == 30.2672


def test_main_incremental_default_skips_unchanged_and_failed_events(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        incremental=False,
        historical_backfill=False,
        bucket=None,
        ddb_table=None,
        dry_run=True,
        force_events=False,
        include_failed_events=False,
        round_padding_days=0,
        timeout=30,
        progress_every=10,
        max_failure_rate=0.5,
        log_level="INFO",
    )
    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(s3_bucket="bucket", ddb_table="table", aws_region="us-east-1"),
    )
    monkeypatch.setattr(runner, "build_session", lambda cfg: "session")

    candidates = [_candidate(90008), _candidate(90009)]
    monkeypatch.setattr(runner, "load_weather_event_candidates", lambda **kwargs: candidates)
    monkeypatch.setattr(
        runner,
        "get_event_weather_summaries",
        lambda **kwargs: {
            90008: {"status": "success", "last_silver_checkpoint_updated_at": "2026-03-16T00:00:00Z"},
            90009: {"status": "failed", "last_silver_checkpoint_updated_at": "2026-03-15T00:00:00Z"},
        },
    )

    printed = []
    monkeypatch.setattr(runner, "print", lambda obj: printed.append(obj), raising=False)

    exit_code = runner.main()
    assert exit_code == 0

    plan_items = [obj for obj in printed if "weather_run_plan" in obj]
    assert len(plan_items) == 1
    plan = plan_items[0]["weather_run_plan"]
    assert plan["candidate_event_count"] == 2
    assert plan["selected_event_count"] == 0
    assert plan["skipped_incremental_event_count"] == 1
    assert plan["skipped_failed_event_count"] == 1


def test_main_writes_failed_summary_and_returns_zero_below_threshold(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        incremental=True,
        historical_backfill=False,
        bucket=None,
        ddb_table=None,
        dry_run=False,
        force_events=False,
        include_failed_events=True,
        round_padding_days=0,
        timeout=30,
        progress_every=10,
        max_failure_rate=0.5,
        log_level="INFO",
    )
    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(s3_bucket="bucket", ddb_table="table", aws_region="us-east-1"),
    )
    monkeypatch.setattr(runner, "build_session", lambda cfg: "session")

    candidates = [_candidate(90008), _candidate(90009), _candidate(90010)]
    monkeypatch.setattr(runner, "load_weather_event_candidates", lambda **kwargs: candidates)
    monkeypatch.setattr(runner, "get_event_weather_summaries", lambda **kwargs: {})
    monkeypatch.setattr(
        runner,
        "_resolve_event_geopoint",
        lambda **kwargs: (_ for _ in ()).throw(ValueError("boom")) if kwargs["candidate"].event_id == 90008 else (runner.GeoPoint(30.0, -97.0), "metadata"),
    )
    monkeypatch.setattr(runner, "load_player_round_rows", lambda **kwargs: [])
    monkeypatch.setattr(
        runner,
        "_build_round_tasks",
        lambda **kwargs: ([], {}),
    )

    summary_calls = []
    monkeypatch.setattr(runner, "upsert_event_weather_summary", lambda **kwargs: summary_calls.append(kwargs))
    monkeypatch.setattr(runner, "put_weather_run_summary", lambda **kwargs: None)
    monkeypatch.setattr(runner, "print", lambda obj: None, raising=False)

    exit_code = runner.main()

    assert exit_code == 0
    failed_calls = [c for c in summary_calls if c["status"] == "failed"]
    assert len(failed_calls) == 1
    assert failed_calls[0]["event_id"] == 90008
    assert failed_calls[0]["error_type"] == "ValueError"


def test_main_returns_two_when_failure_rate_at_threshold(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        incremental=True,
        historical_backfill=False,
        bucket=None,
        ddb_table=None,
        dry_run=True,
        force_events=False,
        include_failed_events=True,
        round_padding_days=0,
        timeout=30,
        progress_every=10,
        max_failure_rate=0.5,
        log_level="INFO",
    )
    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(s3_bucket="bucket", ddb_table="table", aws_region="us-east-1"),
    )
    monkeypatch.setattr(runner, "build_session", lambda cfg: "session")

    candidates = [_candidate(90008), _candidate(90009)]
    monkeypatch.setattr(runner, "load_weather_event_candidates", lambda **kwargs: candidates)
    monkeypatch.setattr(runner, "get_event_weather_summaries", lambda **kwargs: {})
    monkeypatch.setattr(
        runner,
        "_resolve_event_geopoint",
        lambda **kwargs: (_ for _ in ()).throw(ValueError("boom")) if kwargs["candidate"].event_id == 90008 else (runner.GeoPoint(30.0, -97.0), "metadata"),
    )
    monkeypatch.setattr(runner, "load_player_round_rows", lambda **kwargs: [])
    monkeypatch.setattr(
        runner,
        "_build_round_tasks",
        lambda **kwargs: ([], {}),
    )
    monkeypatch.setattr(runner, "print", lambda obj: None, raising=False)

    exit_code = runner.main()
    assert exit_code == 2
