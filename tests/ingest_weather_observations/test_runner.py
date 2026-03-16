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


def test_main_incremental_default_skips_unchanged_event(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        incremental=False,
        historical_backfill=False,
        bucket=None,
        ddb_table=None,
        dry_run=True,
        force_events=False,
        round_padding_days=0,
        timeout=30,
        progress_every=10,
        log_level="INFO",
    )
    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(s3_bucket="bucket", ddb_table="table", aws_region="us-east-1"),
    )
    monkeypatch.setattr(runner, "build_session", lambda cfg: "session")

    candidates = [_candidate(90008)]
    monkeypatch.setattr(runner, "load_weather_event_candidates", lambda **kwargs: candidates)
    monkeypatch.setattr(
        runner,
        "get_event_weather_summary",
        lambda **kwargs: {"last_silver_checkpoint_updated_at": "2026-03-16T00:00:00Z"},
    )

    printed = []
    monkeypatch.setattr(runner, "print", lambda obj: printed.append(obj), raising=False)

    exit_code = runner.main()
    assert exit_code == 0
    assert any("weather_summary" in obj for obj in printed)