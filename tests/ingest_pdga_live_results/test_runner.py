from types import SimpleNamespace

import pytest

import ingest_pdga_live_results.runner as runner
from ingest_pdga_live_results.dynamo_reader import LiveResultsTask
from ingest_pdga_live_results.response_handler import compute_payload_sha256


def test_parse_status_list_splits_and_trims_values():
    result = runner.parse_status_list("Sanctioned, Errata pending.")
    assert result == ["Sanctioned", "Errata pending."]


def test_resolve_historical_excluded_statuses_uses_defaults_when_not_provided():
    args = SimpleNamespace(historical_excluded_statuses=None)
    result = runner.resolve_historical_excluded_statuses(args)
    assert result == list(runner.DEFAULT_HISTORICAL_BACKFILL_EXCLUDED_STATUSES)


def test_resolve_historical_excluded_statuses_uses_override_when_provided():
    args = SimpleNamespace(historical_excluded_statuses="Sanctioned,In progress.")
    result = runner.resolve_historical_excluded_statuses(args)
    assert result == ["Sanctioned", "In progress."]


def test_parse_s3_uri_parses_bucket_and_key():
    bucket, key = runner.parse_s3_uri("s3://my-bucket/path/to/file.csv")
    assert bucket == "my-bucket"
    assert key == "path/to/file.csv"


def test_parse_s3_uri_raises_for_invalid_uri():
    with pytest.raises(ValueError):
        runner.parse_s3_uri("https://example.com/file.csv")


def test_resolve_event_ids_raises_if_both_sources_set():
    args = SimpleNamespace(event_ids="1,2", event_ids_s3_uri="s3://bucket/file.csv")
    with pytest.raises(ValueError, match="either --event-ids or --event-ids-s3-uri"):
        runner.resolve_event_ids(args, "us-east-1")


def test_resolve_event_ids_from_s3_uri(monkeypatch):
    args = SimpleNamespace(event_ids=None, event_ids_s3_uri="s3://bucket/file.csv")
    monkeypatch.setattr(runner, "load_event_ids_from_s3_uri", lambda **kwargs: [11, 22])
    assert runner.resolve_event_ids(args, "us-east-1") == [11, 22]


def test_process_task_unchanged_skips_s3_write(monkeypatch):
    task = LiveResultsTask(event_id="86076", division="MP40", round_number=1)
    payload = {"results": [{"player": "A"}]}
    payload_hash = compute_payload_sha256(payload)

    monkeypatch.setattr(
        runner,
        "get_live_results_json",
        lambda session, cfg, task: (200, payload, "https://example.test"),
    )
    monkeypatch.setattr(
        runner,
        "get_existing_live_results_sha256",
        lambda **kwargs: payload_hash,
    )

    called = {"s3": 0, "upsert": 0}

    def fake_put_live_results_raw(**kwargs):
        called["s3"] += 1
        return {"s3_json_key": "k1", "s3_meta_key": "k2", "fetched_at": "2026-03-06T00:00:00Z"}

    def fake_upsert(**kwargs):
        called["upsert"] += 1
        return {}

    monkeypatch.setattr(runner, "put_live_results_raw", fake_put_live_results_raw)
    monkeypatch.setattr(runner, "upsert_live_results_state", fake_upsert)

    result = runner.process_task(
        task=task,
        bucket="bucket",
        dry_run=False,
        app_cfg=SimpleNamespace(ddb_table="tbl", aws_region="us-east-1"),
        session=object(),
        http_cfg=SimpleNamespace(timeout_s=30),
        run_id="run-1",
    )

    assert result.classification == "success"
    assert result.unchanged is True
    assert result.changed is False
    assert called["s3"] == 0
    assert called["upsert"] == 1


def test_process_task_changed_writes_s3(monkeypatch):
    task = LiveResultsTask(event_id="86076", division="MP40", round_number=1)
    payload = {"results": [{"player": "A"}]}

    monkeypatch.setattr(
        runner,
        "get_live_results_json",
        lambda session, cfg, task: (200, payload, "https://example.test"),
    )
    monkeypatch.setattr(
        runner,
        "get_existing_live_results_sha256",
        lambda **kwargs: None,
    )

    called = {"s3": 0, "upsert": 0}

    def fake_put_live_results_raw(**kwargs):
        called["s3"] += 1
        return {"s3_json_key": "k1", "s3_meta_key": "k2", "fetched_at": "2026-03-06T00:00:00Z"}

    def fake_upsert(**kwargs):
        called["upsert"] += 1
        return {}

    monkeypatch.setattr(runner, "put_live_results_raw", fake_put_live_results_raw)
    monkeypatch.setattr(runner, "upsert_live_results_state", fake_upsert)

    result = runner.process_task(
        task=task,
        bucket="bucket",
        dry_run=False,
        app_cfg=SimpleNamespace(ddb_table="tbl", aws_region="us-east-1"),
        session=object(),
        http_cfg=SimpleNamespace(timeout_s=30),
        run_id="run-1",
    )

    assert result.classification == "success"
    assert result.changed is True
    assert result.unchanged is False
    assert called["s3"] == 1
    assert called["upsert"] == 1


def test_main_historical_uses_expected_flags_and_marks_ingested(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        event_ids_s3_uri=None,
        historical_backfill=True,
        historical_excluded_statuses=None,
        bucket=None,
        dry_run=False,
        timeout=30,
        sleep_base=0.0,
        sleep_jitter=0.0,
        progress_every=10,
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(
            s3_bucket="bucket",
            ddb_table="table",
            aws_region="us-east-1",
            ddb_status_end_date_gsi="gsi_status_end_date",
        ),
    )
    monkeypatch.setattr(runner, "build_session", lambda cfg: "session")
    monkeypatch.setattr(runner, "resolve_event_ids", lambda _args, _region: None)

    captured_load = {}

    def fake_load_live_results_tasks(**kwargs):
        captured_load.update(kwargs)
        return [
            LiveResultsTask(event_id="1001", division="MPO", round_number=1),
            LiveResultsTask(event_id="1001", division="MPO", round_number=2),
            LiveResultsTask(event_id="1002", division="FPO", round_number=1),
        ]

    monkeypatch.setattr(runner, "load_live_results_tasks", fake_load_live_results_tasks)
    monkeypatch.setattr(
        runner,
        "run_task_sequence",
        lambda **kwargs: runner.RunStats(attempted=3, success=3, changed=1, unchanged=2, failed=0),
    )

    summary_calls = []
    mark_calls = []
    printed = []

    monkeypatch.setattr(runner, "put_live_results_run_summary", lambda **kwargs: summary_calls.append(kwargs))
    monkeypatch.setattr(runner, "mark_event_live_results_ingested", lambda **kwargs: mark_calls.append(kwargs))
    monkeypatch.setattr(runner, "print", lambda obj: printed.append(obj), raising=False)

    exit_code = runner.main()

    assert exit_code == 0
    assert captured_load["use_status_end_date_gsi"] is True
    assert captured_load["status_end_date_gsi_name"] == "gsi_status_end_date"
    assert captured_load["exclude_already_live_results_ingested"] is True
    assert len(summary_calls) == 1
    assert [call["event_id"] for call in mark_calls] == [1001, 1002]
    assert any("live_results_run_plan" in item for item in printed)
    assert any("summary" in item for item in printed)


def test_main_dry_run_does_not_write_summary_or_mark_ingested(monkeypatch):
    args = SimpleNamespace(
        event_ids="1001",
        event_ids_s3_uri=None,
        historical_backfill=False,
        historical_excluded_statuses=None,
        bucket=None,
        dry_run=True,
        timeout=30,
        sleep_base=0.0,
        sleep_jitter=0.0,
        progress_every=10,
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(
            s3_bucket="bucket",
            ddb_table="table",
            aws_region="us-east-1",
            ddb_status_end_date_gsi="gsi_status_end_date",
        ),
    )
    monkeypatch.setattr(runner, "build_session", lambda cfg: "session")
    monkeypatch.setattr(runner, "resolve_event_ids", lambda _args, _region: [1001])
    monkeypatch.setattr(
        runner,
        "load_live_results_tasks",
        lambda **kwargs: [LiveResultsTask(event_id="1001", division="MPO", round_number=1)],
    )
    monkeypatch.setattr(
        runner,
        "run_task_sequence",
        lambda **kwargs: runner.RunStats(attempted=1, success=1, changed=0, unchanged=1, failed=0),
    )

    summary_calls = []
    mark_calls = []

    monkeypatch.setattr(runner, "put_live_results_run_summary", lambda **kwargs: summary_calls.append(kwargs))
    monkeypatch.setattr(runner, "mark_event_live_results_ingested", lambda **kwargs: mark_calls.append(kwargs))

    exit_code = runner.main()

    assert exit_code == 0
    assert summary_calls == []
    assert mark_calls == []