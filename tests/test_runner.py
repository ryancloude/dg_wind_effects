from types import SimpleNamespace
from unittest.mock import Mock

import requests

import ingest_pdga_event_pages.runner as runner


def make_http_error(status_code: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    response.url = "https://www.pdga.com/tour/event/999999"
    return requests.HTTPError(f"{status_code} error", response=response)


def test_parse_status_list_splits_and_trims_values():
    result = runner.parse_status_list("Sanctioned, Errata pending.")
    assert result == ["Sanctioned", "Errata pending."]


def test_resolve_incremental_statuses_uses_defaults_when_not_provided():
    args = SimpleNamespace(incremental_statuses=None)
    result = runner.resolve_incremental_statuses(args)
    assert result == list(runner.DEFAULT_INCREMENTAL_STATUSES)


def test_resolve_incremental_statuses_uses_override_when_provided():
    args = SimpleNamespace(incremental_statuses="Sanctioned,In progress.")
    result = runner.resolve_incremental_statuses(args)
    assert result == ["Sanctioned", "In progress."]


def test_update_no_event_streak_with_placeholder():
    assert runner.update_no_event_streak(2, is_unscheduled_placeholder=True) == 3


def test_update_no_event_streak_with_404():
    assert runner.update_no_event_streak(2, is_not_found_404=True) == 3


def test_update_no_event_streak_resets_on_real_event():
    assert runner.update_no_event_streak(2) == 0


def test_run_forward_scan_counts_404_toward_stop(monkeypatch):
    app_cfg = SimpleNamespace(ddb_table="tbl", aws_region="us-east-1")
    session = object()
    http_cfg = object()

    calls = {"n": 0}

    def fake_process_event(**kwargs):
        calls["n"] += 1
        raise make_http_error(404)

    monkeypatch.setattr(runner, "process_event", fake_process_event)
    monkeypatch.setattr(runner, "polite_sleep", lambda cfg: None)

    ok, failed = runner.run_forward_scan(
        start_event_id=1000,
        stop_after_unscheduled=3,
        max_event_id=None,
        bucket="bucket",
        dry_run=True,
        app_cfg=app_cfg,
        session=session,
        http_cfg=http_cfg,
    )

    assert ok == 3
    assert failed == 0
    assert calls["n"] == 3


def test_run_forward_scan_resets_streak_on_non_404_error(monkeypatch):
    app_cfg = SimpleNamespace(ddb_table="tbl", aws_region="us-east-1")
    session = object()
    http_cfg = object()

    sequence = [
        make_http_error(404),
        make_http_error(500),
        make_http_error(404),
        make_http_error(404),
        make_http_error(404),
    ]
    idx = {"i": 0}

    def fake_process_event(**kwargs):
        err = sequence[idx["i"]]
        idx["i"] += 1
        raise err

    monkeypatch.setattr(runner, "process_event", fake_process_event)
    monkeypatch.setattr(runner, "polite_sleep", lambda cfg: None)

    ok, failed = runner.run_forward_scan(
        start_event_id=2000,
        stop_after_unscheduled=3,
        max_event_id=2004,
        bucket="bucket",
        dry_run=True,
        app_cfg=app_cfg,
        session=session,
        http_cfg=http_cfg,
    )

    assert ok == 4
    assert failed == 1


def test_main_incremental_uses_default_statuses(monkeypatch):
    args = SimpleNamespace(
        ids=None,
        range=None,
        backfill_start_id=None,
        incremental=True,
        incremental_statuses=None,
        bucket=None,
        dry_run=True,
        timeout=30,
        sleep_base=0.0,
        sleep_jitter=0.0,
        log_level="INFO",
        backfill_stop_after_unscheduled=5,
        backfill_max_event_id=None,
        incremental_window_days=183,
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(s3_bucket="bucket", ddb_table="table", aws_region="us-east-1"),
    )
    monkeypatch.setattr(runner, "build_session", lambda cfg: "session")

    captured = {}

    def fake_iter_rescrape_event_ids(**kwargs):
        captured["statuses"] = kwargs["status_texts"]
        return iter([101, 102])

    monkeypatch.setattr(runner, "iter_rescrape_event_ids", fake_iter_rescrape_event_ids)
    monkeypatch.setattr(runner, "run_event_sequence", lambda **kwargs: (2, 0))
    monkeypatch.setattr(runner, "get_max_event_id", lambda **kwargs: 5000)
    monkeypatch.setattr(runner, "run_forward_scan", lambda **kwargs: (3, 0))

    exit_code = runner.main()

    assert exit_code == 0
    assert captured["statuses"] == list(runner.DEFAULT_INCREMENTAL_STATUSES)