from types import SimpleNamespace

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


def test_should_exit_nonzero_when_failure_rate_at_threshold():
    stats = runner.RunStats(scraped=10, failed=10)
    assert runner.should_exit_nonzero(stats=stats, max_failure_rate=0.5) is True


def test_should_not_exit_nonzero_when_failure_rate_below_threshold():
    stats = runner.RunStats(scraped=31, failed=20)
    assert runner.should_exit_nonzero(stats=stats, max_failure_rate=0.5) is False


def test_run_event_sequence_emits_progress_every_n(monkeypatch):
    app_cfg = SimpleNamespace(ddb_table="tbl", aws_region="us-east-1")
    session = object()
    http_cfg = object()
    printed = []

    def fake_process_event(**kwargs):
        event_id = kwargs["event_id"]
        return runner.ProcessResult(
            event_id=event_id,
            parsed={"division_rounds": {}, "is_unscheduled_placeholder": False, "status_text": "Sanctioned"},
            http_status=200,
            s3_ptrs={},
            ddb_attrs={},
            unchanged=True,
            change_type="unchanged",
        )

    monkeypatch.setattr(runner, "process_event", fake_process_event)
    monkeypatch.setattr(runner, "polite_sleep", lambda cfg: None)
    monkeypatch.setattr(runner, "print", lambda obj: printed.append(obj), raising=False)

    stats = runner.run_event_sequence(
        event_ids=[101, 102, 103],
        bucket="bucket",
        dry_run=True,
        app_cfg=app_cfg,
        session=session,
        http_cfg=http_cfg,
        run_id="run-1",
        progress_every=2,
    )

    assert stats.scraped == 3
    progress_items = [x for x in printed if "incremental_progress" in x]
    assert len(progress_items) == 2
    assert progress_items[0]["incremental_progress"]["processed_events"] == 2
    assert progress_items[1]["incremental_progress"]["processed_events"] == 3


def test_run_event_sequence_records_failure_metadata(monkeypatch):
    app_cfg = SimpleNamespace(ddb_table="tbl", aws_region="us-east-1")
    session = object()
    http_cfg = object()
    failure_calls = []

    monkeypatch.setattr(runner, "process_event", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(runner, "record_event_fetch_failure", lambda **kwargs: failure_calls.append(kwargs))
    monkeypatch.setattr(runner, "polite_sleep", lambda cfg: None)
    monkeypatch.setattr(runner, "print", lambda obj: None, raising=False)

    stats = runner.run_event_sequence(
        event_ids=[101],
        bucket="bucket",
        dry_run=False,
        app_cfg=app_cfg,
        session=session,
        http_cfg=http_cfg,
        run_id="run-1",
        progress_every=50,
    )

    assert stats.failed == 1
    assert len(failure_calls) == 1
    assert failure_calls[0]["event_id"] == 101


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

    stats = runner.run_forward_scan(
        start_event_id=1000,
        stop_after_unscheduled=3,
        max_event_id=None,
        bucket="bucket",
        dry_run=True,
        app_cfg=app_cfg,
        session=session,
        http_cfg=http_cfg,
        run_id="run-1",
        progress_every=50,
    )

    assert stats.scraped == 0
    assert stats.not_found_404 == 3
    assert stats.failed == 0
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

    failure_calls = []

    monkeypatch.setattr(runner, "process_event", fake_process_event)
    monkeypatch.setattr(runner, "record_event_fetch_failure", lambda **kwargs: failure_calls.append(kwargs))
    monkeypatch.setattr(runner, "polite_sleep", lambda cfg: None)

    stats = runner.run_forward_scan(
        start_event_id=2000,
        stop_after_unscheduled=3,
        max_event_id=2004,
        bucket="bucket",
        dry_run=False,
        app_cfg=app_cfg,
        session=session,
        http_cfg=http_cfg,
        run_id="run-1",
        progress_every=50,
    )

    assert stats.scraped == 0
    assert stats.not_found_404 == 4
    assert stats.failed == 1
    assert len(failure_calls) == 1
    assert failure_calls[0]["event_id"] == 2001


def test_main_incremental_uses_default_statuses_and_emits_candidate_count(monkeypatch):
    args = SimpleNamespace(
        ids=None,
        range=None,
        backfill_start_id=None,
        incremental=True,
        incremental_statuses=None,
        incremental_window_days=183,
        incremental_refetch_hours=48,
        failed_refetch_cooldown_hours=72,
        max_failure_rate=0.5,
        progress_every=50,
        bucket=None,
        dry_run=True,
        timeout=30,
        sleep_base=0.0,
        sleep_jitter=0.0,
        log_level="INFO",
        backfill_stop_after_unscheduled=5,
        backfill_max_event_id=None,
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

    captured = {}
    printed = []

    def fake_iter_rescrape_event_ids_via_gsi(**kwargs):
        captured["statuses"] = kwargs["status_texts"]
        captured["gsi_name"] = kwargs["gsi_name"]
        captured["start_date"] = kwargs["start_date"]
        captured["end_before_date"] = kwargs["end_before_date"]
        captured["older_than_ts"] = kwargs["older_than_ts"]
        captured["failed_older_than_ts"] = kwargs["failed_older_than_ts"]
        return iter([101, 102])

    def fake_print(obj):
        printed.append(obj)

    monkeypatch.setattr(
        runner,
        "iter_rescrape_event_ids_via_gsi",
        fake_iter_rescrape_event_ids_via_gsi,
    )
    monkeypatch.setattr(
        runner,
        "run_event_sequence",
        lambda **kwargs: runner.RunStats(scraped=2, updated_scraped=1, unchanged_scraped=1),
    )
    monkeypatch.setattr(runner, "get_max_event_id", lambda **kwargs: 5000)
    monkeypatch.setattr(
        runner,
        "run_forward_scan",
        lambda **kwargs: runner.RunStats(scraped=3, new_scraped=2, unchanged_scraped=1, not_found_404=4),
    )
    monkeypatch.setattr(runner, "print", fake_print, raising=False)

    exit_code = runner.main()

    assert exit_code == 0
    assert captured["statuses"] == list(runner.DEFAULT_INCREMENTAL_STATUSES)
    assert captured["gsi_name"] == "gsi_status_end_date"
    assert captured["older_than_ts"]
    assert captured["failed_older_than_ts"]

    candidate_items = [x for x in printed if "incremental_rescrape_candidate_count" in x]
    assert len(candidate_items) == 1
    candidate_payload = candidate_items[0]
    assert candidate_payload["incremental_rescrape_candidate_count"] == 2
    assert candidate_payload["incremental_window_days"] == 183
    assert candidate_payload["incremental_refetch_hours"] == 48
    assert candidate_payload["failed_refetch_cooldown_hours"] == 72
    assert candidate_payload["incremental_statuses"] == list(runner.DEFAULT_INCREMENTAL_STATUSES)

    summary_items = [x for x in printed if "incremental_summary" in x]
    assert len(summary_items) == 1
    summary = summary_items[0]["incremental_summary"]

    assert summary["updated_scraped"] == 1
    assert summary["new_scraped"] == 2
    assert summary["unchanged_scraped"] == 2
    assert summary["scraped_total"] == 5
    assert summary["not_found_404"] == 4
    assert summary["failed"] == 0
    assert summary["attempted_total"] == 9
    assert summary["failure_rate"] == 0.0
    assert summary["max_failure_rate"] == 0.5


def test_main_returns_zero_when_failure_rate_below_threshold(monkeypatch):
    args = SimpleNamespace(
        ids=None,
        range=None,
        backfill_start_id=None,
        incremental=True,
        incremental_statuses=None,
        incremental_window_days=183,
        incremental_refetch_hours=48,
        failed_refetch_cooldown_hours=72,
        max_failure_rate=0.5,
        progress_every=50,
        bucket=None,
        dry_run=True,
        timeout=30,
        sleep_base=0.0,
        sleep_jitter=0.0,
        log_level="INFO",
        backfill_stop_after_unscheduled=5,
        backfill_max_event_id=None,
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
    monkeypatch.setattr(runner, "iter_rescrape_event_ids_via_gsi", lambda **kwargs: iter([101]))
    monkeypatch.setattr(
        runner,
        "run_event_sequence",
        lambda **kwargs: runner.RunStats(scraped=31, failed=20, unchanged_scraped=31),
    )
    monkeypatch.setattr(runner, "get_max_event_id", lambda **kwargs: 5000)
    monkeypatch.setattr(runner, "run_forward_scan", lambda **kwargs: runner.RunStats())
    monkeypatch.setattr(runner, "print", lambda obj: None, raising=False)

    assert runner.main() == 0


def test_main_returns_two_when_failure_rate_at_or_above_threshold(monkeypatch):
    args = SimpleNamespace(
        ids=None,
        range=None,
        backfill_start_id=None,
        incremental=True,
        incremental_statuses=None,
        incremental_window_days=183,
        incremental_refetch_hours=48,
        failed_refetch_cooldown_hours=72,
        max_failure_rate=0.5,
        progress_every=50,
        bucket=None,
        dry_run=True,
        timeout=30,
        sleep_base=0.0,
        sleep_jitter=0.0,
        log_level="INFO",
        backfill_stop_after_unscheduled=5,
        backfill_max_event_id=None,
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
    monkeypatch.setattr(runner, "iter_rescrape_event_ids_via_gsi", lambda **kwargs: iter([101]))
    monkeypatch.setattr(
        runner,
        "run_event_sequence",
        lambda **kwargs: runner.RunStats(scraped=20, failed=20, unchanged_scraped=20),
    )
    monkeypatch.setattr(runner, "get_max_event_id", lambda **kwargs: 5000)
    monkeypatch.setattr(runner, "run_forward_scan", lambda **kwargs: runner.RunStats())
    monkeypatch.setattr(runner, "print", lambda obj: None, raising=False)

    assert runner.main() == 2
