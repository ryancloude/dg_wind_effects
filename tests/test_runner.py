from types import SimpleNamespace
from unittest.mock import Mock

import ingest_pdga_event_pages.runner as runner


def test_iter_explicit_event_ids_from_ids():
    args = SimpleNamespace(ids="100,101, 102", range=None, backfill_start_id=None)
    assert list(runner.iter_explicit_event_ids(args)) == [100, 101, 102]


def test_iter_explicit_event_ids_from_range():
    args = SimpleNamespace(ids=None, range="100-103", backfill_start_id=None)
    assert list(runner.iter_explicit_event_ids(args)) == [100, 101, 102, 103]


def test_parse_status_list_splits_and_trims_values():
    result = runner.parse_status_list("Sanctioned, Event report received; official ratings pending., Errata pending.")
    assert result == [
        "Sanctioned",
        "Event report received; official ratings pending.",
        "Errata pending.",
    ]


def test_parse_status_list_raises_when_empty():
    try:
        runner.parse_status_list(" , , ")
    except ValueError as exc:
        assert str(exc) == "--incremental-statuses requires at least one status_text value"
    else:
        raise AssertionError("Expected ValueError for empty incremental status list")


def test_update_unscheduled_streak_increments_for_placeholder():
    assert runner.update_unscheduled_streak(2, True) == 3


def test_update_unscheduled_streak_resets_for_real_event():
    assert runner.update_unscheduled_streak(2, False) == 0


def test_should_stop_backfill_when_threshold_reached():
    assert runner.should_stop_backfill(5, 5) is True


def test_should_not_stop_backfill_below_threshold():
    assert runner.should_stop_backfill(4, 5) is False


def test_process_event_dry_run_skips_aws_calls(monkeypatch):
    app_cfg = SimpleNamespace(ddb_table="tbl", aws_region="us-east-1")
    session = object()
    http_cfg = object()

    monkeypatch.setattr(
        runner,
        "get_event_page_html",
        lambda session, http_cfg, event_id: (200, "<html><h1>Event</h1></html>"),
    )
    monkeypatch.setattr(
        runner,
        "parse_event_page",
        lambda event_id, html, source_url=None: {
            "event_id": event_id,
            "name": "Event",
            "division_rounds": {},
            "status_text": "",
            "content_sha256": "abc",
            "parser_version": "event-page-v2",
            "idempotency_sha256": "idem",
            "is_unscheduled_placeholder": False,
        },
    )

    get_existing = Mock()
    put_raw = Mock()
    upsert = Mock()

    monkeypatch.setattr(runner, "get_existing_content_sha256", get_existing)
    monkeypatch.setattr(runner, "put_event_page_raw", put_raw)
    monkeypatch.setattr(runner, "upsert_event_metadata", upsert)

    result = runner.process_event(
        event_id=123,
        bucket="bucket",
        dry_run=True,
        app_cfg=app_cfg,
        session=session,
        http_cfg=http_cfg,
    )

    assert result.event_id == 123
    assert result.unchanged is False
    get_existing.assert_not_called()
    put_raw.assert_not_called()
    upsert.assert_not_called()


def test_process_event_skips_write_when_unchanged(monkeypatch):
    app_cfg = SimpleNamespace(ddb_table="tbl", aws_region="us-east-1")
    session = object()
    http_cfg = object()

    monkeypatch.setattr(
        runner,
        "get_event_page_html",
        lambda session, http_cfg, event_id: (200, "<html><h1>Event</h1></html>"),
    )
    monkeypatch.setattr(
        runner,
        "parse_event_page",
        lambda event_id, html, source_url=None: {
            "event_id": event_id,
            "name": "Event",
            "division_rounds": {},
            "status_text": "",
            "content_sha256": "abc",
            "parser_version": "event-page-v2",
            "idempotency_sha256": "same-hash",
            "is_unscheduled_placeholder": False,
        },
    )
    monkeypatch.setattr(runner, "get_existing_content_sha256", lambda **kwargs: "same-hash")

    put_raw = Mock()
    upsert = Mock()
    monkeypatch.setattr(runner, "put_event_page_raw", put_raw)
    monkeypatch.setattr(runner, "upsert_event_metadata", upsert)

    result = runner.process_event(
        event_id=123,
        bucket="bucket",
        dry_run=False,
        app_cfg=app_cfg,
        session=session,
        http_cfg=http_cfg,
    )

    assert result.unchanged is True
    put_raw.assert_not_called()
    upsert.assert_not_called()


def test_process_event_writes_when_changed(monkeypatch):
    app_cfg = SimpleNamespace(ddb_table="tbl", aws_region="us-east-1")
    session = object()
    http_cfg = object()

    monkeypatch.setattr(
        runner,
        "get_event_page_html",
        lambda session, http_cfg, event_id: (200, "<html><h1>Event</h1></html>"),
    )
    monkeypatch.setattr(
        runner,
        "parse_event_page",
        lambda event_id, html, source_url=None: {
            "event_id": event_id,
            "name": "Event",
            "division_rounds": {},
            "status_text": "",
            "content_sha256": "abc",
            "parser_version": "event-page-v2",
            "idempotency_sha256": "new-hash",
            "is_unscheduled_placeholder": False,
        },
    )
    monkeypatch.setattr(runner, "get_existing_content_sha256", lambda **kwargs: "old-hash")
    monkeypatch.setattr(
        runner,
        "put_event_page_raw",
        lambda **kwargs: {
            "s3_html_key": "bronze/test.html.gz",
            "s3_meta_key": "bronze/test.meta.json",
            "fetched_at": "2026-03-03T12:00:00Z",
        },
    )

    upsert = Mock(return_value={"pk": "EVENT#123", "sk": "METADATA"})
    monkeypatch.setattr(runner, "upsert_event_metadata", upsert)

    result = runner.process_event(
        event_id=123,
        bucket="bucket",
        dry_run=False,
        app_cfg=app_cfg,
        session=session,
        http_cfg=http_cfg,
    )

    assert result.unchanged is False
    assert result.s3_ptrs["s3_html_key"] == "bronze/test.html.gz"
    upsert.assert_called_once()


def test_run_forward_scan_stops_after_placeholder_streak(monkeypatch):
    app_cfg = SimpleNamespace(ddb_table="tbl", aws_region="us-east-1")
    session = object()
    http_cfg = object()

    calls = []

    def fake_process_event(**kwargs):
        event_id = kwargs["event_id"]
        calls.append(event_id)
        is_placeholder = event_id in {201, 202, 203}
        return runner.ProcessResult(
            event_id=event_id,
            parsed={
                "name": f"Event {event_id}",
                "status_text": "",
                "division_rounds": {},
                "is_unscheduled_placeholder": is_placeholder,
            },
            http_status=200,
            s3_ptrs={},
            ddb_attrs={},
            unchanged=False,
        )

    monkeypatch.setattr(runner, "process_event", fake_process_event)
    monkeypatch.setattr(runner, "log_event_result", lambda result: None)
    monkeypatch.setattr(runner, "polite_sleep", lambda cfg: None)

    ok, failed = runner.run_forward_scan(
        start_event_id=200,
        stop_after_unscheduled=3,
        max_event_id=None,
        bucket="bucket",
        dry_run=True,
        app_cfg=app_cfg,
        session=session,
        http_cfg=http_cfg,
    )

    assert ok == 4
    assert failed == 0
    assert calls == [200, 201, 202, 203]


def test_run_forward_scan_resets_streak_on_scheduled_event(monkeypatch):
    app_cfg = SimpleNamespace(ddb_table="tbl", aws_region="us-east-1")
    session = object()
    http_cfg = object()

    placeholder_map = {
        300: True,
        301: True,
        302: False,
        303: True,
        304: True,
        305: True,
    }
    calls = []

    def fake_process_event(**kwargs):
        event_id = kwargs["event_id"]
        calls.append(event_id)
        return runner.ProcessResult(
            event_id=event_id,
            parsed={
                "name": f"Event {event_id}",
                "status_text": "",
                "division_rounds": {},
                "is_unscheduled_placeholder": placeholder_map[event_id],
            },
            http_status=200,
            s3_ptrs={},
            ddb_attrs={},
            unchanged=False,
        )

    monkeypatch.setattr(runner, "process_event", fake_process_event)
    monkeypatch.setattr(runner, "log_event_result", lambda result: None)
    monkeypatch.setattr(runner, "polite_sleep", lambda cfg: None)

    ok, failed = runner.run_forward_scan(
        start_event_id=300,
        stop_after_unscheduled=3,
        max_event_id=305,
        bucket="bucket",
        dry_run=True,
        app_cfg=app_cfg,
        session=session,
        http_cfg=http_cfg,
    )

    assert ok == 6
    assert failed == 0
    assert calls == [300, 301, 302, 303, 304, 305]


def test_run_forward_scan_resets_streak_on_failure(monkeypatch):
    app_cfg = SimpleNamespace(ddb_table="tbl", aws_region="us-east-1")
    session = object()
    http_cfg = object()

    calls = []

    def fake_process_event(**kwargs):
        event_id = kwargs["event_id"]
        calls.append(event_id)
        if event_id == 401:
            raise RuntimeError("boom")
        is_placeholder = event_id in {400, 402, 403, 404}
        return runner.ProcessResult(
            event_id=event_id,
            parsed={
                "name": f"Event {event_id}",
                "status_text": "",
                "division_rounds": {},
                "is_unscheduled_placeholder": is_placeholder,
            },
            http_status=200,
            s3_ptrs={},
            ddb_attrs={},
            unchanged=False,
        )

    monkeypatch.setattr(runner, "process_event", fake_process_event)
    monkeypatch.setattr(runner, "log_event_result", lambda result: None)
    monkeypatch.setattr(runner, "polite_sleep", lambda cfg: None)

    ok, failed = runner.run_forward_scan(
        start_event_id=400,
        stop_after_unscheduled=3,
        max_event_id=404,
        bucket="bucket",
        dry_run=True,
        app_cfg=app_cfg,
        session=session,
        http_cfg=http_cfg,
    )

    assert ok == 4
    assert failed == 1
    assert calls == [400, 401, 402, 403, 404]


def test_main_incremental_mode_rescrapes_candidates_then_scans_forward(monkeypatch):
    args = SimpleNamespace(
        ids=None,
        range=None,
        backfill_start_id=None,
        incremental_statuses="Sanctioned,Errata pending.",
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
    monkeypatch.setattr(runner, "build_session", lambda http_cfg: "session")

    rescrape_ids_seen = []
    forward_scan_calls = []

    def fake_iter_rescrape_event_ids(**kwargs):
        assert kwargs["status_texts"] == ["Sanctioned", "Errata pending."]
        assert kwargs["table_name"] == "table"
        rescrape_ids_seen.append(True)
        return iter([101, 102])

    def fake_run_event_sequence(**kwargs):
        assert list(kwargs["event_ids"]) == [101, 102]
        return (2, 0)

    def fake_get_max_event_id(**kwargs):
        return 5000

    def fake_run_forward_scan(**kwargs):
        forward_scan_calls.append(kwargs)
        assert kwargs["start_event_id"] == 5001
        assert kwargs["stop_after_unscheduled"] == 5
        return (3, 0)

    monkeypatch.setattr(runner, "iter_rescrape_event_ids", fake_iter_rescrape_event_ids)
    monkeypatch.setattr(runner, "run_event_sequence", fake_run_event_sequence)
    monkeypatch.setattr(runner, "get_max_event_id", fake_get_max_event_id)
    monkeypatch.setattr(runner, "run_forward_scan", fake_run_forward_scan)

    exit_code = runner.main()

    assert exit_code == 0
    assert rescrape_ids_seen == [True]
    assert len(forward_scan_calls) == 1