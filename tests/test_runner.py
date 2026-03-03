from types import SimpleNamespace
from unittest.mock import Mock

import ingest_pdga_event_pages.runner as runner


def test_iter_explicit_event_ids_from_ids():
    args = SimpleNamespace(ids="100,101, 102", range=None, backfill_start_id=None)
    assert list(runner.iter_explicit_event_ids(args)) == [100, 101, 102]


def test_iter_explicit_event_ids_from_range():
    args = SimpleNamespace(ids=None, range="100-103", backfill_start_id=None)
    assert list(runner.iter_explicit_event_ids(args)) == [100, 101, 102, 103]


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