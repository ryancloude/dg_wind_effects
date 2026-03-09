from decimal import Decimal
from unittest.mock import Mock

import silver_pdga_live_results.candidate_reader as candidate_reader


def _make_item(
    *,
    event_id=90008,
    division="MA3",
    round_number=1,
    fetch_status="success",
    content_sha256="hash-1",
    last_fetched_at="2026-03-08T11:00:00Z",
    latest_s3_json_key="bronze/pdga/live_results/event_id=90008/division=MA3/round=1/fetch_date=2026-03-08/fetch_ts=2026-03-08T11:00:00Z.json",
):
    return {
        "event_id": Decimal(str(event_id)),
        "division": division,
        "round_number": Decimal(str(round_number)),
        "fetch_status": fetch_status,
        "content_sha256": content_sha256,
        "last_fetched_at": last_fetched_at,
        "latest_s3_json_key": latest_s3_json_key,
        "latest_s3_meta_key": latest_s3_json_key.replace(".json", ".meta.json"),
        "source_url": "https://www.pdga.com/apps/tournament/live-api/live_results_fetch_round?TournID=90008&Division=MA3&Round=1",
    }


def test_build_pointer_from_state_item_parses_valid_item():
    pointer = candidate_reader.build_pointer_from_state_item(_make_item())

    assert pointer is not None
    assert pointer.event_id == 90008
    assert pointer.division == "MA3"
    assert pointer.round_number == 1
    assert pointer.fetch_status == "success"


def test_collect_live_results_state_pointers_filters_by_cursor_and_status(monkeypatch):
    table = Mock()
    table.scan.side_effect = [
        {
            "Items": [
                _make_item(
                    last_fetched_at="2026-03-08T10:00:00Z",
                    latest_s3_json_key="bronze/.../fetch_ts=2026-03-08T10:00:00Z.json",
                ),
                _make_item(
                    last_fetched_at="2026-03-08T12:00:00Z",
                    latest_s3_json_key="bronze/.../fetch_ts=2026-03-08T12:00:00Z.json",
                ),
                _make_item(
                    fetch_status="not_found",
                    last_fetched_at="2026-03-08T13:00:00Z",
                    latest_s3_json_key="bronze/.../fetch_ts=2026-03-08T13:00:00Z.json",
                ),
            ],
            "LastEvaluatedKey": {"pk": "next"},
        },
        {
            "Items": [
                _make_item(
                    event_id=90009,
                    division="MPO",
                    round_number=2,
                    last_fetched_at="2026-03-08T14:00:00Z",
                    latest_s3_json_key="bronze/.../fetch_ts=2026-03-08T14:00:00Z.json",
                )
            ]
        },
    ]

    resource = Mock()
    resource.Table.return_value = table
    monkeypatch.setattr(candidate_reader.boto3, "resource", lambda *args, **kwargs: resource)

    out = candidate_reader.collect_live_results_state_pointers(
        table_name="pdga-event-index",
        cursor_fetch_ts="2026-03-08T11:00:00Z",
        cursor_s3_key="bronze/.../fetch_ts=2026-03-08T11:00:00Z.json",
        aws_region="us-east-1",
    )

    assert [(p.event_id, p.division, p.round_number) for p in out] == [
        (90008, "MA3", 1),
        (90009, "MPO", 2),
    ]


def test_latest_pointer_per_round_keeps_newest():
    pointers = [
        candidate_reader.LiveResultsStatePointer(
            event_id=90008,
            division="MA3",
            round_number=1,
            fetch_status="success",
            content_sha256="old-hash",
            last_fetched_at="2026-03-08T11:00:00Z",
            latest_s3_json_key="bronze/...11.json",
            latest_s3_meta_key="bronze/...11.meta.json",
            source_url="https://example.test",
        ),
        candidate_reader.LiveResultsStatePointer(
            event_id=90008,
            division="MA3",
            round_number=1,
            fetch_status="success",
            content_sha256="new-hash",
            last_fetched_at="2026-03-08T12:00:00Z",
            latest_s3_json_key="bronze/...12.json",
            latest_s3_meta_key="bronze/...12.meta.json",
            source_url="https://example.test",
        ),
        candidate_reader.LiveResultsStatePointer(
            event_id=90009,
            division="MPO",
            round_number=2,
            fetch_status="empty",
            content_sha256="hash-2",
            last_fetched_at="2026-03-08T10:00:00Z",
            latest_s3_json_key="bronze/...10.json",
            latest_s3_meta_key="bronze/...10.meta.json",
            source_url="https://example.test",
        ),
    ]

    out = candidate_reader.latest_pointer_per_round(pointers)

    assert len(out) == 2
    assert any(p.content_sha256 == "new-hash" for p in out)