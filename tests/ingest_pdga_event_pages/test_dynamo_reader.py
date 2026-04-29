from unittest.mock import Mock

import ingest_pdga_event_pages.dynamo_reader as dynamo_reader


def test_get_existing_content_sha256_returns_none_when_item_missing(monkeypatch):
    table = Mock()
    table.get_item.return_value = {}

    resource = Mock()
    resource.Table.return_value = table

    monkeypatch.setattr(dynamo_reader.boto3, "resource", lambda *args, **kwargs: resource)

    result = dynamo_reader.get_existing_content_sha256(
        table_name="pdga-table",
        event_id=123,
        aws_region="us-east-1",
    )

    assert result is None
    table.get_item.assert_called_once_with(
        Key={"pk": "EVENT#123", "sk": "METADATA"},
        ConsistentRead=False,
    )


def test_get_existing_content_sha256_returns_hash(monkeypatch):
    table = Mock()
    table.get_item.return_value = {"Item": {"idempotency_sha256": "abc123"}}

    resource = Mock()
    resource.Table.return_value = table

    monkeypatch.setattr(dynamo_reader.boto3, "resource", lambda *args, **kwargs: resource)

    result = dynamo_reader.get_existing_content_sha256(
        table_name="pdga-table",
        event_id=123,
        aws_region="us-east-1",
    )

    assert result == "abc123"


def test_get_max_event_id_returns_largest_metadata_event_id(monkeypatch):
    table = Mock()
    table.scan.side_effect = [
        {
            "Items": [
                {"event_id": 1001, "sk": "METADATA"},
                {"event_id": 1015, "sk": "METADATA"},
                {"event_id": 9999, "sk": "OTHER"},
            ],
            "LastEvaluatedKey": {"pk": "next"},
        },
        {
            "Items": [
                {"event_id": 1022, "sk": "METADATA"},
                {"event_id": 1008, "sk": "METADATA"},
            ]
        },
    ]

    resource = Mock()
    resource.Table.return_value = table

    monkeypatch.setattr(dynamo_reader.boto3, "resource", lambda *args, **kwargs: resource)

    result = dynamo_reader.get_max_event_id(
        table_name="pdga-table",
        aws_region="us-east-1",
    )

    assert result == 1022


def test_iter_rescrape_event_ids_via_gsi_filters_and_returns_metadata_event_ids(monkeypatch):
    table = Mock()
    table.query.side_effect = [
        {
            "Items": [
                {
                    "event_id": 1001,
                    "sk": "METADATA",
                    "status_text": "Sanctioned",
                    "end_date": "2026-02-01",
                },
                {
                    "event_id": 1005,
                    "sk": "OTHER",
                    "status_text": "Sanctioned",
                    "end_date": "2026-02-01",
                },
            ],
            "LastEvaluatedKey": {"pk": "next"},
        },
        {
            "Items": [
                {
                    "event_id": 1002,
                    "sk": "METADATA",
                    "status_text": "Sanctioned",
                    "end_date": "2026-02-02",
                },
            ]
        },
        {
            "Items": [
                {
                    "event_id": 1003,
                    "sk": "METADATA",
                    "status_text": "Errata pending.",
                    "end_date": "2026-01-15",
                },
            ]
        },
    ]

    resource = Mock()
    resource.Table.return_value = table

    monkeypatch.setattr(dynamo_reader.boto3, "resource", lambda *args, **kwargs: resource)

    result = list(
        dynamo_reader.iter_rescrape_event_ids_via_gsi(
            table_name="pdga-table",
            gsi_name="gsi_status_end_date",
            status_texts=["Sanctioned", "Errata pending."],
            start_date="2025-09-04",
            end_before_date="2026-03-04",
            aws_region="us-east-1",
        )
    )

    assert result == [1001, 1002, 1003]
    assert table.query.call_count == 3


def test_iter_rescrape_event_ids_via_gsi_skips_recently_fetched_items(monkeypatch):
    table = Mock()
    table.query.side_effect = [
        {
            "Items": [
                {
                    "event_id": 1001,
                    "sk": "METADATA",
                    "status_text": "Sanctioned",
                    "end_date": "2026-02-01",
                    "last_fetched_at": "2026-04-26T10:00:00Z",
                },
                {
                    "event_id": 1002,
                    "sk": "METADATA",
                    "status_text": "Sanctioned",
                    "end_date": "2026-02-02",
                    "last_fetched_at": "2026-04-28T23:00:00Z",
                },
                {
                    "event_id": 1003,
                    "sk": "METADATA",
                    "status_text": "Sanctioned",
                    "end_date": "2026-02-03",
                },
            ]
        }
    ]

    resource = Mock()
    resource.Table.return_value = table

    monkeypatch.setattr(dynamo_reader.boto3, "resource", lambda *args, **kwargs: resource)

    result = list(
        dynamo_reader.iter_rescrape_event_ids_via_gsi(
            table_name="pdga-table",
            gsi_name="gsi_status_end_date",
            status_texts=["Sanctioned"],
            start_date="2025-09-04",
            end_before_date="2026-05-01",
            older_than_ts="2026-04-27T00:00:00Z",
            aws_region="us-east-1",
        )
    )

    assert result == [1001, 1003]


def test_iter_rescrape_event_ids_via_gsi_skips_recent_failures(monkeypatch):
    table = Mock()
    table.query.side_effect = [
        {
            "Items": [
                {
                    "event_id": 1001,
                    "sk": "METADATA",
                    "status_text": "Sanctioned",
                    "end_date": "2026-02-01",
                    "last_fetched_at": "2026-04-20T10:00:00Z",
                    "last_fetch_status": "failed",
                    "last_fetch_failed_at": "2026-04-29T06:00:00Z",
                },
                {
                    "event_id": 1002,
                    "sk": "METADATA",
                    "status_text": "Sanctioned",
                    "end_date": "2026-02-02",
                    "last_fetched_at": "2026-04-20T10:00:00Z",
                    "last_fetch_status": "failed",
                    "last_fetch_failed_at": "2026-04-25T06:00:00Z",
                },
                {
                    "event_id": 1003,
                    "sk": "METADATA",
                    "status_text": "Sanctioned",
                    "end_date": "2026-02-03",
                    "last_fetched_at": "2026-04-20T10:00:00Z",
                    "last_fetch_status": "success",
                    "last_fetch_failed_at": "",
                },
            ]
        }
    ]

    resource = Mock()
    resource.Table.return_value = table

    monkeypatch.setattr(dynamo_reader.boto3, "resource", lambda *args, **kwargs: resource)

    result = list(
        dynamo_reader.iter_rescrape_event_ids_via_gsi(
            table_name="pdga-table",
            gsi_name="gsi_status_end_date",
            status_texts=["Sanctioned"],
            start_date="2025-09-04",
            end_before_date="2026-05-01",
            older_than_ts="2026-04-27T00:00:00Z",
            failed_older_than_ts="2026-04-26T12:00:00Z",
            aws_region="us-east-1",
        )
    )

    assert result == [1002, 1003]


def test_iter_rescrape_event_ids_via_gsi_returns_empty_when_window_invalid(monkeypatch):
    table = Mock()
    resource = Mock()
    resource.Table.return_value = table

    monkeypatch.setattr(dynamo_reader.boto3, "resource", lambda *args, **kwargs: resource)

    result = list(
        dynamo_reader.iter_rescrape_event_ids_via_gsi(
            table_name="pdga-table",
            gsi_name="gsi_status_end_date",
            status_texts=["Sanctioned"],
            start_date="2026-03-05",
            end_before_date="2026-03-04",
            aws_region="us-east-1",
        )
    )

    assert result == []
    table.query.assert_not_called()
