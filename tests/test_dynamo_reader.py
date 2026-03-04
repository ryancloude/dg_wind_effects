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


def test_iter_rescrape_event_ids_filters_by_status_and_date(monkeypatch):
    table = Mock()
    table.scan.side_effect = [
        {
            "Items": [
                {
                    "event_id": 1001,
                    "sk": "METADATA",
                    "status_text": "Sanctioned",
                    "end_date": "2026-02-01",
                },
                {
                    "event_id": 1002,
                    "sk": "METADATA",
                    "status_text": "Event complete.",
                    "end_date": "2026-02-01",
                },
                {
                    "event_id": 1003,
                    "sk": "METADATA",
                    "status_text": "Sanctioned",
                    "end_date": "2025-08-01",
                },
                {
                    "event_id": 1004,
                    "sk": "METADATA",
                    "status_text": "Sanctioned",
                    "end_date": "2026-03-10",
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
                    "event_id": 1006,
                    "sk": "METADATA",
                    "status_text": "Errata pending.",
                    "end_date": "2026-01-15",
                },
                {
                    "event_id": 1007,
                    "sk": "METADATA",
                    "status_text": "Sanctioned",
                    "end_date": "",
                },
            ]
        },
    ]

    resource = Mock()
    resource.Table.return_value = table

    monkeypatch.setattr(dynamo_reader.boto3, "resource", lambda *args, **kwargs: resource)

    event_ids = list(
        dynamo_reader.iter_rescrape_event_ids(
            table_name="pdga-table",
            status_texts=["Sanctioned", "Errata pending."],
            start_date="2025-09-04",
            end_before_date="2026-03-04",
            aws_region="us-east-1",
        )
    )

    assert event_ids == [1001, 1006]


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