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