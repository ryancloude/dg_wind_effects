from unittest.mock import Mock

import ingest_pdga_event_pages.dynamo_writer as dynamo_writer


def test_upsert_event_metadata_writes_expected_fields(monkeypatch):
    table = Mock()
    table.update_item.return_value = {"Attributes": {"pk": "EVENT#123", "sk": "METADATA"}}

    resource = Mock()
    resource.Table.return_value = table

    monkeypatch.setattr(dynamo_writer.boto3, "resource", lambda *args, **kwargs: resource)
    monkeypatch.setattr(dynamo_writer, "utc_now_iso", lambda: "2026-03-03T12:00:00Z")

    parsed = {
        "event_id": 123,
        "source_url": "https://www.pdga.com/tour/event/123",
        "name": "Test Event",
        "raw_date_str": "12-Apr-2025",
        "start_date": "2025-04-12",
        "end_date": "2025-04-12",
        "status_text": "Official",
        "division_rounds": {"MA1": 2},
        "location_raw": "Austin, TX, United States",
        "city": "Austin",
        "state": "TX",
        "country": "United States",
        "content_sha256": "content-hash",
        "parse_warnings": [],
        "parser_version": "event-page-v3",
        "idempotency_sha256": "idem-hash",
        "raw_html_sha256": "raw-hash",
        "is_unscheduled_placeholder": False,
    }
    s3_ptrs = {
        "s3_html_key": "bronze/pdga/a.html.gz",
        "s3_meta_key": "bronze/pdga/a.meta.json",
        "fetched_at": "2026-03-03T12:05:00Z",
    }

    result = dynamo_writer.upsert_event_metadata(
        table_name="pdga-table",
        parsed=parsed,
        s3_ptrs=s3_ptrs,
        aws_region="us-east-1",
    )

    assert result == {"pk": "EVENT#123", "sk": "METADATA"}

    kwargs = table.update_item.call_args.kwargs
    assert kwargs["Key"] == {"pk": "EVENT#123", "sk": "METADATA"}
    assert kwargs["ExpressionAttributeValues"][":event_id"] == 123
    assert kwargs["ExpressionAttributeValues"][":division_rounds"] == {"MA1": 2}
    assert kwargs["ExpressionAttributeValues"][":location_raw"] == "Austin, TX, United States"
    assert kwargs["ExpressionAttributeValues"][":city"] == "Austin"
    assert kwargs["ExpressionAttributeValues"][":state"] == "TX"
    assert kwargs["ExpressionAttributeValues"][":country"] == "United States"
    assert kwargs["ExpressionAttributeValues"][":is_unscheduled_placeholder"] is False