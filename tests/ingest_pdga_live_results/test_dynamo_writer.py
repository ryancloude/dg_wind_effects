from unittest.mock import Mock

import ingest_pdga_live_results.dynamo_writer as dynamo_writer


def test_get_existing_live_results_sha256_returns_none_when_item_missing(monkeypatch):
    table = Mock()
    table.get_item.return_value = {}

    resource = Mock()
    resource.Table.return_value = table

    monkeypatch.setattr(dynamo_writer.boto3, "resource", lambda *args, **kwargs: resource)

    result = dynamo_writer.get_existing_live_results_sha256(
        table_name="pdga-table",
        event_id=123,
        division="MPO",
        round_number=2,
        aws_region="us-east-1",
    )

    assert result is None
    table.get_item.assert_called_once_with(
        Key={"pk": "EVENT#123", "sk": "LIVE_RESULTS#DIV#MPO#ROUND#2"},
        ConsistentRead=False,
    )


def test_get_existing_live_results_sha256_returns_hash(monkeypatch):
    table = Mock()
    table.get_item.return_value = {"Item": {"content_sha256": "abc123"}}

    resource = Mock()
    resource.Table.return_value = table

    monkeypatch.setattr(dynamo_writer.boto3, "resource", lambda *args, **kwargs: resource)

    result = dynamo_writer.get_existing_live_results_sha256(
        table_name="pdga-table",
        event_id=123,
        division="MPO",
        round_number=2,
        aws_region="us-east-1",
    )

    assert result == "abc123"


def test_upsert_live_results_state_writes_expected_fields(monkeypatch):
    table = Mock()
    table.update_item.return_value = {"Attributes": {"pk": "EVENT#123", "sk": "LIVE_RESULTS#DIV#MPO#ROUND#2"}}

    resource = Mock()
    resource.Table.return_value = table

    monkeypatch.setattr(dynamo_writer.boto3, "resource", lambda *args, **kwargs: resource)
    monkeypatch.setattr(dynamo_writer, "utc_now_iso", lambda: "2026-03-06T12:00:00Z")

    result = dynamo_writer.upsert_live_results_state(
        table_name="pdga-table",
        event_id=123,
        division="MPO",
        round_number=2,
        source_url="https://www.pdga.com/apps/tournament/live-api/live_results_fetch_round?TournID=123&Division=MPO&Round=2",
        status_text="success",
        content_sha256="hash-1",
        s3_ptrs={
            "s3_json_key": "bronze/pdga/live_results/a.json",
            "s3_meta_key": "bronze/pdga/live_results/a.meta.json",
            "fetched_at": "2026-03-06T12:00:05Z",
        },
        run_id="run-123",
        aws_region="us-east-1",
    )

    assert result == {"pk": "EVENT#123", "sk": "LIVE_RESULTS#DIV#MPO#ROUND#2"}

    kwargs = table.update_item.call_args.kwargs
    assert kwargs["Key"] == {"pk": "EVENT#123", "sk": "LIVE_RESULTS#DIV#MPO#ROUND#2"}
    assert kwargs["ExpressionAttributeValues"][":event_id"] == 123
    assert kwargs["ExpressionAttributeValues"][":division"] == "MPO"
    assert kwargs["ExpressionAttributeValues"][":round_number"] == 2
    assert kwargs["ExpressionAttributeValues"][":content_sha256"] == "hash-1"
    assert kwargs["ExpressionAttributeValues"][":last_run_id"] == "run-123"


def test_put_live_results_run_summary_writes_item(monkeypatch):
    table = Mock()

    resource = Mock()
    resource.Table.return_value = table

    monkeypatch.setattr(dynamo_writer.boto3, "resource", lambda *args, **kwargs: resource)
    monkeypatch.setattr(dynamo_writer, "utc_now_iso", lambda: "2026-03-06T12:00:00Z")

    stats = {
        "attempted": 10,
        "success": 9,
        "not_found_404": 1,
        "empty": 0,
        "changed": 3,
        "unchanged": 6,
        "failed": 0,
    }

    item = dynamo_writer.put_live_results_run_summary(
        table_name="pdga-table",
        run_id="run-abc",
        stats=stats,
        aws_region="us-east-1",
    )

    assert item["pk"] == "RUN#run-abc"
    assert item["sk"] == "LIVE_RESULTS#SUMMARY"
    assert item["created_at"] == "2026-03-06T12:00:00Z"
    assert item["attempted"] == 10

    table.put_item.assert_called_once()
    assert table.put_item.call_args.kwargs["Item"] == item


def test_mark_event_live_results_ingested_sets_metadata_flags(monkeypatch):
    table = Mock()
    table.update_item.return_value = {"Attributes": {"pk": "EVENT#123", "sk": "METADATA", "live_results_ingested": True}}

    resource = Mock()
    resource.Table.return_value = table

    monkeypatch.setattr(dynamo_writer.boto3, "resource", lambda *args, **kwargs: resource)
    monkeypatch.setattr(dynamo_writer, "utc_now_iso", lambda: "2026-03-06T12:00:00Z")

    result = dynamo_writer.mark_event_live_results_ingested(
        table_name="pdga-table",
        event_id=123,
        run_id="run-abc",
        aws_region="us-east-1",
    )

    assert result["live_results_ingested"] is True

    kwargs = table.update_item.call_args.kwargs
    assert kwargs["Key"] == {"pk": "EVENT#123", "sk": "METADATA"}
    assert kwargs["ExpressionAttributeValues"][":true_value"] is True
    assert kwargs["ExpressionAttributeValues"][":ingested_at"] == "2026-03-06T12:00:00Z"
    assert kwargs["ExpressionAttributeValues"][":run_id"] == "run-abc"