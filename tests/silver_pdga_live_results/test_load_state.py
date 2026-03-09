from decimal import Decimal
from unittest.mock import Mock

from botocore.exceptions import ClientError

import silver_pdga_live_results.load_state as load_state


def test_build_round_unit_key_normalizes_division():
    key = load_state.build_round_unit_key(90008, "ma3", 1)
    assert key == "EVENT#90008#DIV#MA3#ROUND#1"


def test_get_global_checkpoint_returns_empty_when_missing(monkeypatch):
    table = Mock()
    table.get_item.return_value = {}

    resource = Mock()
    resource.Table.return_value = table
    monkeypatch.setattr(load_state.boto3, "resource", lambda *args, **kwargs: resource)

    checkpoint = load_state.get_global_checkpoint(
        table_name="pdga-event-index",
        pipeline_name="live_results_silver",
        aws_region="us-east-1",
    )

    assert checkpoint.last_processed_fetch_ts is None
    assert checkpoint.last_processed_s3_key is None
    assert checkpoint.has_cursor() is False


def test_put_global_checkpoint_returns_false_on_conditional_failure(monkeypatch):
    table = Mock()
    table.update_item.side_effect = ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException", "Message": "stale cursor"}},
        "UpdateItem",
    )

    resource = Mock()
    resource.Table.return_value = table
    monkeypatch.setattr(load_state.boto3, "resource", lambda *args, **kwargs: resource)
    monkeypatch.setattr(load_state, "utc_now_iso", lambda: "2026-03-08T12:00:00Z")

    updated = load_state.put_global_checkpoint(
        table_name="pdga-event-index",
        pipeline_name="live_results_silver",
        last_processed_fetch_ts="2026-03-08T11:00:00Z",
        last_processed_s3_key="bronze/pdga/live_results/event_id=1/division=MPO/round=1/fetch_date=2026-03-08/fetch_ts=2026-03-08T11:00:00Z.json",
        run_id="run-1",
        aws_region="us-east-1",
    )

    assert updated is False


def test_get_round_unit_state_parses_row_count(monkeypatch):
    table = Mock()
    table.get_item.return_value = {
        "Item": {
            "pk": "SILVER#LIVE_RESULTS_SILVER",
            "sk": "STATE#UNIT#EVENT#90008#DIV#MA3#ROUND#1",
            "last_applied_sha256": "abc123",
            "last_applied_fetch_ts": "2026-03-08T11:00:00Z",
            "last_applied_s3_key": "bronze/a.json",
            "last_applied_row_count": Decimal("11"),
            "last_run_id": "run-2",
            "updated_at": "2026-03-08T12:00:00Z",
        }
    }

    resource = Mock()
    resource.Table.return_value = table
    monkeypatch.setattr(load_state.boto3, "resource", lambda *args, **kwargs: resource)

    state = load_state.get_round_unit_state(
        table_name="pdga-event-index",
        pipeline_name="live_results_silver",
        unit_key="EVENT#90008#DIV#MA3#ROUND#1",
        aws_region="us-east-1",
    )

    assert state is not None
    assert state.last_applied_sha256 == "abc123"
    assert state.last_applied_row_count == 11


def test_put_round_unit_state_writes_expected_values(monkeypatch):
    table = Mock()
    table.update_item.return_value = {"Attributes": {"ok": True}}

    resource = Mock()
    resource.Table.return_value = table
    monkeypatch.setattr(load_state.boto3, "resource", lambda *args, **kwargs: resource)
    monkeypatch.setattr(load_state, "utc_now_iso", lambda: "2026-03-08T12:00:00Z")

    updated = load_state.put_round_unit_state(
        table_name="pdga-event-index",
        pipeline_name="live_results_silver",
        unit_key="EVENT#90008#DIV#MA3#ROUND#1",
        last_applied_sha256="abc123",
        last_applied_fetch_ts="2026-03-08T11:00:00Z",
        last_applied_s3_key="bronze/a.json",
        last_applied_row_count=11,
        run_id="run-3",
        aws_region="us-east-1",
    )

    assert updated is True
    kwargs = table.update_item.call_args.kwargs
    assert kwargs["Key"]["pk"] == "SILVER#LIVE_RESULTS_SILVER"
    assert kwargs["ExpressionAttributeValues"][":row_count"] == 11