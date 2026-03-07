from decimal import Decimal
from unittest.mock import Mock

from botocore.exceptions import ClientError

import ingest_pdga_live_results.dynamo_reader as dynamo_reader
from ingest_pdga_live_results.dynamo_reader import LiveResultsTask


def test_expand_tasks_from_metadata_item_happy_path():
    metadata_item = {
        "event_id": "86076",
        "division_rounds": {
            "MP40": 3,
            "MA1": 2,
        },
    }

    tasks = dynamo_reader.expand_tasks_from_metadata_item(metadata_item)

    assert tasks == [
        LiveResultsTask(event_id="86076", division="MP40", round_number=1),
        LiveResultsTask(event_id="86076", division="MP40", round_number=2),
        LiveResultsTask(event_id="86076", division="MP40", round_number=3),
        LiveResultsTask(event_id="86076", division="MA1", round_number=1),
        LiveResultsTask(event_id="86076", division="MA1", round_number=2),
    ]


def test_expand_tasks_accepts_decimal_round_values():
    metadata_item = {
        "event_id": "92608",
        "division_rounds": {
            "MPO": Decimal("2"),
            "MA4": Decimal("2"),
        },
    }

    tasks = dynamo_reader.expand_tasks_from_metadata_item(metadata_item)

    assert tasks == [
        LiveResultsTask(event_id="92608", division="MPO", round_number=1),
        LiveResultsTask(event_id="92608", division="MPO", round_number=2),
        LiveResultsTask(event_id="92608", division="MA4", round_number=1),
        LiveResultsTask(event_id="92608", division="MA4", round_number=2),
    ]


def test_expand_tasks_skips_invalid_round_values():
    metadata_item = {
        "event_id": "86076",
        "division_rounds": {
            "MP40": 0,
            "MA1": -2,
            "FA1": "3",
            "FPO": 1,
            "": 2,
        },
    }

    tasks = dynamo_reader.expand_tasks_from_metadata_item(metadata_item)

    assert tasks == [
        LiveResultsTask(event_id="86076", division="FPO", round_number=1),
    ]


def test_should_include_metadata_item_excludes_ingested():
    item = {"live_results_ingested": True, "division_rounds": {"MPO": Decimal("2")}}
    assert (
        dynamo_reader.should_include_metadata_item(
            item,
            exclude_already_live_results_ingested=True,
        )
        is False
    )


def test_should_include_metadata_item_excludes_status():
    item = {"status_text": "Sanctioned", "division_rounds": {"MPO": Decimal("2")}}
    assert (
        dynamo_reader.should_include_metadata_item(
            item,
            excluded_statuses={"Sanctioned"},
        )
        is False
    )


def test_should_include_metadata_item_requires_non_empty_division_rounds():
    item = {"status_text": "Event complete; official ratings processed.", "division_rounds": {}}
    assert (
        dynamo_reader.should_include_metadata_item(
            item,
            require_non_empty_division_rounds=True,
        )
        is False
    )


def test_resolve_required_statuses_excludes_statuses():
    statuses = dynamo_reader._resolve_required_statuses({"Event complete; unofficial ratings processed."})
    assert statuses == ["Event complete; official ratings processed."]


def test_iter_event_ids_via_status_gsi_dedupes_and_filters_metadata_only():
    table = Mock()
    table.query.side_effect = [
        {
            "Items": [
                {"event_id": 1001, "sk": "METADATA"},
                {"event_id": 1001, "sk": "METADATA"},
                {"event_id": 1009, "sk": "OTHER"},
            ],
            "LastEvaluatedKey": {"pk": "next"},
        },
        {
            "Items": [{"event_id": 1002, "sk": "METADATA"}],
        },
        {
            "Items": [{"event_id": 1003, "sk": "METADATA"}],
        },
    ]

    result = list(
        dynamo_reader._iter_event_ids_via_status_gsi(
            table=table,
            gsi_name="gsi_status_end_date",
            statuses=[
                "Event complete; official ratings processed.",
                "Event complete; unofficial ratings processed.",
            ],
            end_before_date="2026-03-07",
            filter_not_ingested_on_gsi=True,
        )
    )

    assert result == [1001, 1002, 1003]
    assert table.query.call_count == 3


def test_iter_event_ids_via_status_gsi_falls_back_when_filter_attr_not_projected():
    table = Mock()
    table.query.side_effect = [
        ClientError(
            {
                "Error": {
                    "Code": "ValidationException",
                    "Message": "Invalid attribute: live_results_ingested",
                }
            },
            "Query",
        ),
        {"Items": [{"event_id": 2001, "sk": "METADATA"}]},
    ]

    result = list(
        dynamo_reader._iter_event_ids_via_status_gsi(
            table=table,
            gsi_name="gsi_status_end_date",
            statuses=["Event complete; official ratings processed."],
            end_before_date="2026-03-07",
            filter_not_ingested_on_gsi=True,
        )
    )

    assert result == [2001]
    assert table.query.call_count == 2


def test_iter_metadata_items_event_ids_path_filters(monkeypatch):
    table = Mock()
    resource = Mock()
    resource.Table.return_value = table

    monkeypatch.setattr(dynamo_reader.boto3, "resource", lambda *args, **kwargs: resource)

    items_by_id = {
        1: {
            "event_id": 1,
            "status_text": "Event complete; official ratings processed.",
            "division_rounds": {"MPO": Decimal("2")},
            "live_results_ingested": False,
        },
        2: {
            "event_id": 2,
            "status_text": "Event complete; official ratings processed.",
            "division_rounds": {"FPO": Decimal("2")},
            "live_results_ingested": False,
        },
    }

    monkeypatch.setattr(dynamo_reader, "_get_metadata_item_by_event_id", lambda _table, event_id: items_by_id.get(event_id))
    monkeypatch.setattr(dynamo_reader, "_event_has_any_live_results_state", lambda _table, event_id: event_id == 2)

    result = list(
        dynamo_reader.iter_metadata_items(
            table_name="pdga-table",
            event_ids=[1, 2],
            require_non_empty_division_rounds=True,
            skip_events_with_live_results_state=True,
            use_status_end_date_gsi=False,
            aws_region="us-east-1",
        )
    )

    assert result == [items_by_id[1]]


def test_load_live_results_tasks_expands_metadata(monkeypatch):
    metadata_items = [
        {"event_id": "86076", "division_rounds": {"MPO": 2}},
        {"event_id": "86077", "division_rounds": {"FPO": 1}},
    ]

    monkeypatch.setattr(dynamo_reader, "iter_metadata_items", lambda **kwargs: iter(metadata_items))

    result = dynamo_reader.load_live_results_tasks(table_name="pdga-table")

    assert result == [
        LiveResultsTask(event_id="86076", division="MPO", round_number=1),
        LiveResultsTask(event_id="86076", division="MPO", round_number=2),
        LiveResultsTask(event_id="86077", division="FPO", round_number=1),
    ]