from ingest_pdga_live_results.dynamo_reader import (
    LiveResultsTask,
    expand_tasks_from_metadata_item,
    expand_tasks_from_metadata_items,
)


def test_expand_tasks_from_metadata_item_happy_path() -> None:
    metadata_item = {
        "event_id": "86076",
        "division_rounds": {
            "MP40": 3,
            "MA1": 2,
        },
    }

    tasks = expand_tasks_from_metadata_item(metadata_item)

    assert tasks == [
        LiveResultsTask(event_id="86076", division="MP40", round_number=1),
        LiveResultsTask(event_id="86076", division="MP40", round_number=2),
        LiveResultsTask(event_id="86076", division="MP40", round_number=3),
        LiveResultsTask(event_id="86076", division="MA1", round_number=1),
        LiveResultsTask(event_id="86076", division="MA1", round_number=2),
    ]


def test_expand_tasks_skips_invalid_round_values() -> None:
    metadata_item = {
        "event_id": "86076",
        "division_rounds": {
            "MP40": 0,        # invalid
            "MA1": -2,        # invalid
            "FA1": "3",       # invalid type
            "FPO": 1,         # valid
            "": 2,            # invalid division key
        },
    }

    tasks = expand_tasks_from_metadata_item(metadata_item)

    assert tasks == [
        LiveResultsTask(event_id="86076", division="FPO", round_number=1),
    ]


def test_expand_tasks_returns_empty_for_missing_event_id_or_division_rounds() -> None:
    assert expand_tasks_from_metadata_item({}) == []
    assert expand_tasks_from_metadata_item({"event_id": "86076"}) == []
    assert expand_tasks_from_metadata_item({"division_rounds": {"MP40": 2}}) == []
    assert expand_tasks_from_metadata_item({"event_id": "86076", "division_rounds": []}) == []


def test_expand_tasks_from_multiple_metadata_items() -> None:
    items = [
        {"event_id": "86076", "division_rounds": {"MP40": 2}},
        {"event_id": "86077", "division_rounds": {"MA1": 1}},
    ]

    tasks = expand_tasks_from_metadata_items(items)

    assert tasks == [
        LiveResultsTask(event_id="86076", division="MP40", round_number=1),
        LiveResultsTask(event_id="86076", division="MP40", round_number=2),
        LiveResultsTask(event_id="86077", division="MA1", round_number=1),
    ]