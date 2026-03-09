import pytest

import silver_pdga_live_results.layout_hole_transform as transform
from silver_pdga_live_results.candidate_reader import LiveResultsStatePointer


def _pointer():
    return LiveResultsStatePointer(
        event_id=90008,
        division="MA3",
        round_number=1,
        fetch_status="success",
        content_sha256="source-hash-1",
        last_fetched_at="2026-03-08T11:00:00Z",
        latest_s3_json_key="bronze/pdga/live_results/event_id=90008/division=MA3/round=1/fetch_date=2026-03-08/fetch_ts=2026-03-08T11:00:00Z.json",
        latest_s3_meta_key="bronze/pdga/live_results/event_id=90008/division=MA3/round=1/fetch_date=2026-03-08/fetch_ts=2026-03-08T11:00:00Z.meta.json",
        source_url="https://example.test/live_results",
    )


def test_transform_layout_hole_rows_happy_path():
    payload = {
        "hash": "api-hash",
        "data": {
            "layouts": [
                {
                    "LayoutID": 712276,
                    "Name": "Main",
                    "Holes": 18,
                    "Par": 54,
                    "Length": 5207,
                    "Units": "Feet",
                    "CourseID": 25248,
                    "CourseName": "War Memorial Park",
                    "UpdateDate": "2025-05-17 22:36:02",
                }
            ],
            "holes": [
                {"Hole": "H1", "HoleOrdinal": 1, "Label": "1", "Par": 3, "Length": 297, "Ordinal": 1},
                {"Hole": "H2", "HoleOrdinal": 2, "Label": "2", "Par": 3, "Length": 265, "Ordinal": 2},
                {"Hole": "HX", "Label": "bad", "Par": 3},  # invalid missing ordinal
            ],
        },
    }

    rows, stats = transform.transform_layout_hole_rows(
        pointer=_pointer(),
        payload=payload,
        run_id="run-layout-1",
        loaded_at_iso="2026-03-08T12:00:00Z",
    )

    assert stats["total_layouts"] == 1
    assert stats["total_holes"] == 3
    assert stats["output_rows"] == 2
    assert stats["skipped_invalid_holes"] == 1
    assert len(rows) == 2
    assert rows[0]["layout_id"] == 712276
    assert rows[0]["source_event_id"] == 90008
    assert len(rows[0]["layout_row_hash"]) == 64


def test_validate_layout_hole_rows_duplicate_key_raises():
    rows = [
        {"layout_id": 10, "hole_ordinal": 1, "hole_par": 3},
        {"layout_id": 10, "hole_ordinal": 1, "hole_par": 3},
    ]
    with pytest.raises(ValueError, match="layout_hole_duplicate_primary_key"):
        transform.validate_layout_hole_rows(rows)


def test_compute_layout_group_hash_is_stable_for_row_order():
    rows_a = [
        {"layout_id": 10, "hole_ordinal": 1, "layout_row_hash": "a"},
        {"layout_id": 10, "hole_ordinal": 2, "layout_row_hash": "b"},
    ]
    rows_b = [
        {"layout_id": 10, "hole_ordinal": 2, "layout_row_hash": "b"},
        {"layout_id": 10, "hole_ordinal": 1, "layout_row_hash": "a"},
    ]

    assert transform.compute_layout_group_hash(rows_a) == transform.compute_layout_group_hash(rows_b)


def test_transform_layout_hole_rows_bad_payload_raises():
    with pytest.raises(ValueError, match="live_results_payload_must_be_object"):
        transform.transform_layout_hole_rows(
            pointer=_pointer(),
            payload=[],
            run_id="run-layout-bad",
        )