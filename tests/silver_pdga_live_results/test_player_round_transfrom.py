import pytest

import silver_pdga_live_results.player_round_transform as transform
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


def test_transform_player_round_rows_happy_path_and_skips_missing_result_id():
    payload = {
        "hash": "api-hash",
        "data": {
            "live_round_id": 12345,
            "scores": [
                {
                    "ResultID": 211700400,
                    "RoundID": 122551235,
                    "LayoutID": 712276,
                    "PDGANum": 192598,
                    "Name": "Jesse Quilimaco",
                    "Round": 1,
                    "RoundScore": 50,
                    "RoundtoPar": -4,
                    "ToPar": -7,
                    "Par": 54,
                    "Played": 18,
                    "Completed": 1,
                    "CardNum": 2,
                    "Pool": "A",
                    "UpdateDate": "2025-05-17 22:35:04",
                    "ScorecardUpdatedAt": "2025-05-17 15:35:29",
                },
                {
                    "RoundID": 999,
                    "LayoutID": 712276,
                    "Name": "Missing ResultID Row",
                },
            ],
        },
    }

    rows, stats = transform.transform_player_round_rows(
        pointer=_pointer(),
        payload=payload,
        run_id="silver-run-1",
        loaded_at_iso="2026-03-08T12:00:00Z",
    )

    assert stats["total_scores"] == 2
    assert stats["output_rows"] == 1
    assert stats["skipped_missing_result_id"] == 1
    assert len(rows) == 1

    row = rows[0]
    assert row["event_id"] == 90008
    assert row["division_code"] == "MA3"
    assert row["round_number"] == 1
    assert row["result_id"] == 211700400
    assert row["layout_id"] == 712276
    assert row["round_par_consistent"] is True
    assert row["source_content_sha256"] == "source-hash-1"
    assert row["bronze_s3_json_key"].endswith(".json")
    assert len(row["silver_row_hash"]) == 64


def test_row_hash_is_stable_across_run_id_and_loaded_at():
    payload = {
        "hash": "api-hash",
        "data": {
            "scores": [
                {
                    "ResultID": 1,
                    "LayoutID": 10,
                    "Name": "Player A",
                    "Round": 1,
                    "RoundScore": 55,
                    "RoundtoPar": 1,
                    "Par": 54,
                }
            ]
        },
    }

    rows_a, _ = transform.transform_player_round_rows(
        pointer=_pointer(),
        payload=payload,
        run_id="run-a",
        loaded_at_iso="2026-03-08T12:00:00Z",
    )
    rows_b, _ = transform.transform_player_round_rows(
        pointer=_pointer(),
        payload=payload,
        run_id="run-b",
        loaded_at_iso="2026-03-08T12:01:00Z",
    )

    assert rows_a[0]["silver_row_hash"] == rows_b[0]["silver_row_hash"]


def test_transform_raises_for_duplicate_primary_key_rows():
    payload = {
        "data": {
            "scores": [
                {"ResultID": 1, "LayoutID": 10, "Name": "A", "Round": 1},
                {"ResultID": 1, "LayoutID": 10, "Name": "A", "Round": 1},
            ]
        }
    }

    with pytest.raises(ValueError, match="player_round_duplicate_primary_key"):
        transform.transform_player_round_rows(
            pointer=_pointer(),
            payload=payload,
            run_id="run-dup",
        )


def test_normalize_raises_for_bad_payload_shape():
    with pytest.raises(ValueError, match="live_results_payload_must_be_object"):
        transform.normalize_player_round_rows(
            pointer=_pointer(),
            payload=[],
            run_id="run-bad",
        )