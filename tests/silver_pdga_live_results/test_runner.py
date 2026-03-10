from silver_pdga_live_results.models import ROUND_PK_COLS, ROUND_TIEBREAK_COLS
from silver_pdga_live_results.runner import dedup_rows, parse_event_ids


def test_parse_event_ids():
    assert parse_event_ids(None) is None
    assert parse_event_ids("90008, 90009") == [90008, 90009]


def test_dedup_rows_prefers_latest_tiebreak():
    old_row = {
        "tourn_id": 90008,
        "round_number": 1,
        "player_key": "PDGA#123",
        "round_score": 55,
        "source_fetched_at_utc": "2025-05-17T22:00:00Z",
        "scorecard_updated_at_ts": "2025-05-17T15:00:00",
        "update_date_ts": "2025-05-17T22:00:00",
        "source_json_key": "older.json",
    }
    new_row = dict(old_row)
    new_row["round_score"] = 50
    new_row["source_fetched_at_utc"] = "2025-05-17T22:35:04Z"
    new_row["source_json_key"] = "newer.json"

    out = dedup_rows([old_row, new_row], ROUND_PK_COLS, ROUND_TIEBREAK_COLS)

    assert len(out) == 1
    assert out[0]["round_score"] == 50
    assert out[0]["source_json_key"] == "newer.json"