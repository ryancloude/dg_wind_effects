from silver_pdga_live_results.models import BronzeRoundSource
from silver_pdga_live_results.normalize import normalize_event_records


def test_normalize_uses_result_id_fallback_and_enriches_location():
    event_metadata = {
        "event_id": 90008,
        "name": "Tall Grass Open",
        "status_text": "Event complete; official ratings processed.",
        "start_date": "2025-05-17",
        "end_date": "2025-05-17",
        "raw_location": "Blackwell, OK, US",
        "city": "Blackwell",
        "state": "OK",
        "country": "US",
    }

    payload = {
        "data": {
            "scores": [
                {
                    "Division": "MA3",
                    "Round": 1,
                    "LayoutID": 712276,
                    "Name": "Draigen Dodson",
                    "FirstName": "Draigen",
                    "LastName": "Dodson",
                    "PDGANum": None,
                    "ResultID": 211819644,
                    "HoleScores": ["3", "2"],
                    "Holes": 2,
                    "Played": 2,
                    "RoundScore": 5,
                    "RoundtoPar": -1,
                    "RoundStatus": "I",
                    "ScoreID": 24092892,
                    "RoundID": 122551235,
                }
            ],
            "layouts": [
                {
                    "LayoutID": 712276,
                    "TournID": 90008,
                    "CourseID": 25248,
                    "CourseName": "War Memorial Park",
                    "Name": "Main",
                    "Holes": 2,
                    "Par": 6,
                    "Length": 490,
                    "Units": "Feet",
                    "Detail": [
                        {"Ordinal": 1, "Hole": "H1", "Label": "1", "Par": 3, "Length": 250},
                        {"Ordinal": 2, "Hole": "H2", "Label": "2", "Par": 3, "Length": 240},
                    ],
                }
            ],
            "holes": [],
        }
    }

    source = BronzeRoundSource(
        event_id=90008,
        division="MA3",
        round_number=1,
        source_json_key="bronze/pdga/live_results/event_id=90008/division=MA3/round=1/fetch_date=2025-05-17/fetch_ts=2025-05-17T22:35:04Z.json",
        source_meta_key="bronze/pdga/live_results/event_id=90008/division=MA3/round=1/fetch_date=2025-05-17/fetch_ts=2025-05-17T22:35:04Z.meta.json",
        source_content_sha256="abc123",
        source_fetched_at_utc="2025-05-17T22:35:04Z",
        payload=payload,
    )

    round_rows, hole_rows = normalize_event_records(
        event_metadata=event_metadata,
        round_sources=[source],
        event_source_fingerprint="fp1",
        run_id="run-1",
        silver_processed_at_utc="2026-03-10T10:00:00Z",
    )

    assert len(round_rows) == 1
    assert len(hole_rows) == 2

    round_row = round_rows[0]
    assert round_row["player_key"] == "RESULT#211819644"
    assert round_row["player_key_type"] == "result_id"
    assert round_row["event_location_raw"] == "Blackwell, OK, US"
    assert round_row["event_city"] == "Blackwell"
    assert round_row["event_state"] == "OK"
    assert round_row["event_country"] == "US"

    assert {row["hole_number"] for row in hole_rows} == {1, 2}
    assert {row["hole_score"] for row in hole_rows} == {2, 3}


def test_normalize_falls_back_to_scores_string_when_hole_scores_missing():
    event_metadata = {
        "event_id": 90009,
        "status_text": "Event complete; unofficial ratings processed.",
        "end_date": "2025-06-01",
    }

    payload = {
        "data": {
            "scores": [
                {
                    "Division": "MPO",
                    "Round": 1,
                    "LayoutID": 101,
                    "Name": "No HoleScores",
                    "PDGANum": 12345,
                    "ResultID": 555,
                    "Scores": "3,4,,,,",
                    "Holes": 2,
                    "Played": 2,
                    "RoundScore": 7,
                    "RoundtoPar": 1,
                    "RoundStatus": "I",
                }
            ],
            "layouts": [
                {
                    "LayoutID": 101,
                    "Name": "A",
                    "Holes": 2,
                    "Par": 6,
                    "Detail": [
                        {"Ordinal": 1, "Hole": "H1", "Par": 3, "Length": 300},
                        {"Ordinal": 2, "Hole": "H2", "Par": 3, "Length": 280},
                    ],
                }
            ],
            "holes": [],
        }
    }

    source = BronzeRoundSource(
        event_id=90009,
        division="MPO",
        round_number=1,
        source_json_key="k.json",
        source_meta_key="k.meta.json",
        source_content_sha256="sha",
        source_fetched_at_utc="2025-06-01T20:00:00Z",
        payload=payload,
    )

    _, hole_rows = normalize_event_records(
        event_metadata=event_metadata,
        round_sources=[source],
        event_source_fingerprint="fp2",
        run_id="run-2",
        silver_processed_at_utc="2026-03-10T10:00:00Z",
    )

    assert len(hole_rows) == 2
    assert [row["hole_score"] for row in hole_rows] == [3, 4]