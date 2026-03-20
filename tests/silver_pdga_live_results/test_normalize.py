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
                    "ShortName": "D. Dodson",
                    "ProfileURL": "/player/211819644",
                    "City": "Blackwell",
                    "StateProv": "OK",
                    "Country": "US",
                    "FullLocation": "Blackwell, OK, US",
                    "Rating": 915,
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
    assert round_row["short_name"] == "D. Dodson"
    assert round_row["profile_url"] == "/player/211819644"
    assert round_row["player_city"] == "Blackwell"
    assert round_row["player_state_prov"] == "OK"
    assert round_row["player_country"] == "US"
    assert round_row["player_full_location"] == "Blackwell, OK, US"
    assert round_row["player_rating"] == 915

    assert round_row["tee_time_est_method"] == "missing_inputs"
    assert round_row["tee_time_est_ts"] == ""

    assert {row["hole_number"] for row in hole_rows} == {1, 2}
    assert {row["hole_score"] for row in hole_rows} == {2, 3}

    for hole_row in hole_rows:
        assert hole_row["first_name"] == "Draigen"
        assert hole_row["last_name"] == "Dodson"
        assert hole_row["short_name"] == "D. Dodson"
        assert hole_row["profile_url"] == "/player/211819644"
        assert hole_row["player_city"] == "Blackwell"
        assert hole_row["player_state_prov"] == "OK"
        assert hole_row["player_country"] == "US"
        assert hole_row["player_full_location"] == "Blackwell, OK, US"
        assert hole_row["player_rating"] == 915