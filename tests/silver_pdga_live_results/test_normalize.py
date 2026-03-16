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

    assert round_row["tee_time_est_method"] == "missing_inputs"
    assert round_row["tee_time_est_ts"] == ""

    assert {row["hole_number"] for row in hole_rows} == {1, 2}
    assert {row["hole_score"] for row in hole_rows} == {2, 3}

def test_hole_time_estimation_and_tee_time_raw_in_holes():
    event_metadata = {
        "event_id": 93001,
        "status_text": "Event complete; official ratings processed.",
        "start_date": "2025-06-20",
        "end_date": "2025-06-20",
    }

    payload = {
        "data": {
            "scores": [
                {
                    "Division": "MPO",
                    "Round": 1,
                    "LayoutID": 1,
                    "Name": "Player X",
                    "PDGANum": 999,
                    "TeeTime": "09:00:00",
                    "ScorecardUpdatedAt": "2025-06-20 10:00:00",
                    "HoleScores": ["3", "4"],
                    "Holes": 2,
                    "Played": 2,
                    "RoundScore": 7,
                    "RoundtoPar": 1,
                    "RoundStatus": "I",
                }
            ],
            "layouts": [
                {
                    "LayoutID": 1,
                    "Name": "Main",
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
        event_id=93001,
        division="MPO",
        round_number=1,
        source_json_key="k.json",
        source_meta_key="k.meta.json",
        source_content_sha256="sha",
        source_fetched_at_utc="2025-06-20T12:00:00Z",
        payload=payload,
    )

    _, hole_rows = normalize_event_records(
        event_metadata=event_metadata,
        round_sources=[source],
        event_source_fingerprint="fp",
        run_id="run",
        silver_processed_at_utc="2026-03-12T10:00:00Z",
    )

    assert len(hole_rows) == 2
    assert {r["tee_time_raw"] for r in hole_rows} == {"09:00:00"}
    assert all(r["round_duration_est_minutes"] == 240 for r in hole_rows)
    assert all(r["hole_time_est_method"] == "uniform_from_round_duration" for r in hole_rows)
    assert all(r["hole_start_est_ts"] != "" for r in hole_rows)
    assert all(r["hole_end_est_ts"] != "" for r in hole_rows)
    assert all(r["tee_time_join_method"] == "round_date_interp_plus_raw_tee" for r in hole_rows)
    assert {r["tee_time_join_ts"] for r in hole_rows} == {"2025-06-20T09:00:00"}

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


def test_tee_estimation_missing_tee_uses_global_lag():
    event_metadata = {
        "event_id": 92001,
        "status_text": "Event complete; official ratings processed.",
        "start_date": "2025-06-20",
        "end_date": "2025-06-20",
    }

    payload = {
        "data": {
            "scores": [
                {
                    "Division": "MPO",
                    "Round": 1,
                    "LayoutID": 1,
                    "Name": "Player A",
                    "PDGANum": 111,
                    "HoleScores": ["3", "3"],
                    "Holes": 2,
                    "Played": 2,
                    "RoundScore": 6,
                    "RoundtoPar": 0,
                    "RoundStatus": "I",
                    "ScorecardUpdatedAt": "2025-06-20 09:49:05",
                }
            ],
            "layouts": [
                {
                    "LayoutID": 1,
                    "Name": "Main",
                    "Holes": 2,
                    "Par": 6,
                    "Detail": [
                        {"Ordinal": 1, "Hole": "H1", "Par": 3, "Length": 300},
                        {"Ordinal": 2, "Hole": "H2", "Par": 3, "Length": 300},
                    ],
                }
            ],
            "holes": [],
        }
    }

    source = BronzeRoundSource(
        event_id=92001,
        division="MPO",
        round_number=1,
        source_json_key="x.json",
        source_meta_key="x.meta.json",
        source_content_sha256="sha",
        source_fetched_at_utc="2025-06-20T15:00:00Z",
        payload=payload,
    )

    round_rows, hole_rows = normalize_event_records(
        event_metadata=event_metadata,
        round_sources=[source],
        event_source_fingerprint="fp",
        run_id="run-tee-est",
        silver_processed_at_utc="2026-03-12T10:00:00Z",
    )

    assert len(round_rows) == 1
    rr = round_rows[0]
    assert rr["tee_time_est_method"] == "score_minus_global_median_lag"
    assert rr["lag_minutes_used"] == 449
    assert rr["lag_bucket_used"] == "global"
    assert rr["round_duration_est_minutes"] == 240
    assert rr["tee_time_est_ts"] == "2025-06-20T02:20:05"

    assert len(hole_rows) == 2
    assert {h["tee_time_est_method"] for h in hole_rows} == {"score_minus_global_median_lag"}
    assert {h["tee_time_est_ts"] for h in hole_rows} == {"2025-06-20T02:20:05"}
    assert rr["tee_time_join_method"] == "fallback_score_based"
    assert rr["tee_time_join_ts"] == rr["tee_time_est_ts"]

    assert {h["tee_time_join_method"] for h in hole_rows} == {"fallback_score_based"}

def test_tee_time_join_noon_fallback_when_tee_and_score_missing():
    event_metadata = {
        "event_id": 94001,
        "status_text": "Event complete; official ratings processed.",
        "start_date": "2025-06-20",
        "end_date": "2025-06-22",
        "division_rounds": {"MPO": 3},
    }

    payload = {
        "data": {
            "scores": [
                {
                    "Division": "MPO",
                    "Round": 2,
                    "LayoutID": 1,
                    "Name": "Player Noon",
                    "PDGANum": 123,
                    "HoleScores": ["3"],
                    "Holes": 1,
                    "Played": 1,
                    "RoundScore": 3,
                    "RoundtoPar": 0,
                    "RoundStatus": "I",
                    # TeeTime missing
                    # ScorecardUpdatedAt missing
                }
            ],
            "layouts": [
                {
                    "LayoutID": 1,
                    "Name": "Main",
                    "Holes": 1,
                    "Par": 3,
                    "Detail": [{"Ordinal": 1, "Hole": "H1", "Par": 3, "Length": 300}],
                }
            ],
            "holes": [],
        }
    }

    source = BronzeRoundSource(
        event_id=94001,
        division="MPO",
        round_number=2,
        source_json_key="z.json",
        source_meta_key="z.meta.json",
        source_content_sha256="sha",
        source_fetched_at_utc="2025-06-21T20:00:00Z",
        payload=payload,
    )

    round_rows, hole_rows = normalize_event_records(
        event_metadata=event_metadata,
        round_sources=[source],
        event_source_fingerprint="fp",
        run_id="run-noon",
        silver_processed_at_utc="2026-03-13T10:00:00Z",
    )

    assert len(round_rows) == 1
    rr = round_rows[0]
    assert rr["round_date_interp"] == "2025-06-21"
    assert rr["tee_time_join_method"] == "round_date_interp_noon_fallback"
    assert rr["tee_time_join_ts"] == "2025-06-21T12:00:00"

    assert len(hole_rows) == 1
    hr = hole_rows[0]
    assert hr["tee_time_join_method"] == "round_date_interp_noon_fallback"
    assert hr["tee_time_join_ts"] == "2025-06-21T12:00:00"

def test_tee_estimation_raw_tee_aligns_previous_day_when_needed():
    event_metadata = {
        "event_id": 92002,
        "status_text": "Event complete; official ratings processed.",
        "start_date": "2025-06-20",
        "end_date": "2025-06-20",
    }

    payload = {
        "data": {
            "scores": [
                {
                    "Division": "MPO",
                    "Round": 1,
                    "LayoutID": 2,
                    "Name": "Player B",
                    "PDGANum": 222,
                    "TeeTime": "11:30:00",
                    "HoleScores": ["3"],
                    "Holes": 1,
                    "Played": 1,
                    "RoundScore": 3,
                    "RoundtoPar": 0,
                    "RoundStatus": "I",
                    "ScorecardUpdatedAt": "2025-06-20 01:30:00",
                }
            ],
            "layouts": [
                {
                    "LayoutID": 2,
                    "Name": "Night",
                    "Holes": 1,
                    "Par": 3,
                    "Detail": [{"Ordinal": 1, "Hole": "H1", "Par": 3, "Length": 250}],
                }
            ],
            "holes": [],
        }
    }

    source = BronzeRoundSource(
        event_id=92002,
        division="MPO",
        round_number=1,
        source_json_key="y.json",
        source_meta_key="y.meta.json",
        source_content_sha256="sha",
        source_fetched_at_utc="2025-06-20T15:00:00Z",
        payload=payload,
    )

    round_rows, hole_rows = normalize_event_records(
        event_metadata=event_metadata,
        round_sources=[source],
        event_source_fingerprint="fp",
        run_id="run-tee-est-2",
        silver_processed_at_utc="2026-03-12T10:00:00Z",
    )
    
    rr = round_rows[0]
    assert rr["tee_time_est_method"] == "raw_tee_time"
    assert rr["tee_time_est_ts"] == "2025-06-19T11:30:00"
    assert rr["lag_minutes_used"] == 840
    assert rr["lag_bucket_used"] == "raw"

    hr = hole_rows[0]
    assert hr["tee_time_est_method"] == "raw_tee_time"
    assert hr["tee_time_est_ts"] == "2025-06-19T11:30:00"