from gold_wind_effects.transform import (
    build_hole_features,
    build_round_features,
    compute_gold_event_fingerprint,
)


def test_build_round_features_adds_gold_columns():
    rows = [
        {
            "tourn_id": 90008,
            "round_number": 1,
            "player_key": "P1",
            "round_score": 57,
            "round_to_par": -3,
            "wx_wind_speed_mps": 4.2,
            "wx_weather_missing_flag": False,
            "source_content_sha256": "abc",
        }
    ]

    out = build_round_features(rows, run_id="run-1", processed_at_utc="2026-03-18T12:00:00Z")
    assert len(out) == 1
    assert out[0]["gold_grain"] == "round"
    assert out[0]["actual_strokes"] == 57
    assert out[0]["strokes_over_par"] == -3
    assert out[0]["weather_available_flag"] is True
    assert out[0]["wind_speed_bucket"] == "light"
    assert out[0]["row_hash_sha256"]


def test_build_hole_features_adds_gold_columns():
    rows = [
        {
            "tourn_id": 90008,
            "round_number": 1,
            "hole_number": 3,
            "player_key": "P1",
            "hole_score": 4,
            "hole_par": 3,
            "wx_wind_speed_mps": 8.4,
            "wx_weather_missing_flag": False,
            "source_content_sha256": "def",
        }
    ]

    out = build_hole_features(rows, run_id="run-1", processed_at_utc="2026-03-18T12:00:00Z")
    assert len(out) == 1
    assert out[0]["gold_grain"] == "hole"
    assert out[0]["actual_strokes"] == 4
    assert out[0]["strokes_over_par"] == 1
    assert out[0]["wind_speed_bucket"] == "strong"
    assert out[0]["row_hash_sha256"]


def test_compute_gold_event_fingerprint_deterministic():
    round_rows = [{"tourn_id": 1, "round_number": 1, "player_key": "P1", "round_score": 55}]
    hole_rows = [{"tourn_id": 1, "round_number": 1, "hole_number": 1, "player_key": "P1", "hole_score": 3}]

    fp1 = compute_gold_event_fingerprint(round_rows=round_rows, hole_rows=hole_rows)
    fp2 = compute_gold_event_fingerprint(
        round_rows=list(reversed(round_rows)),
        hole_rows=list(reversed(hole_rows)),
    )
    assert fp1 == fp2