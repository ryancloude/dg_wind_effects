from gold_wind_effects.quality import validate_gold_quality


def _round_in():
    return {"tourn_id": 90008, "round_number": 1, "player_key": "P1"}


def _hole_in():
    return {"tourn_id": 90008, "round_number": 1, "hole_number": 1, "player_key": "P1"}


def _round_out():
    return {
        "tourn_id": 90008,
        "round_number": 1,
        "player_key": "P1",
        "gold_grain": "round",
        "gold_model_version": "v1",
        "gold_run_id": "run-1",
        "gold_processed_at_utc": "2026-03-18T12:00:00Z",
        "actual_strokes": 57,
        "strokes_over_par": -3,
        "weather_available_flag": True,
        "wind_speed_bucket": "light",
        "row_hash_sha256": "abc",
    }


def _hole_out():
    return {
        "tourn_id": 90008,
        "round_number": 1,
        "hole_number": 1,
        "player_key": "P1",
        "gold_grain": "hole",
        "gold_model_version": "v1",
        "gold_run_id": "run-1",
        "gold_processed_at_utc": "2026-03-18T12:00:00Z",
        "actual_strokes": 3,
        "strokes_over_par": 0,
        "weather_available_flag": True,
        "wind_speed_bucket": "calm",
        "row_hash_sha256": "def",
    }


def test_validate_gold_quality_happy_path():
    errors = validate_gold_quality(
        round_input_rows=[_round_in()],
        hole_input_rows=[_hole_in()],
        round_output_rows=[_round_out()],
        hole_output_rows=[_hole_out()],
    )
    assert errors == []


def test_validate_gold_quality_flags_missing_cols():
    bad_round = _round_out()
    bad_round.pop("row_hash_sha256")
    errors = validate_gold_quality(
        round_input_rows=[_round_in()],
        hole_input_rows=[_hole_in()],
        round_output_rows=[bad_round],
        hole_output_rows=[_hole_out()],
    )
    assert errors