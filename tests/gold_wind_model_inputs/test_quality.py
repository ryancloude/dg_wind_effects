from gold_wind_model_inputs.quality import validate_model_inputs_quality


def _hole_input():
    return {
        "tourn_id": 90008,
        "round_number": 1,
        "hole_number": 1,
        "player_key": "P1",
    }


def _hole_output():
    return {
        "event_year": 2026,
        "tourn_id": 90008,
        "round_number": 1,
        "hole_number": 1,
        "player_key": "P1",
        "model_inputs_grain": "hole",
        "model_inputs_version": "v1",
        "target_strokes_over_par": 0,
        "weather_available_flag": True,
        "row_hash_sha256": "abc",
    }


def _round_input():
    return {
        "tourn_id": 90008,
        "round_number": 1,
        "player_key": "P1",
    }


def _round_output():
    return {
        "event_year": 2026,
        "tourn_id": 90008,
        "round_number": 1,
        "player_key": "P1",
        "model_inputs_grain": "round",
        "model_inputs_version": "v1",
        "target_strokes_over_par": -3,
        "weather_available_flag": True,
        "row_hash_sha256": "def",
    }


def test_validate_model_inputs_quality_happy_path():
    errors = validate_model_inputs_quality(
        hole_input_rows=[_hole_input()],
        hole_output_rows=[_hole_output()],
        round_input_rows=[_round_input()],
        round_output_rows=[_round_output()],
    )
    assert errors == []


def test_validate_model_inputs_quality_missing_required_column():
    bad = _hole_output()
    bad.pop("row_hash_sha256")

    errors = validate_model_inputs_quality(
        hole_input_rows=[_hole_input()],
        hole_output_rows=[bad],
    )
    assert errors
    assert any(e["rule"] in ("columns:hole_required", "not_null:row_hash_sha256:hole") for e in errors)


def test_validate_model_inputs_quality_duplicate_hole_pk():
    inp = [_hole_input(), _hole_input()]
    out = [_hole_output(), _hole_output()]

    errors = validate_model_inputs_quality(
        hole_input_rows=inp,
        hole_output_rows=out,
    )
    assert any(e["rule"] == "uniqueness:hole_pk" for e in errors)


def test_validate_model_inputs_quality_null_target():
    bad = _hole_output()
    bad["target_strokes_over_par"] = None

    errors = validate_model_inputs_quality(
        hole_input_rows=[_hole_input()],
        hole_output_rows=[bad],
    )
    assert any(e["rule"] == "not_null:hole_target_strokes_over_par" for e in errors)