from silver_pdga_live_results.quality import validate_quality


def _base_round_row():
    return {
        "tourn_id": 90008,
        "round_number": 1,
        "player_key": "PDGA#123",
        "division": "MA3",
        "source_json_key": "k1.json",
        "source_content_sha256": "abc",
        "source_fetched_at_utc": "2025-05-17T22:35:04Z",
        "silver_run_id": "run-1",
    }


def _base_hole_row():
    return {
        "tourn_id": 90008,
        "round_number": 1,
        "hole_number": 1,
        "player_key": "PDGA#123",
        "division": "MA3",
        "layout_holes": 18,
        "source_json_key": "k1.json",
        "source_content_sha256": "abc",
        "source_fetched_at_utc": "2025-05-17T22:35:04Z",
        "silver_run_id": "run-1",
    }


def test_quality_detects_duplicate_round_keys():
    row = _base_round_row()
    errors = validate_quality(round_rows=[row, dict(row)], hole_rows=[_base_hole_row()])
    assert any("duplicate player_rounds keys" in err for err in errors)


def test_quality_detects_orphan_hole_rows():
    hole = _base_hole_row()
    hole["player_key"] = "PDGA#999"
    errors = validate_quality(round_rows=[_base_round_row()], hole_rows=[hole])
    assert any("player_holes parent missing" in err for err in errors)


def test_quality_detects_division_collisions():
    r1 = _base_round_row()
    r2 = dict(r1)
    r2["division"] = "MPO"
    errors = validate_quality(round_rows=[r1, r2], hole_rows=[])
    assert any("division collision" in err for err in errors)