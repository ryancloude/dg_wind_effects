from gold_wind_effects.parquet_io import (
    build_hole_output_key,
    build_quarantine_key,
    build_round_output_key,
)


def test_round_key():
    key = build_round_output_key(event_year=2026, event_id=90008)
    assert key == "gold/pdga/wind_effects/player_rounds_features/event_year=2026/tourn_id=90008/player_rounds_features.parquet"


def test_hole_key():
    key = build_hole_output_key(event_year=2026, event_id=90008)
    assert key == "gold/pdga/wind_effects/player_holes_features/event_year=2026/tourn_id=90008/player_holes_features.parquet"


def test_quarantine_key():
    key = build_quarantine_key(event_year=2026, event_id=90008, run_id="run-1")
    assert key == "gold/pdga/wind_effects/quarantine/event_year=2026/tourn_id=90008/run_id=run-1/dq_errors.json"