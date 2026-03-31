from gold_wind_model_inputs.parquet_io import (
    build_quarantine_key,
    build_round_output_key,
)


def test_build_round_output_key():
    key = build_round_output_key(event_year=2026, event_id=90008)
    assert key == "gold/pdga/wind_effects/model_inputs_round/event_year=2026/tourn_id=90008/model_inputs_round.parquet"


def test_build_quarantine_key():
    key = build_quarantine_key(event_year=2026, event_id=90008, run_id="run-1")
    assert key == "gold/pdga/wind_effects/model_inputs_quarantine/event_year=2026/tourn_id=90008/run_id=run-1/dq_errors.json"