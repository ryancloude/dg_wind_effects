from types import SimpleNamespace

import gold_wind_model_inputs.runner as runner
from gold_wind_model_inputs.dynamo_io import ModelInputsEventCandidate


def _candidate():
    return ModelInputsEventCandidate(
        event_id=90008,
        event_year=2026,
        hole_s3_key="hole.parquet",
        round_s3_key="round.parquet",
    )


def test_main_pending_only_skips_success_with_same_fingerprint(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        bucket=None,
        ddb_table=None,
        dry_run=True,
        force_events=False,
        run_mode="pending_only",
        include_dq_failed_in_pending=False,
        progress_every=10,
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(s3_bucket="bucket", ddb_table="table", aws_region="us-east-1"),
    )
    monkeypatch.setattr(runner, "load_model_inputs_event_candidates", lambda **kwargs: [_candidate()])
    monkeypatch.setattr(
        runner,
        "load_model_inputs_event_checkpoints",
        lambda **kwargs: {90008: {"status": "success", "event_source_fingerprint": "fp-1"}},
    )
    monkeypatch.setattr(
        runner,
        "load_event_input_tables",
        lambda **kwargs: (
            [{"tourn_id": 90008, "round_number": 1, "hole_number": 1, "player_key": "P1"}],
            [{"tourn_id": 90008, "round_number": 1, "player_key": "P1"}],
        ),
    )
    monkeypatch.setattr(runner, "compute_model_inputs_event_fingerprint", lambda **kwargs: "fp-1")
    monkeypatch.setattr(
        runner,
        "get_model_inputs_event_checkpoint",
        lambda **kwargs: {"status": "success", "event_source_fingerprint": "fp-1"},
    )

    exit_code = runner.main()
    assert exit_code == 0


def test_main_full_check_processes_success(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        bucket=None,
        ddb_table=None,
        dry_run=False,
        force_events=False,
        run_mode="full_check",
        include_dq_failed_in_pending=False,
        progress_every=10,
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(s3_bucket="bucket", ddb_table="table", aws_region="us-east-1"),
    )
    monkeypatch.setattr(runner, "load_model_inputs_event_candidates", lambda **kwargs: [_candidate()])
    monkeypatch.setattr(
        runner,
        "load_event_input_tables",
        lambda **kwargs: (
            [{"tourn_id": 90008, "round_number": 1, "hole_number": 1, "player_key": "P1", "hole_score": 3, "hole_par": 3}],
            [{"tourn_id": 90008, "round_number": 1, "player_key": "P1", "round_score": 57, "layout_par": 60}],
        ),
    )
    monkeypatch.setattr(runner, "compute_model_inputs_event_fingerprint", lambda **kwargs: "fp-new")
    monkeypatch.setattr(runner, "get_model_inputs_event_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(runner, "build_hole_model_inputs", lambda rows, **kwargs: [dict(rows[0], row_hash_sha256="hole-hash", target_strokes_over_par=0, model_inputs_grain="hole", model_inputs_version="v1", weather_available_flag=True, event_year=2026)])
    monkeypatch.setattr(runner, "build_round_model_inputs", lambda rows, **kwargs: [dict(rows[0], row_hash_sha256="round-hash", target_strokes_over_par=-3, model_inputs_grain="round", model_inputs_version="v1", weather_available_flag=True, event_year=2026)])
    monkeypatch.setattr(runner, "validate_model_inputs_quality", lambda **kwargs: [])
    monkeypatch.setattr(runner, "overwrite_event_tables", lambda **kwargs: {"hole_key": "hk", "round_key": "rk"})

    checkpoint_calls = []
    run_summary_calls = []

    monkeypatch.setattr(runner, "put_model_inputs_event_checkpoint", lambda **kwargs: checkpoint_calls.append(kwargs))
    monkeypatch.setattr(runner, "put_model_inputs_run_summary", lambda **kwargs: run_summary_calls.append(kwargs))

    exit_code = runner.main()
    assert exit_code == 0
    assert len(checkpoint_calls) == 1
    assert checkpoint_calls[0]["status"] == "success"
    assert len(run_summary_calls) == 1