from types import SimpleNamespace

import gold_wind_effects.runner as runner
from gold_wind_effects.dynamo_io import GoldEventCandidate


def _candidate():
    return GoldEventCandidate(
        event_id=90008,
        event_year=2026,
        round_s3_key="r.parquet",
        hole_s3_key="h.parquet",
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
    monkeypatch.setattr(runner, "load_gold_event_candidates", lambda **kwargs: [_candidate()])
    monkeypatch.setattr(
        runner,
        "load_gold_event_checkpoints",
        lambda **kwargs: {90008: {"status": "success", "event_source_fingerprint": "fp-1"}},
    )
    monkeypatch.setattr(
        runner,
        "load_event_input_tables",
        lambda **kwargs: (
            [{"tourn_id": 90008, "round_number": 1, "player_key": "P1"}],
            [{"tourn_id": 90008, "round_number": 1, "hole_number": 1, "player_key": "P1"}],
        ),
    )
    monkeypatch.setattr(runner, "compute_gold_event_fingerprint", lambda **kwargs: "fp-1")
    monkeypatch.setattr(
        runner,
        "get_gold_event_checkpoint",
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
    monkeypatch.setattr(runner, "load_gold_event_candidates", lambda **kwargs: [_candidate()])
    monkeypatch.setattr(
        runner,
        "load_event_input_tables",
        lambda **kwargs: (
            [{"tourn_id": 90008, "round_number": 1, "player_key": "P1", "round_score": 57}],
            [{"tourn_id": 90008, "round_number": 1, "hole_number": 1, "player_key": "P1", "hole_score": 3}],
        ),
    )
    monkeypatch.setattr(runner, "compute_gold_event_fingerprint", lambda **kwargs: "fp-new")
    monkeypatch.setattr(runner, "get_gold_event_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(runner, "build_round_features", lambda rows, **kwargs: [dict(rows[0], row_hash_sha256="r")])
    monkeypatch.setattr(runner, "build_hole_features", lambda rows, **kwargs: [dict(rows[0], row_hash_sha256="h")])
    monkeypatch.setattr(runner, "validate_gold_quality", lambda **kwargs: [])
    monkeypatch.setattr(runner, "overwrite_event_tables", lambda **kwargs: {"round_key": "rk", "hole_key": "hk"})

    checkpoint_calls = []
    run_summary_calls = []
    monkeypatch.setattr(runner, "put_gold_event_checkpoint", lambda **kwargs: checkpoint_calls.append(kwargs))
    monkeypatch.setattr(runner, "put_gold_run_summary", lambda **kwargs: run_summary_calls.append(kwargs))

    exit_code = runner.main()
    assert exit_code == 0
    assert len(checkpoint_calls) == 1
    assert checkpoint_calls[0]["status"] == "success"
    assert len(run_summary_calls) == 1