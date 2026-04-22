from types import SimpleNamespace

import pandas as pd

import score_round_wind_model.runner as runner
from score_round_wind_model.scoring import ScoringResult


class FakeModel:
    pass


def test_runner_skips_existing_success(monkeypatch):
    args = SimpleNamespace(
        training_request_fingerprint="train-fp",
        event_ids=None,
        bucket=None,
        ddb_table=None,
        dry_run=True,
        force_events=False,
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(s3_bucket="bucket", ddb_table="table", aws_region="us-east-1"),
    )
    monkeypatch.setattr(
        runner,
        "load_model_bundle",
        lambda **kwargs: {
            "artifact_prefix": "artifacts/prefix/",
            "model": FakeModel(),
            "training_manifest": {},
            "feature_columns": [],
            "categorical_feature_columns": [],
        },
    )
    monkeypatch.setattr(
        runner,
        "list_model_input_round_objects",
        lambda **kwargs: [{"key": "gold/pdga/wind_effects/model_inputs_round/event_year=2026/tourn_id=90008/model_inputs_round.parquet", "etag": "e1", "size": 1, "last_modified": "x"}],
    )
    monkeypatch.setattr(runner, "compute_scoring_request_fingerprint", lambda **kwargs: "score-fp")
    monkeypatch.setattr(
        runner,
        "get_score_checkpoint",
        lambda **kwargs: {"status": "success", "scoring_request_fingerprint": "score-fp"},
    )

    assert runner.main() == 0


def test_runner_scores_and_writes(monkeypatch):
    args = SimpleNamespace(
        training_request_fingerprint="train-fp",
        event_ids=None,
        bucket=None,
        ddb_table=None,
        dry_run=False,
        force_events=False,
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(s3_bucket="bucket", ddb_table="table", aws_region="us-east-1"),
    )
    monkeypatch.setattr(
        runner,
        "load_model_bundle",
        lambda **kwargs: {
            "artifact_prefix": "artifacts/prefix/",
            "model": FakeModel(),
            "training_manifest": {"model_name": "round_one_stage_catboost_monotone", "model_version": "v4"},
            "feature_columns": [],
            "categorical_feature_columns": [],
        },
    )
    monkeypatch.setattr(
        runner,
        "list_model_input_round_objects",
        lambda **kwargs: [{"key": "gold/pdga/wind_effects/model_inputs_round/event_year=2026/tourn_id=90008/model_inputs_round.parquet", "etag": "e1", "size": 1, "last_modified": "x"}],
    )
    monkeypatch.setattr(runner, "compute_scoring_request_fingerprint", lambda **kwargs: "score-fp")
    monkeypatch.setattr(runner, "get_score_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(
        runner,
        "load_event_dataframe",
        lambda **kwargs: pd.DataFrame([{"event_year": 2026, "tourn_id": 90008}]),
    )
    monkeypatch.setattr(
        runner,
        "score_round_rows",
        lambda **kwargs: ScoringResult(
            scored_df=pd.DataFrame([{"event_year": 2026, "tourn_id": 90008, "round_number": 1, "player_key": "P1"}]),
            scoring_manifest={"model_name": "round_one_stage_catboost_monotone", "model_version": "v4"},
        ),
    )
    monkeypatch.setattr(runner, "overwrite_event_scored_rounds", lambda **kwargs: "scored_rounds.parquet")

    checkpoint_calls = []
    summary_calls = []

    monkeypatch.setattr(runner, "put_score_checkpoint", lambda **kwargs: checkpoint_calls.append(kwargs))
    monkeypatch.setattr(runner, "put_score_run_summary", lambda **kwargs: summary_calls.append(kwargs))

    assert runner.main() == 0
    assert len(checkpoint_calls) == 1
    assert len(summary_calls) == 1
