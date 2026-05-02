from types import SimpleNamespace

import pandas as pd

import score_round_wind_model.runner as runner
from score_round_wind_model.scoring import ScoringResult


class FakeModel:
    pass


def test_should_skip_failed_checkpoint_by_default():
    should_skip, reason = runner._should_skip_event(
        checkpoint={"status": "failed", "scoring_request_fingerprint": "score-fp"},
        scoring_request_fingerprint="score-fp",
        force_events=False,
        include_failed=False,
    )
    assert should_skip is True
    assert reason == "previous_failed"


def test_should_include_failed_checkpoint_when_enabled():
    should_skip, reason = runner._should_skip_event(
        checkpoint={"status": "failed", "scoring_request_fingerprint": "score-fp"},
        scoring_request_fingerprint="score-fp",
        force_events=False,
        include_failed=True,
    )
    assert should_skip is False
    assert reason == ""


def test_runner_skips_existing_success(monkeypatch):
    args = SimpleNamespace(
        training_request_fingerprint="train-fp",
        event_ids=None,
        bucket=None,
        ddb_table=None,
        dry_run=True,
        force_events=False,
        include_failed_events=False,
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(
            s3_bucket="bucket",
            ddb_table="table",
            aws_region="us-east-1",
            athena_database="pdga_analytics",
            athena_workgroup="pdga-analytics",
            athena_results_s3_uri="s3://athena-results/query-results/",
            athena_source_scored_table="scored_rounds",
        ),
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
        lambda **kwargs: [{
            "key": "gold/pdga/wind_effects/model_inputs_round/event_year=2026/tourn_id=90008/model_inputs_round.parquet",
            "etag": "e1",
            "size": 1,
            "last_modified": "x",
        }],
    )
    monkeypatch.setattr(runner, "compute_scoring_request_fingerprint", lambda **kwargs: "score-fp")
    monkeypatch.setattr(
        runner,
        "get_score_checkpoint",
        lambda **kwargs: {"status": "success", "scoring_request_fingerprint": "score-fp"},
    )

    assert runner.main() == 0


def test_runner_scores_writes_and_registers_partition(monkeypatch):
    args = SimpleNamespace(
        training_request_fingerprint="train-fp",
        event_ids=None,
        bucket=None,
        ddb_table=None,
        dry_run=False,
        force_events=False,
        include_failed_events=False,
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(
            s3_bucket="bucket",
            ddb_table="table",
            aws_region="us-east-1",
            athena_database="pdga_analytics",
            athena_workgroup="pdga-analytics",
            athena_results_s3_uri="s3://athena-results/query-results/",
            athena_source_scored_table="scored_rounds",
        ),
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
        lambda **kwargs: [{
            "key": "gold/pdga/wind_effects/model_inputs_round/event_year=2026/tourn_id=90008/model_inputs_round.parquet",
            "etag": "e1",
            "size": 1,
            "last_modified": "x",
        }],
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
    monkeypatch.setattr(
        runner,
        "build_scored_round_partition_location",
        lambda **kwargs: "s3://bucket/gold/pdga/wind_effects/scored_rounds/event_year=2026/tourn_id=90008/",
    )
    monkeypatch.setattr(
        runner,
        "register_scored_round_partition",
        lambda **kwargs: {"query_execution_id": "qe-123"},
    )

    checkpoint_calls = []
    summary_calls = []

    monkeypatch.setattr(runner, "put_score_checkpoint", lambda **kwargs: checkpoint_calls.append(kwargs))
    monkeypatch.setattr(runner, "put_score_run_summary", lambda **kwargs: summary_calls.append(kwargs))

    assert runner.main() == 0
    assert len(checkpoint_calls) == 1
    assert len(summary_calls) == 1
    assert checkpoint_calls[0]["extra_attributes"]["athena_partition_location"] == (
        "s3://bucket/gold/pdga/wind_effects/scored_rounds/event_year=2026/tourn_id=90008/"
    )
    assert checkpoint_calls[0]["extra_attributes"]["athena_partition_query_execution_id"] == "qe-123"
    assert summary_calls[0]["stats"]["partitions_registered"] == 1


def test_should_exit_nonzero_when_failure_rate_at_threshold():
    stats = runner.RunStats(attempted_events=10, failed_events=5)
    assert runner._should_exit_nonzero(stats=stats, max_failure_rate=0.5) is True


def test_should_not_exit_nonzero_when_failure_rate_below_threshold():
    stats = runner.RunStats(attempted_events=10, failed_events=4)
    assert runner._should_exit_nonzero(stats=stats, max_failure_rate=0.5) is False
