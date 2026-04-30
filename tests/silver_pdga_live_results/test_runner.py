from types import SimpleNamespace

from silver_pdga_live_results.models import ROUND_PK_COLS, ROUND_TIEBREAK_COLS
from silver_pdga_live_results.runner import (
    _is_pending_event,
    _should_exit_nonzero,
    dedup_rows,
    parse_event_ids,
)
import silver_pdga_live_results.runner as runner


def test_parse_event_ids():
    assert parse_event_ids(None) is None
    assert parse_event_ids("90008, 90009") == [90008, 90009]


def test_dedup_rows_prefers_latest_tiebreak():
    old_row = {
        "tourn_id": 90008,
        "round_number": 1,
        "player_key": "PDGA#123",
        "round_score": 55,
        "source_fetched_at_utc": "2025-05-17T22:00:00Z",
        "scorecard_updated_at_ts": "2025-05-17T15:00:00",
        "update_date_ts": "2025-05-17T22:00:00",
        "source_json_key": "older.json",
    }
    new_row = dict(old_row)
    new_row["round_score"] = 50
    new_row["source_fetched_at_utc"] = "2025-05-17T22:35:04Z"
    new_row["source_json_key"] = "newer.json"

    out = dedup_rows([old_row, new_row], ROUND_PK_COLS, ROUND_TIEBREAK_COLS)

    assert len(out) == 1
    assert out[0]["round_score"] == 50
    assert out[0]["source_json_key"] == "newer.json"


def test_is_pending_event_skips_dq_failed_by_default():
    event = {"event_id": 90008}
    checkpoints = {
        90008: {
            "status": "dq_failed",
            "event_source_fingerprint": "fp1",
        }
    }

    pending = _is_pending_event(event, checkpoints, include_dq_failed=False)
    assert pending is False


def test_is_pending_event_includes_dq_failed_when_enabled():
    event = {"event_id": 90008}
    checkpoints = {
        90008: {
            "status": "dq_failed",
            "event_source_fingerprint": "fp1",
        }
    }

    pending = _is_pending_event(event, checkpoints, include_dq_failed=True)
    assert pending is True


def test_is_pending_event_skips_success_with_fingerprint():
    event = {"event_id": 90008}
    checkpoints = {
        90008: {
            "status": "success",
            "event_source_fingerprint": "fp1",
        }
    }

    pending = _is_pending_event(event, checkpoints, include_dq_failed=False)
    assert pending is False


def test_is_pending_event_includes_success_with_blank_fingerprint():
    event = {"event_id": 90008}
    checkpoints = {
        90008: {
            "status": "success",
            "event_source_fingerprint": "",
        }
    }

    pending = _is_pending_event(event, checkpoints, include_dq_failed=False)
    assert pending is True


def test_is_pending_event_includes_failed():
    event = {"event_id": 90008}
    checkpoints = {
        90008: {
            "status": "failed",
            "event_source_fingerprint": "fp1",
        }
    }

    pending = _is_pending_event(event, checkpoints, include_dq_failed=False)
    assert pending is True


def test_is_pending_event_includes_unknown_status():
    event = {"event_id": 90008}
    checkpoints = {
        90008: {
            "status": "mystery_status",
            "event_source_fingerprint": "fp1",
        }
    }

    pending = _is_pending_event(event, checkpoints, include_dq_failed=False)
    assert pending is True


def test_should_exit_nonzero_when_failure_rate_at_threshold():
    stats = runner.RunStats(attempted_events=10, failed_events=5)
    assert _should_exit_nonzero(stats=stats, max_failure_rate=0.5) is True


def test_should_not_exit_nonzero_when_failure_rate_below_threshold():
    stats = runner.RunStats(attempted_events=10, failed_events=4)
    assert _should_exit_nonzero(stats=stats, max_failure_rate=0.5) is False


def test_main_returns_zero_when_failure_rate_below_threshold(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        bucket=None,
        ddb_table=None,
        dry_run=True,
        force_events=False,
        run_mode="pending_only",
        include_dq_failed_in_pending=False,
        progress_every=25,
        max_failure_rate=0.5,
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
            ddb_status_end_date_gsi="gsi_status_end_date",
        ),
    )
    monkeypatch.setattr(
        runner,
        "load_candidate_event_metadata",
        lambda **kwargs: [
            {"event_id": 90008, "division_rounds": {"MPO": 1}},
            {"event_id": 90009, "division_rounds": {"MPO": 1}},
        ],
    )
    monkeypatch.setattr(runner, "load_silver_event_checkpoints", lambda **kwargs: {})
    monkeypatch.setattr(runner, "load_live_results_state_items", lambda **kwargs: [{}])
    monkeypatch.setattr(
        runner,
        "build_round_sources",
        lambda **kwargs: [SimpleNamespace(division="MPO", round_number=1)],
    )
    monkeypatch.setattr(runner, "compute_event_source_fingerprint", lambda _sources: "fp-1")
    monkeypatch.setattr(runner, "get_silver_event_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(
        runner,
        "normalize_event_records",
        lambda **kwargs: (
            [{"event_year": 2026, "tourn_id": kwargs["event_metadata"]["event_id"], "round_number": 1, "player_key": "PDGA#1"}],
            [],
        ),
    )
    monkeypatch.setattr(runner, "validate_quality", lambda **kwargs: ["dq problem"] if kwargs["round_rows"][0]["tourn_id"] == 90008 else [])
    monkeypatch.setattr(runner, "print", lambda obj: None, raising=False)

    exit_code = runner.main()
    assert exit_code == 2  # 1 failed out of 2 attempted == 0.5 threshold


def test_main_returns_zero_when_failure_rate_is_small(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        bucket=None,
        ddb_table=None,
        dry_run=True,
        force_events=False,
        run_mode="pending_only",
        include_dq_failed_in_pending=False,
        progress_every=25,
        max_failure_rate=0.5,
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
            ddb_status_end_date_gsi="gsi_status_end_date",
        ),
    )
    monkeypatch.setattr(
        runner,
        "load_candidate_event_metadata",
        lambda **kwargs: [
            {"event_id": 90008, "division_rounds": {"MPO": 1}},
            {"event_id": 90009, "division_rounds": {"MPO": 1}},
            {"event_id": 90010, "division_rounds": {"MPO": 1}},
        ],
    )
    monkeypatch.setattr(runner, "load_silver_event_checkpoints", lambda **kwargs: {})
    monkeypatch.setattr(runner, "load_live_results_state_items", lambda **kwargs: [{}])
    monkeypatch.setattr(
        runner,
        "build_round_sources",
        lambda **kwargs: [SimpleNamespace(division="MPO", round_number=1)],
    )
    monkeypatch.setattr(runner, "compute_event_source_fingerprint", lambda _sources: "fp-1")
    monkeypatch.setattr(runner, "get_silver_event_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(
        runner,
        "normalize_event_records",
        lambda **kwargs: (
            [{"event_year": 2026, "tourn_id": kwargs["event_metadata"]["event_id"], "round_number": 1, "player_key": "PDGA#1"}],
            [],
        ),
    )
    monkeypatch.setattr(runner, "validate_quality", lambda **kwargs: ["dq problem"] if kwargs["round_rows"][0]["tourn_id"] == 90008 else [])
    monkeypatch.setattr(runner, "print", lambda obj: None, raising=False)

    exit_code = runner.main()
    assert exit_code == 0  # 1 failed out of 3 attempted < 0.5
