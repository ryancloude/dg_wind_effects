from types import SimpleNamespace

import pytest

import silver_pdga_live_results.runner as runner
from silver_pdga_live_results.candidate_reader import LiveResultsStatePointer
from silver_pdga_live_results.planner import IncrementalPlan


def _pointer(event_id: int, division: str, round_number: int, ts: str, key: str, sha: str) -> LiveResultsStatePointer:
    return LiveResultsStatePointer(
        event_id=event_id,
        division=division,
        round_number=round_number,
        fetch_status="success",
        content_sha256=sha,
        last_fetched_at=ts,
        latest_s3_json_key=key,
        latest_s3_meta_key=key.replace(".json", ".meta.json"),
        source_url="https://example.test/live_results",
    )


def _base_config():
    return SimpleNamespace(
        s3_bucket="pdga-bucket",
        ddb_table="pdga-event-index",
        aws_region="us-east-1",
        pipeline_name="LIVE_RESULTS_SILVER",
        silver_player_round_prefix="silver/default/player_round_current",
    )


def _plan():
    pointer_a = _pointer(90008, "MA3", 1, "2026-03-08T11:00:00Z", "bronze/a.json", "sha-a")
    pointer_b = _pointer(90009, "MPO", 2, "2026-03-08T12:00:00Z", "bronze/b.json", "sha-b")
    return IncrementalPlan(
        pipeline_name="LIVE_RESULTS_SILVER",
        checkpoint_fetch_ts=None,
        checkpoint_s3_key=None,
        raw_candidate_count=2,
        deduped_unit_count=2,
        max_candidate_fetch_ts="2026-03-08T12:00:00Z",
        max_candidate_s3_key="bronze/b.json",
        candidate_pointers=(pointer_a, pointer_b),
    )


def test_main_apply_invokes_player_applier(monkeypatch):
    args = SimpleNamespace(
        pipeline_name=None,
        table_name=None,
        aws_region=None,
        allowed_statuses=None,
        candidate_limit=None,
        preview_units=3,
        preview_transform=False,
        preview_transform_units=3,
        apply=True,
        apply_units=5,
        silver_prefix="silver/custom/player_round_current",
        apply_layout_holes=False,
        apply_layout_units=10,
        layout_prefix=None,
        layout_pipeline_name=None,
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(runner, "load_silver_config", _base_config)
    monkeypatch.setattr(runner, "build_incremental_plan", lambda **kwargs: _plan())

    captured_apply = {}

    def fake_apply_player_round_units(**kwargs):
        captured_apply.update(kwargs)
        return {
            "run_id": "silver-run-1",
            "status": "success",
            "summary": {"selected_units": 2, "applied_units": 1, "failed_units": 0},
            "checkpoint_target": {"last_processed_fetch_ts": "2026-03-08T12:00:00Z", "last_processed_s3_key": "bronze/b.json"},
            "units": [{"unit_key": "EVENT#90008#DIV#MA3#ROUND#1", "status": "applied"}],
        }

    monkeypatch.setattr(runner, "apply_player_round_units", fake_apply_player_round_units)

    printed = []
    monkeypatch.setattr(runner, "print", lambda obj: printed.append(obj), raising=False)

    exit_code = runner.main()

    assert exit_code == 0
    assert captured_apply["table_name"] == "pdga-event-index"
    assert captured_apply["pipeline_name"] == "LIVE_RESULTS_SILVER"
    assert captured_apply["bucket"] == "pdga-bucket"
    assert captured_apply["silver_prefix"] == "silver/custom/player_round_current"
    assert captured_apply["max_units"] == 5
    assert len(captured_apply["pointers"]) == 2

    assert len(printed) == 1
    assert "apply_result" in printed[0]
    assert printed[0]["apply_result"]["status"] == "success"


def test_main_apply_layout_invokes_layout_applier(monkeypatch):
    args = SimpleNamespace(
        pipeline_name=None,
        table_name=None,
        aws_region=None,
        allowed_statuses=None,
        candidate_limit=None,
        preview_units=3,
        preview_transform=False,
        preview_transform_units=3,
        apply=False,
        apply_units=5,
        silver_prefix=None,
        apply_layout_holes=True,
        apply_layout_units=7,
        layout_prefix="silver/custom/layout_hole_current",
        layout_pipeline_name="live_results_silver_layout_custom",
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(runner, "load_silver_config", _base_config)
    monkeypatch.setattr(runner, "build_incremental_plan", lambda **kwargs: _plan())

    captured_layout_apply = {}

    def fake_apply_layout_hole_units(**kwargs):
        captured_layout_apply.update(kwargs)
        return {
            "run_id": "layout-run-1",
            "status": "success",
            "summary": {"selected_round_units": 2, "applied_layout_units": 1, "failed_round_units": 0},
            "checkpoint_target": {"last_processed_fetch_ts": "2026-03-08T12:00:00Z", "last_processed_s3_key": "bronze/b.json"},
            "units": [{"layout_id": 712276, "status": "applied"}],
        }

    monkeypatch.setattr(runner, "apply_layout_hole_units", fake_apply_layout_hole_units)

    printed = []
    monkeypatch.setattr(runner, "print", lambda obj: printed.append(obj), raising=False)

    exit_code = runner.main()

    assert exit_code == 0
    assert captured_layout_apply["table_name"] == "pdga-event-index"
    assert captured_layout_apply["pipeline_name"] == "LIVE_RESULTS_SILVER_LAYOUT_CUSTOM"
    assert captured_layout_apply["bucket"] == "pdga-bucket"
    assert captured_layout_apply["silver_prefix"] == "silver/custom/layout_hole_current"
    assert captured_layout_apply["max_units"] == 7
    assert len(captured_layout_apply["pointers"]) == 2

    assert len(printed) == 1
    assert "apply_layout_result" in printed[0]
    assert printed[0]["apply_layout_result"]["status"] == "success"


def test_main_rejects_preview_with_any_apply(monkeypatch):
    args = SimpleNamespace(
        pipeline_name=None,
        table_name=None,
        aws_region=None,
        allowed_statuses=None,
        candidate_limit=None,
        preview_units=3,
        preview_transform=True,
        preview_transform_units=3,
        apply=True,
        apply_units=5,
        silver_prefix=None,
        apply_layout_holes=False,
        apply_layout_units=7,
        layout_prefix=None,
        layout_pipeline_name=None,
        log_level="INFO",
    )
    monkeypatch.setattr(runner, "parse_args", lambda: args)

    with pytest.raises(ValueError, match="preview-only mode or apply mode"):
        runner.main()