from types import SimpleNamespace

import silver_pdga_live_results.apply_player_round as applier
from silver_pdga_live_results.candidate_reader import LiveResultsStatePointer
from silver_pdga_live_results.load_state import UnitState


def _pointer(
    *,
    event_id=90008,
    division="MA3",
    round_number=1,
    fetch_ts="2026-03-08T11:00:00Z",
    s3_key="bronze/pdga/live_results/event_id=90008/division=MA3/round=1/fetch_date=2026-03-08/fetch_ts=2026-03-08T11:00:00Z.json",
    sha="source-hash-1",
):
    return LiveResultsStatePointer(
        event_id=event_id,
        division=division,
        round_number=round_number,
        fetch_status="success",
        content_sha256=sha,
        last_fetched_at=fetch_ts,
        latest_s3_json_key=s3_key,
        latest_s3_meta_key=s3_key.replace(".json", ".meta.json"),
        source_url="https://example.test/live_results",
    )


def test_apply_skips_unchanged_and_advances_checkpoint(monkeypatch):
    pointer = _pointer()
    existing_state = UnitState(
        unit_key="EVENT#90008#DIV#MA3#ROUND#1",
        last_applied_sha256="source-hash-1",
        last_applied_fetch_ts="2026-03-08T10:00:00Z",
        last_applied_s3_key="silver/old.jsonl",
        last_applied_row_count=11,
        last_run_id="run-old",
        updated_at="2026-03-08T10:05:00Z",
    )

    monkeypatch.setattr(applier, "get_round_unit_state", lambda **kwargs: existing_state)
    monkeypatch.setattr(
        applier,
        "load_payload_for_pointer",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not load payload for unchanged unit")),
    )
    monkeypatch.setattr(applier, "put_global_checkpoint", lambda **kwargs: True)

    summary_calls = []
    monkeypatch.setattr(applier, "put_run_summary", lambda **kwargs: summary_calls.append(kwargs))

    out = applier.apply_player_round_units(
        table_name="pdga-event-index",
        pipeline_name="LIVE_RESULTS_SILVER",
        bucket="pdga-bucket",
        silver_prefix="silver/pdga/live_results/player_round_current",
        pointers=[pointer],
        run_id="run-1",
        aws_region="us-east-1",
    )

    assert out["status"] == "success"
    assert out["summary"]["selected_units"] == 1
    assert out["summary"]["skipped_unchanged_units"] == 1
    assert out["summary"]["applied_units"] == 0
    assert out["summary"]["checkpoint_advanced"] == 1
    assert summary_calls[0]["status"] == "success"


def test_apply_changed_unit_writes_and_updates_state(monkeypatch):
    pointer = _pointer()

    monkeypatch.setattr(applier, "get_round_unit_state", lambda **kwargs: None)
    monkeypatch.setattr(applier, "load_payload_for_pointer", lambda **kwargs: SimpleNamespace(payload={"data": {"scores": []}}))
    monkeypatch.setattr(
        applier,
        "transform_player_round_rows",
        lambda **kwargs: (
            [{"event_id": 90008, "division_code": "MA3", "round_number": 1, "result_id": 1, "layout_id": 10}],
            {"total_scores": 1, "output_rows": 1, "skipped_non_object_scores": 0, "skipped_missing_result_id": 0},
        ),
    )
    monkeypatch.setattr(
        applier,
        "put_player_round_current",
        lambda **kwargs: {
            "s3_rows_key": "silver/pdga/live_results/player_round_current/event_id=90008/division=MA3/round=1/source_fetch_date=2026-03-08/source_fetch_ts=2026-03-08T11:00:00Z.jsonl",
            "s3_meta_key": "silver/meta.json",
            "row_count": 1,
        },
    )
    monkeypatch.setattr(applier, "put_round_unit_state", lambda **kwargs: True)
    monkeypatch.setattr(applier, "put_global_checkpoint", lambda **kwargs: True)

    summary_calls = []
    monkeypatch.setattr(applier, "put_run_summary", lambda **kwargs: summary_calls.append(kwargs))

    out = applier.apply_player_round_units(
        table_name="pdga-event-index",
        pipeline_name="LIVE_RESULTS_SILVER",
        bucket="pdga-bucket",
        silver_prefix="silver/pdga/live_results/player_round_current",
        pointers=[pointer],
        run_id="run-2",
        aws_region="us-east-1",
    )

    assert out["status"] == "success"
    assert out["summary"]["applied_units"] == 1
    assert out["summary"]["rows_written"] == 1
    assert out["summary"]["failed_units"] == 0
    assert out["summary"]["checkpoint_advanced"] == 1
    assert out["units"][0]["status"] == "applied"
    assert summary_calls[0]["status"] == "success"


def test_apply_failure_does_not_advance_checkpoint(monkeypatch):
    pointer = _pointer()

    monkeypatch.setattr(applier, "get_round_unit_state", lambda **kwargs: None)
    monkeypatch.setattr(
        applier,
        "load_payload_for_pointer",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    checkpoint_calls = []
    monkeypatch.setattr(applier, "put_global_checkpoint", lambda **kwargs: checkpoint_calls.append(kwargs))

    summary_calls = []
    monkeypatch.setattr(applier, "put_run_summary", lambda **kwargs: summary_calls.append(kwargs))

    out = applier.apply_player_round_units(
        table_name="pdga-event-index",
        pipeline_name="LIVE_RESULTS_SILVER",
        bucket="pdga-bucket",
        silver_prefix="silver/pdga/live_results/player_round_current",
        pointers=[pointer],
        run_id="run-3",
        aws_region="us-east-1",
    )

    assert out["status"] == "failed"
    assert out["summary"]["failed_units"] == 1
    assert out["summary"]["checkpoint_advanced"] == 0
    assert checkpoint_calls == []
    assert summary_calls[0]["status"] == "failed"