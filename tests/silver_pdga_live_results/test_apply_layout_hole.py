from types import SimpleNamespace

import silver_pdga_live_results.apply_layout_hole as applier
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


def test_apply_layout_hole_skips_unchanged(monkeypatch):
    pointer = _pointer()
    rows = [{"layout_id": 712276, "hole_ordinal": 1, "layout_row_hash": "h1"}]

    monkeypatch.setattr(applier, "load_payload_for_pointer", lambda **kwargs: SimpleNamespace(payload={"data": {}}))
    monkeypatch.setattr(applier, "transform_layout_hole_rows", lambda **kwargs: (rows, {"total_layouts": 1, "total_holes": 1}))
    monkeypatch.setattr(applier, "group_rows_by_layout", lambda _rows: {712276: _rows})
    monkeypatch.setattr(applier, "compute_layout_group_hash", lambda _rows: "layout-sha")

    existing_state = UnitState(
        unit_key="LAYOUT#712276",
        last_applied_sha256="layout-sha",
        last_applied_fetch_ts="2026-03-08T10:00:00Z",
        last_applied_s3_key="silver/layout_old.jsonl",
        last_applied_row_count=18,
        last_run_id="run-old",
        updated_at="2026-03-08T10:05:00Z",
    )
    monkeypatch.setattr(applier, "get_round_unit_state", lambda **kwargs: existing_state)
    monkeypatch.setattr(
        applier,
        "put_layout_hole_current",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not write unchanged layout")),
    )
    monkeypatch.setattr(applier, "put_global_checkpoint", lambda **kwargs: True)

    summary_calls = []
    monkeypatch.setattr(applier, "put_run_summary", lambda **kwargs: summary_calls.append(kwargs))

    out = applier.apply_layout_hole_units(
        table_name="pdga-event-index",
        pipeline_name="LIVE_RESULTS_SILVER_LAYOUT_HOLE",
        bucket="pdga-bucket",
        silver_prefix="silver/pdga/live_results/layout_hole_current",
        pointers=[pointer],
        run_id="layout-run-1",
        aws_region="us-east-1",
    )

    assert out["status"] == "success"
    assert out["summary"]["selected_round_units"] == 1
    assert out["summary"]["applied_layout_units"] == 0
    assert out["summary"]["skipped_unchanged_layout_units"] == 1
    assert out["summary"]["checkpoint_advanced"] == 1
    assert summary_calls[0]["status"] == "success"


def test_apply_layout_hole_changed_applies(monkeypatch):
    pointer = _pointer()
    rows = [
        {"layout_id": 712276, "hole_ordinal": 1, "layout_row_hash": "h1"},
        {"layout_id": 712276, "hole_ordinal": 2, "layout_row_hash": "h2"},
    ]

    monkeypatch.setattr(applier, "load_payload_for_pointer", lambda **kwargs: SimpleNamespace(payload={"data": {}}))
    monkeypatch.setattr(applier, "transform_layout_hole_rows", lambda **kwargs: (rows, {"total_layouts": 1, "total_holes": 2}))
    monkeypatch.setattr(applier, "group_rows_by_layout", lambda _rows: {712276: _rows})
    monkeypatch.setattr(applier, "compute_layout_group_hash", lambda _rows: "layout-sha-new")
    monkeypatch.setattr(applier, "get_round_unit_state", lambda **kwargs: None)
    monkeypatch.setattr(
        applier,
        "put_layout_hole_current",
        lambda **kwargs: {"s3_rows_key": "silver/layout_new.jsonl", "s3_meta_key": "silver/layout_new.meta.json", "row_count": 2},
    )
    monkeypatch.setattr(applier, "put_round_unit_state", lambda **kwargs: True)
    monkeypatch.setattr(applier, "put_global_checkpoint", lambda **kwargs: True)

    summary_calls = []
    monkeypatch.setattr(applier, "put_run_summary", lambda **kwargs: summary_calls.append(kwargs))

    out = applier.apply_layout_hole_units(
        table_name="pdga-event-index",
        pipeline_name="LIVE_RESULTS_SILVER_LAYOUT_HOLE",
        bucket="pdga-bucket",
        silver_prefix="silver/pdga/live_results/layout_hole_current",
        pointers=[pointer],
        run_id="layout-run-2",
        aws_region="us-east-1",
    )

    assert out["status"] == "success"
    assert out["summary"]["applied_layout_units"] == 1
    assert out["summary"]["rows_written"] == 2
    assert out["summary"]["failed_round_units"] == 0
    assert out["summary"]["checkpoint_advanced"] == 1
    assert out["units"][0]["status"] == "applied"
    assert summary_calls[0]["status"] == "success"


def test_apply_layout_hole_failure_does_not_advance_checkpoint(monkeypatch):
    pointer = _pointer()

    monkeypatch.setattr(
        applier,
        "load_payload_for_pointer",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    checkpoint_calls = []
    monkeypatch.setattr(applier, "put_global_checkpoint", lambda **kwargs: checkpoint_calls.append(kwargs))

    summary_calls = []
    monkeypatch.setattr(applier, "put_run_summary", lambda **kwargs: summary_calls.append(kwargs))

    out = applier.apply_layout_hole_units(
        table_name="pdga-event-index",
        pipeline_name="LIVE_RESULTS_SILVER_LAYOUT_HOLE",
        bucket="pdga-bucket",
        silver_prefix="silver/pdga/live_results/layout_hole_current",
        pointers=[pointer],
        run_id="layout-run-3",
        aws_region="us-east-1",
    )

    assert out["status"] == "failed"
    assert out["summary"]["failed_round_units"] == 1
    assert out["summary"]["checkpoint_advanced"] == 0
    assert checkpoint_calls == []
    assert summary_calls[0]["status"] == "failed"