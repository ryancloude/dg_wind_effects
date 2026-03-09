import silver_pdga_live_results.planner as planner
from silver_pdga_live_results.candidate_reader import LiveResultsStatePointer
from silver_pdga_live_results.load_state import GlobalCheckpoint


def _pointer(event_id, division, round_number, ts, key, sha):
    return LiveResultsStatePointer(
        event_id=event_id,
        division=division,
        round_number=round_number,
        fetch_status="success",
        content_sha256=sha,
        last_fetched_at=ts,
        latest_s3_json_key=key,
        latest_s3_meta_key=key.replace(".json", ".meta.json"),
        source_url="https://example.test",
    )


def test_build_incremental_plan_uses_checkpoint_and_dedupes(monkeypatch):
    monkeypatch.setattr(
        planner,
        "get_global_checkpoint",
        lambda **kwargs: GlobalCheckpoint(
            last_processed_fetch_ts="2026-03-08T09:00:00Z",
            last_processed_s3_key="bronze/...09.json",
            last_run_id="run-1",
            updated_at="2026-03-08T09:05:00Z",
        ),
    )

    raw_candidates = [
        _pointer(90008, "MA3", 1, "2026-03-08T10:00:00Z", "bronze/...10a.json", "old"),
        _pointer(90008, "MA3", 1, "2026-03-08T11:00:00Z", "bronze/...11a.json", "new"),
        _pointer(90009, "MPO", 2, "2026-03-08T12:00:00Z", "bronze/...12a.json", "x"),
    ]
    monkeypatch.setattr(planner, "collect_live_results_state_pointers", lambda **kwargs: raw_candidates)

    plan = planner.build_incremental_plan(
        table_name="pdga-event-index",
        pipeline_name="live_results_silver",
        aws_region="us-east-1",
    )

    assert plan.raw_candidate_count == 3
    assert plan.deduped_unit_count == 2
    assert plan.max_candidate_fetch_ts == "2026-03-08T12:00:00Z"
    assert plan.max_candidate_s3_key == "bronze/...12a.json"

    payload = plan.to_dict(preview_units=10)
    assert payload["raw_candidate_count"] == 3
    assert payload["deduped_unit_count"] == 2
    assert len(payload["preview_units"]) == 2