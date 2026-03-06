from types import SimpleNamespace

import ingest_pdga_live_results.runner as runner
from ingest_pdga_live_results.dynamo_reader import LiveResultsTask
from ingest_pdga_live_results.response_handler import compute_payload_sha256


def test_process_task_unchanged_skips_s3_write(monkeypatch):
    task = LiveResultsTask(event_id="86076", division="MP40", round_number=1)
    payload = {"results": [{"player": "A"}]}
    payload_hash = compute_payload_sha256(payload)

    monkeypatch.setattr(
        runner,
        "get_live_results_json",
        lambda session, cfg, task: (200, payload, "https://example.test"),
    )
    monkeypatch.setattr(
        runner,
        "get_existing_live_results_sha256",
        lambda **kwargs: payload_hash,
    )

    called = {"s3": 0, "upsert": 0}

    def fake_put_live_results_raw(**kwargs):
        called["s3"] += 1
        return {"s3_json_key": "k1", "s3_meta_key": "k2", "fetched_at": "2026-03-05T00:00:00Z"}

    def fake_upsert(**kwargs):
        called["upsert"] += 1
        return {}

    monkeypatch.setattr(runner, "put_live_results_raw", fake_put_live_results_raw)
    monkeypatch.setattr(runner, "upsert_live_results_state", fake_upsert)

    result = runner.process_task(
        task=task,
        bucket="bucket",
        dry_run=False,
        app_cfg=SimpleNamespace(ddb_table="tbl", aws_region="us-east-1"),
        session=object(),
        http_cfg=SimpleNamespace(timeout_s=30),
        run_id="run-1",
    )

    assert result.classification == "success"
    assert result.unchanged is True
    assert result.changed is False
    assert called["s3"] == 0
    assert called["upsert"] == 1


def test_process_task_changed_writes_s3(monkeypatch):
    task = LiveResultsTask(event_id="86076", division="MP40", round_number=1)
    payload = {"results": [{"player": "A"}]}

    monkeypatch.setattr(
        runner,
        "get_live_results_json",
        lambda session, cfg, task: (200, payload, "https://example.test"),
    )
    monkeypatch.setattr(
        runner,
        "get_existing_live_results_sha256",
        lambda **kwargs: None,
    )

    called = {"s3": 0, "upsert": 0}

    def fake_put_live_results_raw(**kwargs):
        called["s3"] += 1
        return {"s3_json_key": "k1", "s3_meta_key": "k2", "fetched_at": "2026-03-05T00:00:00Z"}

    def fake_upsert(**kwargs):
        called["upsert"] += 1
        return {}

    monkeypatch.setattr(runner, "put_live_results_raw", fake_put_live_results_raw)
    monkeypatch.setattr(runner, "upsert_live_results_state", fake_upsert)

    result = runner.process_task(
        task=task,
        bucket="bucket",
        dry_run=False,
        app_cfg=SimpleNamespace(ddb_table="tbl", aws_region="us-east-1"),
        session=object(),
        http_cfg=SimpleNamespace(timeout_s=30),
        run_id="run-1",
    )

    assert result.classification == "success"
    assert result.changed is True
    assert result.unchanged is False
    assert called["s3"] == 1
    assert called["upsert"] == 1