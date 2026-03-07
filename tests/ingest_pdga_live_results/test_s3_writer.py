import json

import ingest_pdga_live_results.s3_writer as s3_writer
from ingest_pdga_live_results.dynamo_reader import LiveResultsTask


class FakeS3Client:
    def __init__(self):
        self.put_calls = []

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)


def test_build_live_results_keys():
    task = LiveResultsTask(event_id="92608", division="MPO", round_number=2)
    json_key, meta_key = s3_writer.build_live_results_keys(task, "2026-03-06T12:00:00Z")

    assert json_key == (
        "bronze/pdga/live_results/"
        "event_id=92608/"
        "division=MPO/"
        "round=2/"
        "fetch_date=2026-03-06/"
        "fetch_ts=2026-03-06T12:00:00Z.json"
    )
    assert meta_key == (
        "bronze/pdga/live_results/"
        "event_id=92608/"
        "division=MPO/"
        "round=2/"
        "fetch_date=2026-03-06/"
        "fetch_ts=2026-03-06T12:00:00Z.meta.json"
    )


def test_put_live_results_raw_writes_payload_and_metadata(monkeypatch):
    fake_s3 = FakeS3Client()
    monkeypatch.setattr(s3_writer, "utc_now_iso", lambda: "2026-03-06T12:00:00Z")

    task = LiveResultsTask(event_id="92608", division="MPO", round_number=2)
    result = s3_writer.put_live_results_raw(
        bucket="pdga-bucket",
        task=task,
        source_url="https://www.pdga.com/apps/tournament/live-api/live_results_fetch_round?TournID=92608&Division=MPO&Round=2",
        payload={"results": [{"player": "A", "score": 55}]},
        http_status=200,
        content_sha256="hash-123",
        run_id="run-abc",
        s3_client=fake_s3,
    )

    assert result["event_id"] == 92608
    assert result["division"] == "MPO"
    assert result["round_number"] == 2
    assert len(fake_s3.put_calls) == 2

    json_call = fake_s3.put_calls[0]
    meta_call = fake_s3.put_calls[1]

    assert json_call["Bucket"] == "pdga-bucket"
    assert json_call["Key"] == result["s3_json_key"]
    assert json_call["ContentType"] == "application/json"

    payload = json.loads(json_call["Body"].decode("utf-8"))
    assert payload == {"results": [{"player": "A", "score": 55}]}

    meta = json.loads(meta_call["Body"].decode("utf-8"))
    assert meta["event_id"] == 92608
    assert meta["division"] == "MPO"
    assert meta["round_number"] == 2
    assert meta["http_status"] == 200
    assert meta["content_sha256"] == "hash-123"
    assert meta["run_id"] == "run-abc"
    assert meta["s3_json_key"] == result["s3_json_key"]