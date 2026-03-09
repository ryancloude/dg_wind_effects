import json

import silver_pdga_live_results.player_round_writer as writer
from silver_pdga_live_results.candidate_reader import LiveResultsStatePointer


class FakeS3Client:
    def __init__(self):
        self.put_calls = []

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)


def _pointer():
    return LiveResultsStatePointer(
        event_id=90008,
        division="MA3",
        round_number=1,
        fetch_status="success",
        content_sha256="source-hash-1",
        last_fetched_at="2026-03-08T11:00:00Z",
        latest_s3_json_key="bronze/pdga/live_results/event_id=90008/division=MA3/round=1/fetch_date=2026-03-08/fetch_ts=2026-03-08T11:00:00Z.json",
        latest_s3_meta_key="bronze/pdga/live_results/event_id=90008/division=MA3/round=1/fetch_date=2026-03-08/fetch_ts=2026-03-08T11:00:00Z.meta.json",
        source_url="https://example.test/live_results",
    )


def test_build_player_round_keys():
    rows_key, meta_key = writer.build_player_round_keys(
        silver_prefix="silver/pdga/live_results/player_round_current",
        pointer=_pointer(),
    )

    assert rows_key == (
        "silver/pdga/live_results/player_round_current/"
        "event_id=90008/division=MA3/round=1/"
        "source_fetch_date=2026-03-08/source_fetch_ts=2026-03-08T11:00:00Z.jsonl"
    )
    assert meta_key.endswith(".meta.json")


def test_put_player_round_current_writes_rows_and_meta(monkeypatch):
    fake_s3 = FakeS3Client()
    monkeypatch.setattr(writer, "utc_now_iso", lambda: "2026-03-08T12:00:00Z")

    rows = [
        {"event_id": 90008, "division_code": "MA3", "round_number": 1, "result_id": 2, "value": "b"},
        {"event_id": 90008, "division_code": "MA3", "round_number": 1, "result_id": 1, "value": "a"},
    ]

    result = writer.put_player_round_current(
        bucket="pdga-bucket",
        silver_prefix="silver/pdga/live_results/player_round_current",
        pointer=_pointer(),
        rows=rows,
        run_id="silver-run-1",
        s3_client=fake_s3,
    )

    assert result["row_count"] == 2
    assert len(fake_s3.put_calls) == 2

    rows_call = fake_s3.put_calls[0]
    meta_call = fake_s3.put_calls[1]

    assert rows_call["Bucket"] == "pdga-bucket"
    assert rows_call["Key"] == result["s3_rows_key"]
    assert rows_call["ContentType"] == "application/x-ndjson"

    lines = rows_call["Body"].decode("utf-8").strip().splitlines()
    parsed = [json.loads(line) for line in lines]
    assert [row["result_id"] for row in parsed] == [1, 2]

    meta = json.loads(meta_call["Body"].decode("utf-8"))
    assert meta["row_count"] == 2
    assert meta["run_id"] == "silver-run-1"
    assert meta["s3_rows_key"] == result["s3_rows_key"]