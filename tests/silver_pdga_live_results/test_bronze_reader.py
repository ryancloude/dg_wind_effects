import json

import pytest

import silver_pdga_live_results.bronze_reader as bronze_reader
from silver_pdga_live_results.candidate_reader import LiveResultsStatePointer


class FakeBody:
    def __init__(self, raw: bytes):
        self._raw = raw

    def read(self):
        return self._raw


class FakeS3Client:
    def __init__(self, payload_obj):
        self.payload_obj = payload_obj
        self.calls = []

    def get_object(self, **kwargs):
        self.calls.append(kwargs)
        raw = json.dumps(self.payload_obj).encode("utf-8")
        return {"Body": FakeBody(raw)}


def _pointer():
    return LiveResultsStatePointer(
        event_id=90008,
        division="MA3",
        round_number=1,
        fetch_status="success",
        content_sha256="hash-1",
        last_fetched_at="2026-03-08T11:00:00Z",
        latest_s3_json_key="bronze/pdga/live_results/event_id=90008/division=MA3/round=1/fetch_date=2026-03-08/fetch_ts=2026-03-08T11:00:00Z.json",
        latest_s3_meta_key="bronze/pdga/live_results/event_id=90008/division=MA3/round=1/fetch_date=2026-03-08/fetch_ts=2026-03-08T11:00:00Z.meta.json",
        source_url="https://example.test/live_results",
    )


def test_get_live_results_payload_from_s3_reads_and_parses_json():
    fake_s3 = FakeS3Client({"data": {"scores": []}})
    payload = bronze_reader.get_live_results_payload_from_s3(
        bucket="pdga-bucket",
        s3_key="bronze/a.json",
        s3_client=fake_s3,
    )

    assert payload == {"data": {"scores": []}}
    assert fake_s3.calls[0]["Bucket"] == "pdga-bucket"
    assert fake_s3.calls[0]["Key"] == "bronze/a.json"


def test_load_payload_for_pointer_uses_pointer_key():
    fake_s3 = FakeS3Client({"data": {"scores": [{"ResultID": 1}]}})
    pointer = _pointer()

    out = bronze_reader.load_payload_for_pointer(
        bucket="pdga-bucket",
        pointer=pointer,
        s3_client=fake_s3,
    )

    assert out.pointer == pointer
    assert out.payload["data"]["scores"][0]["ResultID"] == 1
    assert fake_s3.calls[0]["Key"] == pointer.latest_s3_json_key


def test_get_live_results_payload_from_s3_raises_for_invalid_json():
    class BadS3Client:
        def get_object(self, **kwargs):
            return {"Body": FakeBody(b"{not valid json}")}

    with pytest.raises(ValueError, match="invalid_json_payload_for_key"):
        bronze_reader.get_live_results_payload_from_s3(
            bucket="pdga-bucket",
            s3_key="bronze/bad.json",
            s3_client=BadS3Client(),
        )