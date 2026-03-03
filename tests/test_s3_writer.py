import gzip
import json

import ingest_pdga_event_pages.s3_writer as s3_writer


class FakeS3Client:
    def __init__(self):
        self.put_calls = []

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)


def test_build_event_page_keys():
    html_key, meta_key = s3_writer.build_event_page_keys(123, "2026-03-03T12:00:00Z")

    assert html_key == (
        "bronze/pdga/event_page/event_id=123/"
        "fetch_date=2026-03-03/"
        "fetch_ts=2026-03-03T12:00:00Z.html.gz"
    )
    assert meta_key == (
        "bronze/pdga/event_page/event_id=123/"
        "fetch_date=2026-03-03/"
        "fetch_ts=2026-03-03T12:00:00Z.meta.json"
    )


def test_put_event_page_raw_writes_gzipped_html_and_metadata(monkeypatch):
    fake_s3 = FakeS3Client()
    monkeypatch.setattr(s3_writer, "utc_now_iso", lambda: "2026-03-03T12:00:00Z")

    result = s3_writer.put_event_page_raw(
        bucket="test-bucket",
        event_id=123,
        source_url="https://www.pdga.com/tour/event/123",
        html="<html><body>Hello</body></html>",
        http_status=200,
        content_sha256="content-hash-123",
        parser_version="event-page-v2",
        s3_client=fake_s3,
    )

    assert result == {
        "event_id": 123,
        "fetched_at": "2026-03-03T12:00:00Z",
        "s3_html_key": (
            "bronze/pdga/event_page/event_id=123/"
            "fetch_date=2026-03-03/"
            "fetch_ts=2026-03-03T12:00:00Z.html.gz"
        ),
        "s3_meta_key": (
            "bronze/pdga/event_page/event_id=123/"
            "fetch_date=2026-03-03/"
            "fetch_ts=2026-03-03T12:00:00Z.meta.json"
        ),
    }

    assert len(fake_s3.put_calls) == 2

    html_call = fake_s3.put_calls[0]
    meta_call = fake_s3.put_calls[1]

    assert html_call["Bucket"] == "test-bucket"
    assert html_call["Key"] == result["s3_html_key"]
    assert html_call["ContentType"] == "text/html; charset=utf-8"
    assert html_call["ContentEncoding"] == "gzip"
    assert gzip.decompress(html_call["Body"]).decode("utf-8") == "<html><body>Hello</body></html>"

    assert meta_call["Bucket"] == "test-bucket"
    assert meta_call["Key"] == result["s3_meta_key"]
    assert meta_call["ContentType"] == "application/json"

    meta_body = json.loads(meta_call["Body"].decode("utf-8"))
    assert meta_body == {
        "event_id": 123,
        "source_url": "https://www.pdga.com/tour/event/123",
        "fetched_at": "2026-03-03T12:00:00Z",
        "http_status": 200,
        "content_sha256": "content-hash-123",
        "content_length": len("<html><body>Hello</body></html>"),
        "parser_version": "event-page-v2",
        "s3_html_key": result["s3_html_key"],
    }