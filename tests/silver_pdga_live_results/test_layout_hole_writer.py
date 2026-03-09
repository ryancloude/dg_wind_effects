import json

import silver_pdga_live_results.layout_hole_writer as writer


class FakeS3Client:
    def __init__(self):
        self.put_calls = []

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)


def test_build_layout_hole_keys():
    rows_key, meta_key = writer.build_layout_hole_keys(
        silver_prefix="silver/pdga/live_results/layout_hole_current",
        layout_id=712276,
        source_fetch_ts="2026-03-08T11:00:00Z",
    )

    assert rows_key == (
        "silver/pdga/live_results/layout_hole_current/"
        "layout_id=712276/source_fetch_date=2026-03-08/source_fetch_ts=2026-03-08T11:00:00Z.jsonl"
    )
    assert meta_key.endswith(".meta.json")


def test_put_layout_hole_current_writes_rows_and_meta(monkeypatch):
    fake_s3 = FakeS3Client()
    monkeypatch.setattr(writer, "utc_now_iso", lambda: "2026-03-08T12:00:00Z")

    rows = [
        {"layout_id": 712276, "hole_ordinal": 2, "hole_par": 3},
        {"layout_id": 712276, "hole_ordinal": 1, "hole_par": 3},
    ]

    out = writer.put_layout_hole_current(
        bucket="pdga-bucket",
        silver_prefix="silver/pdga/live_results/layout_hole_current",
        layout_id=712276,
        source_fetch_ts="2026-03-08T11:00:00Z",
        source_content_sha256="source-hash-1",
        source_event_id=90008,
        source_division_code="MA3",
        source_round_number=1,
        source_url="https://example.test/live_results",
        rows=rows,
        run_id="layout-run-1",
        s3_client=fake_s3,
    )

    assert out["row_count"] == 2
    assert len(fake_s3.put_calls) == 2

    rows_call = fake_s3.put_calls[0]
    meta_call = fake_s3.put_calls[1]

    lines = rows_call["Body"].decode("utf-8").strip().splitlines()
    parsed = [json.loads(line) for line in lines]
    assert [row["hole_ordinal"] for row in parsed] == [1, 2]

    meta = json.loads(meta_call["Body"].decode("utf-8"))
    assert meta["layout_id"] == 712276
    assert meta["row_count"] == 2
    assert meta["run_id"] == "layout-run-1"