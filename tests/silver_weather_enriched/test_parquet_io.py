import json
from io import BytesIO

import pyarrow.parquet as pq

import silver_weather_enriched.parquet_io as pio


class FakeS3Client:
    def __init__(self):
        self.put_calls = []

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)


def test_build_keys():
    round_key = pio.build_round_output_key(event_year=2026, event_id=90008)
    hole_key = pio.build_hole_output_key(event_year=2026, event_id=90008)
    assert "player_rounds_weather.parquet" in round_key
    assert "player_holes_weather.parquet" in hole_key


def test_overwrite_event_tables_writes_parquet():
    fake = FakeS3Client()
    round_rows = [{"tourn_id": 90008, "round_number": 1, "player_key": "P1", "wx_weather_missing_flag": False}]
    hole_rows = [{"tourn_id": 90008, "round_number": 1, "hole_number": 1, "player_key": "P1", "wx_weather_missing_flag": False}]

    out = pio.overwrite_event_tables(
        bucket="bkt",
        event_year=2026,
        event_id=90008,
        round_rows=round_rows,
        hole_rows=hole_rows,
        s3_client=fake,
    )

    assert "round_key" in out and "hole_key" in out
    assert len(fake.put_calls) == 2

    for call in fake.put_calls:
        table = pq.read_table(BytesIO(call["Body"]))
        assert table.num_rows == 1


def test_put_quarantine_report():
    fake = FakeS3Client()
    key = pio.put_quarantine_report(
        bucket="bkt",
        event_year=2026,
        event_id=90008,
        run_id="run-1",
        errors=[{"rule": "x", "message": "y"}],
        s3_client=fake,
    )
    assert key.endswith("dq_errors.json")
    payload = json.loads(fake.put_calls[0]["Body"].decode("utf-8"))
    assert payload["error_count"] == 1
