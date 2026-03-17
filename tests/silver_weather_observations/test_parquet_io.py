import json
from io import BytesIO

import pyarrow.parquet as pq

import silver_weather_observations.parquet_io as pio


class FakeS3Client:
    def __init__(self):
        self.put_calls = []

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)


def test_build_observations_key():
    key = pio.build_observations_key(event_year=2026, event_id=90008)
    assert key == "silver/weather/observations_hourly/event_year=2026/event_id=90008/observations_hourly.parquet"


def test_overwrite_event_table_writes_parquet():
    fake = FakeS3Client()
    rows = [
        {
            "weather_obs_pk": "pk-1",
            "event_id": 90008,
            "round_number": 1,
            "provider": "open_meteo_archive",
            "source_id": "GRID#A",
            "observation_hour_utc": "2026-03-10T08:00:00Z",
        }
    ]

    out = pio.overwrite_event_table(
        bucket="bkt",
        event_year=2026,
        event_id=90008,
        run_id="run-1",
        observation_rows=rows,
        s3_client=fake,
    )

    assert out["observations_key"].endswith("observations_hourly.parquet")
    assert len(fake.put_calls) == 1
    assert fake.put_calls[0]["ContentType"] == "application/octet-stream"

    table = pq.read_table(BytesIO(fake.put_calls[0]["Body"]))
    loaded = table.to_pylist()
    assert loaded[0]["event_id"] == 90008


def test_put_quarantine_report_writes_json():
    fake = FakeS3Client()
    key = pio.put_quarantine_report(
        bucket="bkt",
        event_year=2026,
        event_id=90008,
        run_id="run-1",
        errors=[{"rule": "not_null:provider", "message": "x"}],
        s3_client=fake,
    )

    assert key.endswith("/dq_errors.json")
    assert len(fake.put_calls) == 1
    assert fake.put_calls[0]["ContentType"] == "application/json"

    payload = json.loads(fake.put_calls[0]["Body"].decode("utf-8"))
    assert payload["event_id"] == 90008
    assert payload["error_count"] == 1