import json
from datetime import date

import ingest_weather_observations.s3_writer as s3_writer
from ingest_weather_observations.models import GeoPoint, WeatherFetchWindow, WeatherObservationTask


class FakeS3Client:
    def __init__(self):
        self.put_calls = []

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)


def _task() -> WeatherObservationTask:
    return WeatherObservationTask(
        event_id=90008,
        event_name="Test Event",
        point=GeoPoint(latitude=30.2672, longitude=-97.7431),
        window=WeatherFetchWindow(
            round_number=2,
            round_date=date(2026, 3, 10),
            start_date=date(2026, 3, 10),
            end_date=date(2026, 3, 10),
        ),
        city="Austin",
        state="TX",
        country="United States",
    )


def test_build_weather_keys():
    task = _task()
    json_key, meta_key = s3_writer.build_weather_keys(task, "2026-03-16T12:00:00Z")
    assert json_key.endswith("fetch_ts=2026-03-16T12_00_00Z.json")
    assert meta_key.endswith("fetch_ts=2026-03-16T12_00_00Z.meta.json")
    assert "provider=open_meteo_archive" in json_key
    assert "event_id=90008" in json_key
    assert "round=2" in json_key


def test_put_weather_raw_writes_json_and_meta(monkeypatch):
    fake_s3 = FakeS3Client()
    monkeypatch.setattr(s3_writer, "utc_now_iso", lambda: "2026-03-16T12:00:00Z")

    result = s3_writer.put_weather_raw(
        bucket="pdga-bucket",
        task=_task(),
        source_url="https://archive-api.open-meteo.com/v1/archive",
        request_params={"latitude": "30.26720"},
        request_fingerprint="req-hash",
        payload={"hourly": {"time": ["2026-03-10T08:00"], "wind_speed_10m": [5.1]}},
        daylight_hour_count=1,
        content_sha256="payload-hash",
        http_status=200,
        run_id="weather-run-1",
        tee_time_source_fingerprint="tee-fp-1",
        s3_client=fake_s3,
    )

    assert len(fake_s3.put_calls) == 2
    assert result["event_id"] == 90008
    assert result["round_number"] == 2

    json_call = fake_s3.put_calls[0]
    meta_call = fake_s3.put_calls[1]
    assert json_call["Bucket"] == "pdga-bucket"
    assert meta_call["Bucket"] == "pdga-bucket"

    meta = json.loads(meta_call["Body"].decode("utf-8"))
    assert meta["request_fingerprint"] == "req-hash"
    assert meta["tee_time_source_fingerprint"] == "tee-fp-1"
    assert meta["daylight_hour_count"] == 1