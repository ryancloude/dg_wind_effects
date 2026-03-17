import json

from silver_weather_observations.bronze_io import (
    build_weather_round_sources,
    compute_event_source_fingerprint,
)
from silver_weather_observations.models import BronzeWeatherRoundSource


class FakeBody:
    def __init__(self, payload: bytes):
        self._payload = payload

    def read(self):
        return self._payload


class FakeS3Client:
    def __init__(self, object_map: dict[str, bytes]):
        self.object_map = object_map

    def get_object(self, *, Bucket, Key):
        if Key not in self.object_map:
            raise KeyError(f"missing key={Key}")
        return {"Body": FakeBody(self.object_map[Key])}


def _json_bytes(value) -> bytes:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def test_build_weather_round_sources_reads_json_and_meta():
    json_key = (
        "bronze/weather/observations/provider=open_meteo_archive/event_id=90008/"
        "round=1/source_id=GRID#30.2672_-97.7431/fetch_date=2026-03-16/fetch_ts=2026-03-16T12_00_00Z.json"
    )
    meta_key = json_key.replace(".json", ".meta.json")

    payload = {"hourly": {"time": ["2026-03-10T08:00"], "wind_speed_10m": [4.2]}}
    meta = {
        "round_number": 1,
        "provider": "open_meteo_archive",
        "source_id": "GRID#30.2672_-97.7431",
        "content_sha256": "abc123",
        "fetched_at": "2026-03-16T12:00:00Z",
        "request_fingerprint": "req-fp",
        "tee_time_source_fingerprint": "tee-fp",
    }

    state_items = [
        {
            "pk": "EVENT#90008",
            "sk": "WEATHER_OBS#ROUND#1#PROV#open_meteo_archive#SRC#GRID#30.2672_-97.7431",
            "latest_s3_json_key": json_key,
            "latest_s3_meta_key": meta_key,
            "content_sha256": "abc123",
            "last_fetched_at": "2026-03-16T12:00:00Z",
            "request_fingerprint": "req-fp",
            "tee_time_source_fingerprint": "tee-fp",
            "provider": "open_meteo_archive",
            "source_id": "GRID#30.2672_-97.7431",
            "round_number": 1,
        }
    ]

    s3 = FakeS3Client({json_key: _json_bytes(payload), meta_key: _json_bytes(meta)})
    out = build_weather_round_sources(bucket="bkt", event_id=90008, state_items=state_items, s3_client=s3)

    assert len(out) == 1
    src = out[0]
    assert src.event_id == 90008
    assert src.round_number == 1
    assert src.provider == "open_meteo_archive"
    assert src.source_id == "GRID#30.2672_-97.7431"
    assert src.source_json_key == json_key
    assert src.source_meta_key == meta_key
    assert src.source_content_sha256 == "abc123"
    assert src.payload["hourly"]["wind_speed_10m"] == [4.2]


def test_build_weather_round_sources_parses_round_provider_source_from_sk_when_missing_fields():
    json_key = "bronze/weather/observations/provider=open_meteo_archive/event_id=90009/round=2/source_id=GRID#1/fetch_date=2026-03-16/fetch_ts=x.json"
    meta_key = json_key.replace(".json", ".meta.json")

    payload = {"hourly": {"time": ["2026-03-11T08:00"]}}
    meta = {"fetched_at": "2026-03-16T12:00:00Z"}

    state_items = [
        {
            "pk": "EVENT#90009",
            "sk": "WEATHER_OBS#ROUND#2#PROV#open_meteo_archive#SRC#GRID#1",
            "latest_s3_json_key": json_key,
            "latest_s3_meta_key": meta_key,
        }
    ]

    s3 = FakeS3Client({json_key: _json_bytes(payload), meta_key: _json_bytes(meta)})
    out = build_weather_round_sources(bucket="bkt", event_id=90009, state_items=state_items, s3_client=s3)

    assert len(out) == 1
    assert out[0].round_number == 2
    assert out[0].provider == "open_meteo_archive"
    assert out[0].source_id == "GRID#1"


def test_compute_event_source_fingerprint_is_deterministic():
    a = BronzeWeatherRoundSource(
        event_id=1,
        round_number=1,
        provider="open_meteo_archive",
        source_id="GRID#A",
        source_json_key="k1",
        source_meta_key="m1",
        source_content_sha256="h1",
        source_fetched_at_utc="2026-03-16T12:00:00Z",
        request_fingerprint="r1",
        tee_time_source_fingerprint="t1",
        payload={"x": 1},
    )
    b = BronzeWeatherRoundSource(
        event_id=1,
        round_number=2,
        provider="open_meteo_archive",
        source_id="GRID#A",
        source_json_key="k2",
        source_meta_key="m2",
        source_content_sha256="h2",
        source_fetched_at_utc="2026-03-16T12:10:00Z",
        request_fingerprint="r2",
        tee_time_source_fingerprint="t1",
        payload={"x": 2},
    )

    fp1 = compute_event_source_fingerprint([a, b])
    fp2 = compute_event_source_fingerprint([b, a])

    assert fp1 == fp2