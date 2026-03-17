from silver_weather_observations.models import BronzeWeatherRoundSource
from silver_weather_observations.normalize import build_weather_obs_pk, normalize_event_records


def _source(payload: dict) -> BronzeWeatherRoundSource:
    return BronzeWeatherRoundSource(
        event_id=90008,
        round_number=1,
        provider="open_meteo_archive",
        source_id="GRID#30.2672_-97.7431",
        source_json_key="bronze/weather/.../fetch_ts=2026-03-16T12_00_00Z.json",
        source_meta_key="bronze/weather/.../fetch_ts=2026-03-16T12_00_00Z.meta.json",
        source_content_sha256="content-hash-1",
        source_fetched_at_utc="2026-03-16T12:00:00Z",
        request_fingerprint="req-fp-1",
        tee_time_source_fingerprint="tee-fp-1",
        payload=payload,
    )


def test_build_weather_obs_pk_is_deterministic():
    pk1 = build_weather_obs_pk(
        event_id=90008,
        round_number=1,
        provider="open_meteo_archive",
        source_id="GRID#30.2672_-97.7431",
        observation_hour_utc="2026-03-10T08:00:00Z",
    )
    pk2 = build_weather_obs_pk(
        event_id=90008,
        round_number=1,
        provider="open_meteo_archive",
        source_id="GRID#30.2672_-97.7431",
        observation_hour_utc="2026-03-10T08:00:00Z",
    )
    assert pk1 == pk2


def test_normalize_event_records_maps_hourly_fields():
    payload = {
        "latitude": 30.2672,
        "longitude": -97.7431,
        "hourly": {
            "time": ["2026-03-10T08:00", "2026-03-10T09:00"],
            "wind_speed_10m": [4.2, 5.1],
            "wind_gusts_10m": [6.0, 7.0],
            "wind_direction_10m": [120, 140],
            "temperature_2m": [19.5, 21.0],
            "pressure_msl": [1012.2, 1011.8],
            "relative_humidity_2m": [72, 68],
            "precipitation": [0.0, 0.2],
        },
    }

    rows = normalize_event_records(
        event_metadata={
            "event_id": 90008,
            "start_date": "2026-03-10",
            "city": "Austin",
            "state": "TX",
            "country": "United States",
            "latitude": 30.2672,
            "longitude": -97.7431,
        },
        round_sources=[_source(payload)],
        event_source_fingerprint="event-fp-1",
        run_id="silver-weather-1",
        silver_processed_at_utc="2026-03-16T13:00:00Z",
    )

    assert len(rows) == 2
    r0 = rows[0]
    assert r0["event_id"] == 90008
    assert r0["round_number"] == 1
    assert r0["provider"] == "open_meteo_archive"
    assert r0["source_id"] == "GRID#30.2672_-97.7431"
    assert r0["observation_hour_utc"] == "2026-03-10T08:00:00Z"
    assert r0["wind_speed_mps"] == 4.2
    assert r0["wind_gust_mps"] == 6.0
    assert r0["wind_dir_deg"] == 120.0
    assert r0["temperature_c"] == 19.5
    assert r0["pressure_hpa"] == 1012.2
    assert r0["relative_humidity_pct"] == 72.0
    assert r0["precip_mm"] == 0.0
    assert r0["event_source_fingerprint"] == "event-fp-1"
    assert r0["silver_run_id"] == "silver-weather-1"


def test_normalize_event_records_handles_misaligned_arrays_with_nulls():
    payload = {
        "hourly": {
            "time": ["2026-03-10T08:00", "2026-03-10T09:00"],
            "wind_speed_10m": [4.2],  # shorter list than time
            "temperature_2m": [19.5, 21.0],
        }
    }

    rows = normalize_event_records(
        event_metadata={"event_id": 90008, "start_date": "2026-03-10"},
        round_sources=[_source(payload)],
        event_source_fingerprint="event-fp-1",
        run_id="silver-weather-1",
        silver_processed_at_utc="2026-03-16T13:00:00Z",
    )

    assert len(rows) == 2
    assert rows[0]["wind_speed_mps"] == 4.2
    assert rows[1]["wind_speed_mps"] is None
    assert rows[1]["temperature_c"] == 21.0