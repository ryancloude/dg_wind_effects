from silver_weather_enriched.join import (
    build_weather_lookup,
    compute_enriched_event_fingerprint,
    enrich_player_hole_rows,
    enrich_player_round_rows,
)


def _weather_row(**overrides):
    row = {
        "event_id": 90008,
        "round_number": 1,
        "observation_hour_utc": "2026-03-17T08:00:00Z",
        "wind_speed_mps": 4.2,
        "wind_gust_mps": 6.0,
        "wind_dir_deg": 120.0,
        "temperature_c": 19.5,
        "pressure_hpa": 1012.0,
        "relative_humidity_pct": 70.0,
        "precip_mm": 0.0,
        "provider": "open_meteo_archive",
        "source_id": "GRID#A",
        "source_json_key": "silver/weather/a.parquet",
        "source_content_sha256": "wx-hash-1",
        "weather_obs_pk": "wxpk-1",
        "source_fetched_at_utc": "2026-03-17T12:00:00Z",
    }
    row.update(overrides)
    return row


def test_build_weather_lookup_tiebreak_prefers_newer_source():
    older = _weather_row(source_fetched_at_utc="2026-03-17T12:00:00Z", source_json_key="a")
    newer = _weather_row(source_fetched_at_utc="2026-03-17T13:00:00Z", source_json_key="b", wind_speed_mps=5.0)

    lookup = build_weather_lookup([older, newer])
    picked = lookup[(90008, 1, "2026-03-17T08:00:00Z")]
    assert picked["wind_speed_mps"] == 5.0
    assert picked["source_json_key"] == "b"


def test_enrich_player_round_rows_matches_weather_on_hour():
    round_rows = [
        {
            "tourn_id": 90008,
            "round_number": 1,
            "player_key": "P1",
            "tee_time_join_ts": "2026-03-17T08:22:00Z",
        }
    ]
    lookup = build_weather_lookup([_weather_row()])

    out = enrich_player_round_rows(round_rows, lookup)
    assert len(out) == 1
    assert out[0]["wx_weather_missing_flag"] is False
    assert out[0]["wx_wind_speed_mps"] == 4.2
    assert out[0]["wx_observation_hour_utc"] == "2026-03-17T08:00:00Z"


def test_enrich_player_round_rows_missing_weather_keeps_row():
    round_rows = [
        {
            "tourn_id": 90008,
            "round_number": 1,
            "player_key": "P1",
            "tee_time_join_ts": "2026-03-17T09:22:00Z",
        }
    ]
    lookup = build_weather_lookup([_weather_row()])  # only 08:00 row

    out = enrich_player_round_rows(round_rows, lookup)
    assert len(out) == 1
    assert out[0]["wx_weather_missing_flag"] is True
    assert out[0]["wx_wind_speed_mps"] is None


def test_enrich_player_hole_rows_prefers_hole_time_est_ts():
    hole_rows = [
        {
            "tourn_id": 90008,
            "round_number": 1,
            "hole_number": 1,
            "player_key": "P1",
            "hole_time_est_ts": "2026-03-17T08:05:00Z",
            "tee_time_join_ts": "2026-03-17T07:30:00Z",
        }
    ]
    lookup = build_weather_lookup([_weather_row(observation_hour_utc="2026-03-17T08:00:00Z", wind_speed_mps=7.5)])

    out = enrich_player_hole_rows(hole_rows, lookup)
    assert len(out) == 1
    assert out[0]["wx_weather_missing_flag"] is False
    assert out[0]["wx_wind_speed_mps"] == 7.5


def test_compute_enriched_event_fingerprint_is_deterministic():
    round_rows = [{"tourn_id": 90008, "round_number": 1, "player_key": "P1", "tee_time_join_ts": "2026-03-17T08:22:00Z"}]
    hole_rows = [{"tourn_id": 90008, "round_number": 1, "hole_number": 1, "player_key": "P1"}]
    weather_rows = [_weather_row()]

    fp1 = compute_enriched_event_fingerprint(
        round_rows=round_rows,
        hole_rows=hole_rows,
        weather_rows=weather_rows,
    )
    fp2 = compute_enriched_event_fingerprint(
        round_rows=list(reversed(round_rows)),
        hole_rows=list(reversed(hole_rows)),
        weather_rows=list(reversed(weather_rows)),
    )

    assert fp1 == fp2