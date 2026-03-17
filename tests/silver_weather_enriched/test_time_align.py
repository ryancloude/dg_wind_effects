from silver_weather_enriched.time_align import (
    floor_hour_utc_iso,
    parse_iso_to_utc,
    resolve_hole_observation_hour_utc,
    resolve_round_observation_hour_utc,
)


def test_parse_iso_to_utc_supports_z():
    dt = parse_iso_to_utc("2026-03-17T15:34:45Z")
    assert dt is not None
    assert dt.isoformat().endswith("+00:00")


def test_floor_hour_utc_iso():
    assert floor_hour_utc_iso("2026-03-17T15:34:45Z") == "2026-03-17T15:00:00Z"


def test_resolve_round_hour_prefers_tee_time():
    row = {
        "tee_time_join_ts": "2026-03-17T08:22:00Z",
        "round_date_interp": "2026-03-17",
    }
    assert resolve_round_observation_hour_utc(row) == "2026-03-17T08:00:00Z"


def test_resolve_round_hour_fallbacks_to_round_date_interp():
    row = {"tee_time_join_ts": None, "round_date_interp": "2026-03-17"}
    assert resolve_round_observation_hour_utc(row) == "2026-03-17T12:00:00Z"


def test_resolve_hole_hour_prefers_hole_time_est_ts():
    row = {
        "hole_time_est_ts": "2026-03-17T09:05:00Z",
        "tee_time_join_ts": "2026-03-17T08:00:00Z",
        "round_date_interp": "2026-03-17",
    }
    assert resolve_hole_observation_hour_utc(row) == "2026-03-17T09:00:00Z"


def test_resolve_hole_hour_fallback_order():
    row = {
        "hole_time_est_ts": None,
        "tee_time_join_ts": "2026-03-17T08:22:00Z",
        "round_date_interp": "2026-03-17",
    }
    assert resolve_hole_observation_hour_utc(row) == "2026-03-17T08:00:00Z"