import random

import pytest

from ingest_weather_observations.utils import (
    build_request_fingerprint,
    compute_backoff_sleep_s,
    sanitize_iso_ts_for_s3,
)


def test_request_fingerprint_is_deterministic():
    fp1 = build_request_fingerprint(
        url="https://archive-api.open-meteo.com/v1/archive",
        params={"longitude": "-97.74310", "latitude": "30.26720", "start_date": "2026-03-09"},
    )
    fp2 = build_request_fingerprint(
        url="https://archive-api.open-meteo.com/v1/archive",
        params={"start_date": "2026-03-09", "latitude": "30.26720", "longitude": "-97.74310"},
    )
    assert fp1 == fp2


def test_sanitize_iso_ts_for_s3_replaces_colons():
    assert sanitize_iso_ts_for_s3("2026-03-13T15:22:10Z") == "2026-03-13T15_22_10Z"


def test_compute_backoff_sleep_s_honors_cap_and_jitter():
    rng = random.Random(7)
    value = compute_backoff_sleep_s(
        attempt_index=10,
        base_sleep_s=1.0,
        max_sleep_s=10.0,
        jitter_s=0.5,
        rng=rng,
    )
    assert 10.0 <= value <= 10.5


@pytest.mark.parametrize(
    "kwargs",
    [
        {"attempt_index": -1, "base_sleep_s": 1.0, "max_sleep_s": 10.0, "jitter_s": 0.2},
        {"attempt_index": 0, "base_sleep_s": 0.0, "max_sleep_s": 10.0, "jitter_s": 0.2},
        {"attempt_index": 0, "base_sleep_s": 1.0, "max_sleep_s": 0.0, "jitter_s": 0.2},
        {"attempt_index": 0, "base_sleep_s": 1.0, "max_sleep_s": 10.0, "jitter_s": -0.1},
    ],
)
def test_compute_backoff_sleep_s_rejects_invalid_input(kwargs):
    with pytest.raises(ValueError):
        compute_backoff_sleep_s(**kwargs)