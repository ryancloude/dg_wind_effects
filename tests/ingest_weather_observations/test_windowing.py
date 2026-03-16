from datetime import date

import pytest

from ingest_weather_observations.windowing import (
    build_fetch_window,
    build_round_date_overrides_from_silver_rows,
    infer_round_date,
    infer_round_dates,
)


def test_infer_round_date_with_start_and_end_caps_at_end():
    assert infer_round_date(
        start_date_str="2026-03-01",
        end_date_str="2026-03-03",
        round_number=4,
        max_rounds=4,
    ) == date(2026, 3, 3)


def test_infer_round_date_uses_override_when_present():
    assert infer_round_date(
        start_date_str="2026-03-01",
        end_date_str="2026-03-03",
        round_number=2,
        max_rounds=3,
        round_date_overrides={2: "2026-03-08"},
    ) == date(2026, 3, 8)


def test_build_round_date_overrides_from_silver_rows_picks_mode_then_earliest_tiebreak():
    rows = [
        {"round_number": 1, "round_date_interp": "2026-03-10"},
        {"round_number": 1, "round_date_interp": "2026-03-10"},
        {"round_number": 1, "round_date_interp": "2026-03-11"},
        {"round_number": 2, "round_date_interp": "2026-03-12"},
        {"round_number": 2, "round_date_interp": "2026-03-13"},
    ]
    out = build_round_date_overrides_from_silver_rows(rows)
    assert out[1] == date(2026, 3, 10)
    assert out[2] == date(2026, 3, 12)


def test_build_fetch_window_padding():
    window = build_fetch_window(round_number=2, round_date=date(2026, 3, 10), padding_days=1)
    assert window.start_date == date(2026, 3, 9)
    assert window.end_date == date(2026, 3, 11)


def test_infer_round_dates_mixes_overrides_and_fallback():
    windows = infer_round_dates(
        start_date_str="2026-03-01",
        end_date_str="2026-03-02",
        max_rounds=3,
        padding_days=1,
        round_date_overrides={2: "2026-03-05"},
    )
    assert windows[0].round_date == date(2026, 3, 1)
    assert windows[1].round_date == date(2026, 3, 5)
    assert windows[2].round_date == date(2026, 3, 2)


def test_infer_round_date_requires_at_least_one_date_when_no_override():
    with pytest.raises(ValueError):
        infer_round_date(start_date_str=None, end_date_str=None, round_number=1, max_rounds=1)