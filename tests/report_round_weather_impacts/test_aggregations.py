import pandas as pd

from report_round_weather_impacts.aggregations import build_event_contributions


def test_build_event_contributions_returns_expected_tables():
    df = pd.DataFrame(
        [
            {
                "event_year": 2026,
                "tourn_id": 90008,
                "event_name": "Test Event",
                "state": "TX",
                "city": "Austin",
                "lat": 30.0,
                "lon": -97.0,
                "round_year": 2026,
                "round_month": 4,
                "round_month_label": "Apr",
                "round_number": 1,
                "player_key": "P1",
                "division": "MA3",
                "rating_band": "900-939",
                "temperature_band_f": "60-69F",
                "round_wind_speed_bucket": "light",
                "course_id": "101",
                "layout_id": "201",
                "observed_wind_mph": 9.0,
                "observed_temp_f": 68.0,
                "actual_round_strokes": 57.0,
                "predicted_round_strokes": 58.0,
                "predicted_round_strokes_wind_reference": 56.5,
                "estimated_wind_impact_strokes": 1.5,
                "estimated_temperature_impact_strokes": 0.5,
                "estimated_total_weather_impact_strokes": 2.0,
            }
        ]
    )

    outputs = build_event_contributions(df)

    assert "weather_by_event" in outputs
    assert "weather_by_state" in outputs
    assert "weather_by_course_layout" in outputs
    assert outputs["weather_by_event"].iloc[0]["rounds_scored"] == 1
