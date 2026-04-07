import pandas as pd

from report_round_weather_impacts.dimensions import prepare_reporting_dataframe


def test_prepare_reporting_dataframe_converts_units_and_bands():
    df = pd.DataFrame(
        [
            {
                "event_year": 2026,
                "tourn_id": 90008,
                "round_number": 1,
                "player_key": "P1",
                "course_id": "101",
                "layout_id": "201",
                "division": "MA3",
                "player_rating": 915,
                "actual_round_strokes": 57,
                "predicted_round_strokes": 58.0,
                "predicted_round_strokes_wind_reference": 56.5,
                "estimated_wind_impact_strokes": 1.5,
                "estimated_temperature_impact_strokes": 0.5,
                "estimated_total_weather_impact_strokes": 2.0,
                "round_wind_speed_mps_mean": 4.0,
                "round_temp_c_mean": 20.0,
                "round_precip_mm_sum": 0.0,
                "state": "tx",
            }
        ]
    )

    out = prepare_reporting_dataframe(df)
    row = out.iloc[0]

    assert row["rating_band"] == "900-939"
    assert row["state"] == "TX"
    assert row["observed_wind_mph"] > 8.9
    assert row["observed_temp_f"] == 68.0
    assert row["temperature_band_f"] == "60-69F"
