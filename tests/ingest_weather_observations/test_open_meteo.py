from datetime import date

from ingest_weather_observations.models import GeoPoint, WeatherFetchWindow
from ingest_weather_observations.open_meteo import build_archive_request


def test_build_archive_request_params():
    point = GeoPoint(latitude=30.2672, longitude=-97.7431)
    window = WeatherFetchWindow(
        round_number=1,
        round_date=date(2026, 3, 10),
        start_date=date(2026, 3, 9),
        end_date=date(2026, 3, 11),
    )

    req = build_archive_request(point=point, window=window)
    params = req.to_params()

    assert params["latitude"] == "30.26720"
    assert params["longitude"] == "-97.74310"
    assert params["start_date"] == "2026-03-09"
    assert params["end_date"] == "2026-03-11"
    assert params["timezone"] == "UTC"
    assert "wind_speed_10m" in params["hourly"]
    assert "wind_gusts_10m" in params["hourly"]