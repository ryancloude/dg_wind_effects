from datetime import date

from ingest_weather_observations.models import GeoPoint, WeatherFetchWindow, WeatherObservationTask


def test_task_source_id_and_provider():
    point = GeoPoint(latitude=30.2672, longitude=-97.7431)
    window = WeatherFetchWindow(
        round_number=1,
        round_date=date(2026, 3, 10),
        start_date=date(2026, 3, 9),
        end_date=date(2026, 3, 11),
    )
    task = WeatherObservationTask(event_id=123, event_name="Test Event", point=point, window=window)

    assert task.provider == "open_meteo_archive"
    assert task.source_id == "GRID#30.2672_-97.7431"