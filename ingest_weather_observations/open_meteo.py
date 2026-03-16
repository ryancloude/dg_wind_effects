from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from ingest_weather_observations.models import GeoPoint, WeatherFetchWindow

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

OPEN_METEO_HOURLY_FIELDS: tuple[str, ...] = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "pressure_msl",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
)


@dataclass(frozen=True)
class OpenMeteoRequest:
    url: str
    latitude: float
    longitude: float
    start_date: date
    end_date: date
    hourly_fields: tuple[str, ...] = OPEN_METEO_HOURLY_FIELDS
    timezone: str = "UTC"
    wind_speed_unit: str = "ms"
    precipitation_unit: str = "mm"
    temperature_unit: str = "celsius"

    def to_params(self) -> dict[str, str]:
        return {
            "latitude": f"{self.latitude:.5f}",
            "longitude": f"{self.longitude:.5f}",
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "hourly": ",".join(self.hourly_fields),
            "timezone": self.timezone,
            "wind_speed_unit": self.wind_speed_unit,
            "precipitation_unit": self.precipitation_unit,
            "temperature_unit": self.temperature_unit,
        }


def build_archive_request(*, point: GeoPoint, window: WeatherFetchWindow) -> OpenMeteoRequest:
    return OpenMeteoRequest(
        url=OPEN_METEO_ARCHIVE_URL,
        latitude=point.latitude,
        longitude=point.longitude,
        start_date=window.start_date,
        end_date=window.end_date,
    )