from __future__ import annotations

from dataclasses import dataclass
from datetime import date

PROVIDER_OPEN_METEO_ARCHIVE = "open_meteo_archive"


@dataclass(frozen=True)
class GeoPoint:
    latitude: float
    longitude: float

    def source_id(self) -> str:
        return f"GRID#{self.latitude:.4f}_{self.longitude:.4f}"


@dataclass(frozen=True)
class WeatherFetchWindow:
    round_number: int
    round_date: date
    start_date: date
    end_date: date


@dataclass(frozen=True)
class WeatherObservationTask:
    event_id: int
    event_name: str
    point: GeoPoint
    window: WeatherFetchWindow
    city: str = ""
    state: str = ""
    country: str = ""

    @property
    def provider(self) -> str:
        return PROVIDER_OPEN_METEO_ARCHIVE

    @property
    def source_id(self) -> str:
        return self.point.source_id()