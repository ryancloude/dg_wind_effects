from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from ingest_weather_observations.models import GeoPoint

_LAT_LON_KEY_PAIRS = (
    ("latitude", "longitude"),
    ("lat", "lon"),
    ("lat", "lng"),
    ("location_lat", "location_lon"),
    ("course_lat", "course_lon"),
)


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _is_valid_lat_lon(latitude: float, longitude: float) -> bool:
    return -90.0 <= latitude <= 90.0 and -180.0 <= longitude <= 180.0


def extract_geopoint(metadata_item: Mapping[str, Any]) -> GeoPoint | None:
    for lat_key, lon_key in _LAT_LON_KEY_PAIRS:
        lat = _coerce_float(metadata_item.get(lat_key))
        lon = _coerce_float(metadata_item.get(lon_key))
        if lat is None or lon is None:
            continue
        if _is_valid_lat_lon(lat, lon):
            return GeoPoint(latitude=lat, longitude=lon)

    nested_location = metadata_item.get("location")
    if isinstance(nested_location, Mapping):
        lat = _coerce_float(nested_location.get("latitude") or nested_location.get("lat"))
        lon = _coerce_float(nested_location.get("longitude") or nested_location.get("lon"))
        if lat is not None and lon is not None and _is_valid_lat_lon(lat, lon):
            return GeoPoint(latitude=lat, longitude=lon)

    return None