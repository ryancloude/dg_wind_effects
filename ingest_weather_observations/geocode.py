from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from ingest_weather_observations.models import GeoPoint
from ingest_weather_observations.utils import sha256_obj

COUNTRY_CODE_MAP = {
    "united states": "US",
    "usa": "US",
    "u.s.": "US",
    "canada": "CA",
    "mexico": "MX",
    "australia": "AU",
    "new zealand": "NZ",
    "united kingdom": "GB",
    "uk": "GB",
}


@dataclass(frozen=True)
class GeocodeQuery:
    query_text: str
    city: str
    state: str
    country: str
    country_code: str | None
    fingerprint: str


@dataclass(frozen=True)
class GeocodeResolution:
    point: GeoPoint
    source_name: str
    source_admin1: str
    source_country: str
    source_country_code: str
    population: int | None


def _normalize_ws(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = _normalize_ws(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    text = _normalize_ws(value)
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _valid_point(lat: float, lon: float) -> bool:
    return -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0


def _to_country_code(country: str) -> str | None:
    c = _normalize_ws(country).lower()
    if not c:
        return None
    if len(c) == 2 and c.isalpha():
        return c.upper()
    return COUNTRY_CODE_MAP.get(c)


def _location_fingerprint(city: str, state: str, country: str, location_raw: str) -> str:
    payload = {
        "city": _normalize_ws(city).lower(),
        "state": _normalize_ws(state).lower(),
        "country": _normalize_ws(country).lower(),
        "location_raw": _normalize_ws(location_raw).lower(),
    }
    return sha256_obj(payload)


def build_geocode_query(metadata_item: Mapping[str, Any]) -> GeocodeQuery | None:
    city = _normalize_ws(metadata_item.get("city"))
    state = _normalize_ws(metadata_item.get("state"))
    country = _normalize_ws(metadata_item.get("country"))
    location_raw = _normalize_ws(metadata_item.get("location_raw") or metadata_item.get("raw_location"))

    parts = [p for p in (city, state, country) if p]
    query_text = ", ".join(parts) if parts else location_raw
    if not query_text:
        return None

    return GeocodeQuery(
        query_text=query_text,
        city=city,
        state=state,
        country=country,
        country_code=_to_country_code(country),
        fingerprint=_location_fingerprint(city=city, state=state, country=country, location_raw=location_raw),
    )


def build_geocode_search_candidates(query: GeocodeQuery) -> list[tuple[str, str | None]]:
    # most specific useful name first, then progressively broader
    out: list[tuple[str, str | None]] = []
    seen: set[tuple[str, str | None]] = set()

    def add(name: str, country_code: str | None):
        key = (_normalize_ws(name), country_code.upper() if country_code else None)
        if not key[0]:
            return
        if key in seen:
            return
        seen.add(key)
        out.append(key)

    if query.city and query.state:
        add(f"{query.city}, {query.state}", query.country_code)
        add(f"{query.city}, {query.state}", None)

    if query.city:
        add(query.city, query.country_code)
        add(query.city, None)

    add(query.query_text, query.country_code)
    add(query.query_text, None)

    return out


def pick_best_geocode_result(payload: Mapping[str, Any], query: GeocodeQuery) -> GeocodeResolution | None:
    raw_results = payload.get("results")
    if not isinstance(raw_results, list) or not raw_results:
        return None

    city_norm = query.city.lower()
    state_norm = query.state.lower()
    country_norm = query.country.lower()
    country_code_norm = (query.country_code or "").lower()

    ranked: list[tuple[tuple[float, int], GeocodeResolution]] = []

    for item in raw_results:
        if not isinstance(item, Mapping):
            continue

        lat = _coerce_float(item.get("latitude"))
        lon = _coerce_float(item.get("longitude"))
        if lat is None or lon is None or not _valid_point(lat, lon):
            continue

        name = _normalize_ws(item.get("name"))
        admin1 = _normalize_ws(item.get("admin1"))
        country = _normalize_ws(item.get("country"))
        country_code = _normalize_ws(item.get("country_code")).upper()
        population = _coerce_int(item.get("population"))

        score = 0.0
        if city_norm:
            if name.lower() == city_norm:
                score += 100.0
            elif name.lower().startswith(city_norm):
                score += 35.0

        if state_norm:
            if admin1.lower() == state_norm:
                score += 30.0
            elif state_norm in admin1.lower():
                score += 10.0

        if country_norm and country.lower() == country_norm:
            score += 25.0
        if country_code_norm and country_code.lower() == country_code_norm:
            score += 25.0

        if population is not None and population > 0:
            score += min(population / 1_000_000.0, 20.0)

        ranked.append(
            (
                (score, population or -1),
                GeocodeResolution(
                    point=GeoPoint(latitude=lat, longitude=lon),
                    source_name=name,
                    source_admin1=admin1,
                    source_country=country,
                    source_country_code=country_code,
                    population=population,
                ),
            )
        )

    if not ranked:
        return None

    ranked.sort(key=lambda x: x[0], reverse=True)
    return ranked[0][1]