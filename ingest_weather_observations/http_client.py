from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingest_weather_observations.open_meteo import OpenMeteoRequest

OPEN_METEO_GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"


@dataclass(frozen=True)
class HttpConfig:
    timeout_s: int = 30
    retry_total: int = 6
    backoff_factor: float = 1.5
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504)
    user_agent: str = "dg-wind-effects-weather-ingest/1.0"


def build_session(cfg: HttpConfig) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": cfg.user_agent,
            "Accept": "application/json",
            "Connection": "keep-alive",
        }
    )

    retry = Retry(
        total=cfg.retry_total,
        connect=cfg.retry_total,
        read=cfg.retry_total,
        status=cfg.retry_total,
        backoff_factor=cfg.backoff_factor,
        status_forcelist=cfg.status_forcelist,
        allowed_methods=("GET",),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _parse_json_object(*, response, url: str) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise ValueError(f"open-meteo response was not valid JSON for url={url}") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"open-meteo response is not a JSON object for url={url}")
    return payload


def get_open_meteo_archive_json(
    *,
    session: requests.Session,
    cfg: HttpConfig,
    request: OpenMeteoRequest,
) -> tuple[int, dict[str, Any], str, dict[str, str]]:
    url = request.url
    params = request.to_params()

    res = session.get(url, params=params, timeout=cfg.timeout_s)
    res.raise_for_status()

    payload = _parse_json_object(response=res, url=url)
    return res.status_code, payload, url, params


def get_open_meteo_geocoding_json(
    *,
    session: requests.Session,
    cfg: HttpConfig,
    query_text: str,
    country_code: str | None,
    count: int = 15,
) -> tuple[int, dict[str, Any], str, dict[str, str]]:
    params: dict[str, str] = {
        "name": query_text,
        "count": str(max(1, min(int(count), 100))),
        "language": "en",
        "format": "json",
    }
    if country_code:
        params["countryCode"] = country_code.upper()

    res = session.get(OPEN_METEO_GEOCODING_URL, params=params, timeout=cfg.timeout_s)
    res.raise_for_status()

    payload = _parse_json_object(response=res, url=OPEN_METEO_GEOCODING_URL)
    return res.status_code, payload, OPEN_METEO_GEOCODING_URL, params