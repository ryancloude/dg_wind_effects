# http_client.py
from __future__ import annotations

import random
import time
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass(frozen=True)
class HttpConfig:
    timeout_s: int = 30

    # Retry policy
    retry_total: int = 6
    backoff_factor: float = 2.0
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504)

    # Politeness / rate limiting
    base_sleep_s: float = 4.0
    jitter_s: float = 2.0

    # Headers
    user_agent: str = "pdga-ingest/1.0"


def build_session(cfg: HttpConfig) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": cfg.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
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
        raise_on_status=False,  # we will raise manually
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def polite_sleep(cfg: HttpConfig) -> None:
    """Sleep base + jitter to avoid a bot-like cadence."""
    if cfg.base_sleep_s <= 0:
        return
    time.sleep(cfg.base_sleep_s + random.random() * max(cfg.jitter_s, 0.0))


def get_event_page_html(session: requests.Session, cfg: HttpConfig, event_id: int) -> tuple[int, str]:
    """
    Fetch https://www.pdga.com/tour/event/{event_id}
    Returns (status_code, html_text).
    Raises requests.HTTPError for non-200 after retries/backoff.
    """
    url = f"https://www.pdga.com/tour/event/{event_id}"

    res = session.get(url, timeout=cfg.timeout_s)

    # Sometimes, even after urllib3 retries, you can still land on 429.
    # If so, honor Retry-After if present, else back off.
    if res.status_code == 429:
        retry_after = res.headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            time.sleep(int(retry_after) + 1)
        else:
            time.sleep(30)
        res = session.get(url, timeout=cfg.timeout_s)

    res.raise_for_status()
    return res.status_code, res.text