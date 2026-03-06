from __future__ import annotations

from typing import Any

import requests

from ingest_pdga_event_pages.http_client import HttpConfig, build_session, polite_sleep
from ingest_pdga_live_results.dynamo_reader import LiveResultsTask

LIVE_RESULTS_URL = "https://www.pdga.com/apps/tournament/live-api/live_results_fetch_round"


def build_live_results_url(task: LiveResultsTask) -> str:
    return (
        f"{LIVE_RESULTS_URL}"
        f"?TournID={task.event_id}"
        f"&Division={task.division}"
        f"&Round={task.round_number}"
    )


def get_live_results_json(
    session: requests.Session,
    cfg: HttpConfig,
    task: LiveResultsTask,
) -> tuple[int, dict[str, Any] | list[Any] | None, str]:
    url = build_live_results_url(task)
    res = session.get(url, timeout=cfg.timeout_s)

    if res.status_code == 404:
        return 404, None, url

    res.raise_for_status()

    try:
        payload = res.json()
    except ValueError as exc:
        raise ValueError(f"live results response is not valid JSON for url={url}") from exc

    return res.status_code, payload, url