import pytest
import requests

import ingest_pdga_live_results.http_client as http_client
from ingest_pdga_live_results.dynamo_reader import LiveResultsTask


class FakeResponse:
    def __init__(self, *, status_code: int, payload=None, json_error: bool = False):
        self.status_code = status_code
        self._payload = payload
        self._json_error = json_error

    def raise_for_status(self):
        if self.status_code >= 400 and self.status_code != 404:
            raise requests.HTTPError(f"{self.status_code} error")

    def json(self):
        if self._json_error:
            raise ValueError("invalid json")
        return self._payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, timeout):
        self.calls.append((url, timeout))
        return self.response


def test_build_live_results_url():
    task = LiveResultsTask(event_id="86076", division="MP40", round_number=1)
    url = http_client.build_live_results_url(task)
    assert url == (
        "https://www.pdga.com/apps/tournament/live-api/live_results_fetch_round"
        "?TournID=86076&Division=MP40&Round=1"
    )


def test_get_live_results_json_returns_404_tuple():
    task = LiveResultsTask(event_id="86076", division="MP40", round_number=1)
    session = FakeSession(FakeResponse(status_code=404))
    cfg = type("Cfg", (), {"timeout_s": 30})()

    status_code, payload, url = http_client.get_live_results_json(session, cfg, task)

    assert status_code == 404
    assert payload is None
    assert "TournID=86076" in url


def test_get_live_results_json_returns_json_payload():
    task = LiveResultsTask(event_id="86076", division="MP40", round_number=1)
    session = FakeSession(FakeResponse(status_code=200, payload={"results": [{"player": "A"}]}))
    cfg = type("Cfg", (), {"timeout_s": 30})()

    status_code, payload, url = http_client.get_live_results_json(session, cfg, task)

    assert status_code == 200
    assert payload == {"results": [{"player": "A"}]}
    assert "Division=MP40" in url


def test_get_live_results_json_raises_value_error_for_invalid_json():
    task = LiveResultsTask(event_id="86076", division="MP40", round_number=1)
    session = FakeSession(FakeResponse(status_code=200, json_error=True))
    cfg = type("Cfg", (), {"timeout_s": 30})()

    with pytest.raises(ValueError, match="not valid JSON"):
        http_client.get_live_results_json(session, cfg, task)