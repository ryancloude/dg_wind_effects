from __future__ import annotations

from dataclasses import dataclass
from typing import Any

OBS_PK_COLS = ("event_id", "round_number", "provider", "source_id", "observation_hour_utc")
OBS_TIEBREAK_COLS = ("source_fetched_at_utc", "source_json_key")

LINEAGE_REQUIRED_COLS = (
    "source_json_key",
    "source_content_sha256",
    "source_fetched_at_utc",
    "silver_run_id",
)


@dataclass(frozen=True)
class BronzeWeatherRoundSource:
    event_id: int
    round_number: int
    provider: str
    source_id: str
    source_json_key: str
    source_meta_key: str | None
    source_content_sha256: str
    source_fetched_at_utc: str
    request_fingerprint: str
    tee_time_source_fingerprint: str
    payload: dict[str, Any]