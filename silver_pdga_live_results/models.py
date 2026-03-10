from __future__ import annotations

from dataclasses import dataclass
from typing import Any

FINAL_EVENT_STATUSES = (
    "Event complete; official ratings processed.",
    "Event complete; unofficial ratings processed.",
)

ROUND_PK_COLS = ("tourn_id", "round_number", "player_key")
HOLE_PK_COLS = ("tourn_id", "round_number", "hole_number", "player_key")

ROUND_TIEBREAK_COLS = (
    "source_fetched_at_utc",
    "scorecard_updated_at_ts",
    "update_date_ts",
    "source_json_key",
)

HOLE_TIEBREAK_COLS = (
    "source_fetched_at_utc",
    "scorecard_updated_at_ts",
    "update_date_ts",
    "source_json_key",
)

LINEAGE_REQUIRED_COLS = (
    "source_json_key",
    "source_content_sha256",
    "source_fetched_at_utc",
    "silver_run_id",
)


@dataclass(frozen=True)
class BronzeRoundSource:
    event_id: int
    division: str
    round_number: int
    source_json_key: str
    source_meta_key: str | None
    source_content_sha256: str
    source_fetched_at_utc: str
    payload: dict[str, Any] | list[Any]