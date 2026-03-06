from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_json(payload: dict[str, Any] | list[Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_payload_sha256(payload: dict[str, Any] | list[Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def is_empty_payload(payload: dict[str, Any] | list[Any] | None) -> bool:
    if payload is None:
        return True
    if payload == {} or payload == []:
        return True
    if isinstance(payload, dict):
        for key in ("results", "players", "cards", "rows"):
            value = payload.get(key)
            if isinstance(value, list) and len(value) == 0:
                return True
    return False


def classify_response(
    *,
    status_code: int | None,
    payload: dict[str, Any] | list[Any] | None,
    error: Exception | None = None,
) -> str:
    if error is not None:
        return "failed"
    if status_code == 404:
        return "not_found"
    if status_code != 200:
        return "failed"
    if is_empty_payload(payload):
        return "empty"
    return "success"