from __future__ import annotations

import hashlib
import json
import random
from datetime import datetime, timezone
from typing import Any, Mapping


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sanitize_iso_ts_for_s3(iso_ts: str) -> str:
    return iso_ts.replace(":", "_")


def canonical_json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_obj(value: Any) -> str:
    return sha256_text(canonical_json_dumps(value))


def build_request_fingerprint(*, url: str, params: Mapping[str, Any]) -> str:
    payload = {"url": url, "params": dict(params)}
    return sha256_obj(payload)


def compute_backoff_sleep_s(
    *,
    attempt_index: int,
    base_sleep_s: float,
    max_sleep_s: float,
    jitter_s: float,
    rng: random.Random | None = None,
) -> float:
    if attempt_index < 0:
        raise ValueError("attempt_index must be >= 0")
    if base_sleep_s <= 0:
        raise ValueError("base_sleep_s must be > 0")
    if max_sleep_s <= 0:
        raise ValueError("max_sleep_s must be > 0")
    if jitter_s < 0:
        raise ValueError("jitter_s must be >= 0")

    jitter_rng = rng if rng is not None else random
    exp = min(max_sleep_s, base_sleep_s * (2**attempt_index))
    return exp + (jitter_rng.random() * jitter_s)