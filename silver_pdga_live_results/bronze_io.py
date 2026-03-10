from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

import json
import hashlib
from silver_pdga_live_results.models import BronzeRoundSource

STATE_SK_RE = re.compile(r"^LIVE_RESULTS#DIV#(?P<division>.+)#ROUND#(?P<round>\d+)$")
FETCH_TS_RE = re.compile(r"fetch_ts=(?P<ts>[^/]+)\.json$")


def canonical_json(payload: dict[str, Any] | list[Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_payload_sha256(payload: dict[str, Any] | list[Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()

def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _parse_division_round_from_state_item(item: dict[str, Any]) -> tuple[str, int] | None:
    division = _normalize_text(item.get("division"))
    round_number = _safe_int(item.get("round_number"))

    if division and round_number and round_number > 0:
        return division, round_number

    sk = _normalize_text(item.get("sk"))
    match = STATE_SK_RE.match(sk)
    if not match:
        return None

    division = match.group("division").strip()
    round_number = int(match.group("round"))
    if not division or round_number <= 0:
        return None
    return division, round_number


def _list_latest_json_key(
    *,
    s3_client,
    bucket: str,
    prefix: str,
) -> str | None:
    keys: list[str] = []
    token = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3_client.list_objects_v2(**kwargs)

        for obj in resp.get("Contents", []):
            key = _normalize_text(obj.get("Key"))
            if key.endswith(".json") and not key.endswith(".meta.json"):
                keys.append(key)

        token = resp.get("NextContinuationToken")
        if not token:
            break

    if not keys:
        return None
    return max(keys)


def _derive_meta_key(json_key: str) -> str:
    if json_key.endswith(".json"):
        return json_key[:-5] + ".meta.json"
    return json_key + ".meta.json"


def _load_json_object(*, s3_client, bucket: str, key: str) -> dict[str, Any] | list[Any]:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def _load_optional_json_object(*, s3_client, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound"):
            return None
        raise
    return json.loads(obj["Body"].read().decode("utf-8"))


def _fallback_fetched_at_from_key(key: str) -> str:
    match = FETCH_TS_RE.search(key)
    if not match:
        return ""
    value = match.group("ts")
    # key has fetch_ts=<UTC_ISO>.json where UTC_ISO already uses Z
    return value


def _normalize_iso_utc(raw: str) -> str:
    raw = _normalize_text(raw)
    if not raw:
        return ""
    if raw.endswith("Z"):
        return raw
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc).replace(microsecond=0)
        return dt.isoformat().replace("+00:00", "Z")
    except ValueError:
        return raw


def build_round_sources(
    *,
    bucket: str,
    event_id: int,
    state_items: list[dict[str, Any]],
    s3_client=None,
) -> list[BronzeRoundSource]:
    s3 = s3_client or boto3.client("s3")
    out: list[BronzeRoundSource] = []

    for item in state_items:
        div_round = _parse_division_round_from_state_item(item)
        if div_round is None:
            continue
        division, round_number = div_round

        json_key = _normalize_text(item.get("latest_s3_json_key"))
        if not json_key:
            prefix = (
                f"bronze/pdga/live_results/event_id={int(event_id)}/"
                f"division={division}/"
                f"round={int(round_number)}/"
            )
            json_key = _list_latest_json_key(s3_client=s3, bucket=bucket, prefix=prefix) or ""

        if not json_key:
            continue

        meta_key = _normalize_text(item.get("latest_s3_meta_key")) or _derive_meta_key(json_key)

        payload = _load_json_object(s3_client=s3, bucket=bucket, key=json_key)
        meta = _load_optional_json_object(s3_client=s3, bucket=bucket, key=meta_key) or {}

        content_sha = _normalize_text(meta.get("content_sha256")) or _normalize_text(item.get("content_sha256"))
        if not content_sha:
            content_sha = compute_payload_sha256(payload)

        fetched_at = _normalize_text(meta.get("fetched_at")) or _normalize_text(item.get("last_fetched_at"))
        if not fetched_at:
            fetched_at = _fallback_fetched_at_from_key(json_key)
        fetched_at = _normalize_iso_utc(fetched_at)

        out.append(
            BronzeRoundSource(
                event_id=int(event_id),
                division=division,
                round_number=int(round_number),
                source_json_key=json_key,
                source_meta_key=meta_key if meta_key else None,
                source_content_sha256=content_sha,
                source_fetched_at_utc=fetched_at,
                payload=payload,
            )
        )

    out.sort(key=lambda x: (x.division, x.round_number, x.source_json_key))
    return out


def compute_event_source_fingerprint(round_sources: list[BronzeRoundSource]) -> str:
    rows = [
        {
            "division": src.division,
            "round_number": int(src.round_number),
            "source_json_key": src.source_json_key,
            "source_content_sha256": src.source_content_sha256,
        }
        for src in round_sources
    ]
    rows.sort(key=lambda x: (x["division"], x["round_number"], x["source_json_key"]))
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()