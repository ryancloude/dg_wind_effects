from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable

import boto3

from silver_pdga_live_results.candidate_reader import LiveResultsStatePointer


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_prefix(prefix: str) -> str:
    normalized = prefix.strip().strip("/")
    if not normalized:
        raise ValueError("silver prefix must not be empty")
    return normalized


def _sort_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return -1


def sort_player_round_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        list(rows),
        key=lambda row: (
            _sort_int(row.get("event_id")),
            str(row.get("division_code", "")),
            _sort_int(row.get("round_number")),
            _sort_int(row.get("result_id")),
        ),
    )


def serialize_player_round_rows_jsonl(rows: Iterable[dict[str, Any]]) -> bytes:
    sorted_rows = sort_player_round_rows(rows)
    lines = [
        json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        for row in sorted_rows
    ]
    text = "\n".join(lines)
    if text:
        text += "\n"
    return text.encode("utf-8")


def build_player_round_keys(
    *,
    silver_prefix: str,
    pointer: LiveResultsStatePointer,
) -> tuple[str, str]:
    prefix = _normalize_prefix(silver_prefix)
    fetch_ts = pointer.last_fetched_at
    fetch_date = fetch_ts[:10] if len(fetch_ts) >= 10 else "unknown"

    base = (
        f"{prefix}/"
        f"event_id={int(pointer.event_id)}/"
        f"division={pointer.division}/"
        f"round={int(pointer.round_number)}/"
        f"source_fetch_date={fetch_date}/"
        f"source_fetch_ts={fetch_ts}"
    )
    return f"{base}.jsonl", f"{base}.meta.json"


def put_player_round_current(
    *,
    bucket: str,
    silver_prefix: str,
    pointer: LiveResultsStatePointer,
    rows: list[dict[str, Any]],
    run_id: str,
    s3_client=None,
) -> dict[str, Any]:
    s3 = s3_client or boto3.client("s3")

    rows_key, meta_key = build_player_round_keys(silver_prefix=silver_prefix, pointer=pointer)
    rows_body = serialize_player_round_rows_jsonl(rows)

    meta = {
        "event_id": int(pointer.event_id),
        "division": pointer.division,
        "round_number": int(pointer.round_number),
        "source_fetch_ts": pointer.last_fetched_at,
        "source_content_sha256": pointer.content_sha256,
        "source_url": pointer.source_url,
        "row_count": len(rows),
        "run_id": run_id,
        "silver_prefix": _normalize_prefix(silver_prefix),
        "s3_rows_key": rows_key,
        "written_at": utc_now_iso(),
    }

    s3.put_object(
        Bucket=bucket,
        Key=rows_key,
        Body=rows_body,
        ContentType="application/x-ndjson",
    )
    s3.put_object(
        Bucket=bucket,
        Key=meta_key,
        Body=json.dumps(meta, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )

    return {
        "event_id": int(pointer.event_id),
        "division": pointer.division,
        "round_number": int(pointer.round_number),
        "row_count": len(rows),
        "s3_rows_key": rows_key,
        "s3_meta_key": meta_key,
    }