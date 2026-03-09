from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable

import boto3


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


def sort_layout_hole_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        list(rows),
        key=lambda row: (
            _sort_int(row.get("layout_id")),
            _sort_int(row.get("hole_ordinal")),
        ),
    )


def serialize_layout_hole_rows_jsonl(rows: Iterable[dict[str, Any]]) -> bytes:
    sorted_rows = sort_layout_hole_rows(rows)
    lines = [
        json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        for row in sorted_rows
    ]
    text = "\n".join(lines)
    if text:
        text += "\n"
    return text.encode("utf-8")


def build_layout_hole_keys(
    *,
    silver_prefix: str,
    layout_id: int,
    source_fetch_ts: str,
) -> tuple[str, str]:
    prefix = _normalize_prefix(silver_prefix)
    fetch_date = source_fetch_ts[:10] if len(source_fetch_ts) >= 10 else "unknown"

    base = (
        f"{prefix}/"
        f"layout_id={int(layout_id)}/"
        f"source_fetch_date={fetch_date}/"
        f"source_fetch_ts={source_fetch_ts}"
    )
    return f"{base}.jsonl", f"{base}.meta.json"


def put_layout_hole_current(
    *,
    bucket: str,
    silver_prefix: str,
    layout_id: int,
    source_fetch_ts: str,
    source_content_sha256: str,
    source_event_id: int,
    source_division_code: str,
    source_round_number: int,
    source_url: str,
    rows: list[dict[str, Any]],
    run_id: str,
    s3_client=None,
) -> dict[str, Any]:
    s3 = s3_client or boto3.client("s3")

    rows_key, meta_key = build_layout_hole_keys(
        silver_prefix=silver_prefix,
        layout_id=layout_id,
        source_fetch_ts=source_fetch_ts,
    )
    rows_body = serialize_layout_hole_rows_jsonl(rows)

    meta = {
        "layout_id": int(layout_id),
        "row_count": len(rows),
        "source_fetch_ts": source_fetch_ts,
        "source_content_sha256": source_content_sha256,
        "source_event_id": int(source_event_id),
        "source_division_code": source_division_code,
        "source_round_number": int(source_round_number),
        "source_url": source_url,
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
        "layout_id": int(layout_id),
        "row_count": len(rows),
        "s3_rows_key": rows_key,
        "s3_meta_key": meta_key,
    }