# s3_writer.py
from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Tuple, Optional

import boto3


def utc_now_iso() -> str:
    """UTC timestamp like 2026-02-01T03:12:45Z"""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_event_page_keys(event_id: int, fetched_at_iso: str) -> Tuple[str, str]:
    """
    Returns (html_key, meta_key)
    """
    fetch_date = fetched_at_iso[:10]  # YYYY-MM-DD
    prefix = f"bronze/pdga/event_page/event_id={event_id}/fetch_date={fetch_date}/"
    base = f"fetch_ts={fetched_at_iso}"
    return (
        prefix + base + ".html.gz",
        prefix + base + ".meta.json",
    )


def put_event_page_raw(
    *,
    bucket: str,
    event_id: int,
    source_url: str,
    html: str,
    http_status: int,
    content_sha256: str,
    parser_version: str,
    s3_client=None,
) -> Dict[str, Any]:
    """
    Writes gzipped HTML + metadata to S3.
    Returns pointers (keys + fetched_at).
    """
    s3 = s3_client or boto3.client("s3")

    fetched_at = utc_now_iso()
    html_key, meta_key = build_event_page_keys(event_id, fetched_at)

    # gzip the HTML payload
    html_gz = gzip.compress(html.encode("utf-8"))

    # metadata sidecar
    meta = {
        "event_id": int(event_id),
        "source_url": source_url,
        "fetched_at": fetched_at,
        "http_status": int(http_status),
        "content_sha256": content_sha256,
        "content_length": len(html),
        "parser_version": parser_version,
        "s3_html_key": html_key,
    }

    # Write gzipped HTML
    s3.put_object(
        Bucket=bucket,
        Key=html_key,
        Body=html_gz,
        ContentType="text/html; charset=utf-8",
        ContentEncoding="gzip",
    )

    # Write metadata JSON
    s3.put_object(
        Bucket=bucket,
        Key=meta_key,
        Body=json.dumps(meta, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )

    return {
        "event_id": int(event_id),
        "fetched_at": fetched_at,
        "s3_html_key": html_key,
        "s3_meta_key": meta_key,
    }