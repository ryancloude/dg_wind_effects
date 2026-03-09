from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Iterator

import boto3
from boto3.dynamodb.conditions import Attr


DEFAULT_ALLOWED_FETCH_STATUSES = frozenset(("success", "empty"))


def _coerce_positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, Decimal):
        try:
            if value != value.to_integral_value():
                return None
            parsed = int(value)
            return parsed if parsed > 0 else None
        except (InvalidOperation, ValueError):
            return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            parsed = int(stripped)
            return parsed if parsed > 0 else None
        except ValueError:
            return None
    return None


@dataclass(frozen=True)
class LiveResultsStatePointer:
    event_id: int
    division: str
    round_number: int
    fetch_status: str
    content_sha256: str
    last_fetched_at: str
    latest_s3_json_key: str
    latest_s3_meta_key: str
    source_url: str

    def unit_key(self) -> tuple[int, str, int]:
        return (self.event_id, self.division, self.round_number)

    def cursor_tuple(self) -> tuple[str, str]:
        return (self.last_fetched_at, self.latest_s3_json_key)


def build_pointer_from_state_item(item: dict[str, Any]) -> LiveResultsStatePointer | None:
    event_id = _coerce_positive_int(item.get("event_id"))
    round_number = _coerce_positive_int(item.get("round_number"))
    division = str(item.get("division", "")).strip().upper()

    fetch_status = str(item.get("fetch_status", "")).strip().lower()
    content_sha256 = str(item.get("content_sha256", "")).strip()
    last_fetched_at = str(item.get("last_fetched_at", "")).strip()
    latest_s3_json_key = str(item.get("latest_s3_json_key", "")).strip()
    latest_s3_meta_key = str(item.get("latest_s3_meta_key", "")).strip()
    source_url = str(item.get("source_url", "")).strip()

    if event_id is None or round_number is None or not division:
        return None
    if not fetch_status or not latest_s3_json_key or not last_fetched_at:
        return None

    return LiveResultsStatePointer(
        event_id=event_id,
        division=division,
        round_number=round_number,
        fetch_status=fetch_status,
        content_sha256=content_sha256,
        last_fetched_at=last_fetched_at,
        latest_s3_json_key=latest_s3_json_key,
        latest_s3_meta_key=latest_s3_meta_key,
        source_url=source_url,
    )


def is_after_cursor(
    pointer: LiveResultsStatePointer,
    *,
    cursor_fetch_ts: str | None,
    cursor_s3_key: str | None,
) -> bool:
    if not cursor_fetch_ts:
        return True
    cursor = (cursor_fetch_ts, cursor_s3_key or "")
    return pointer.cursor_tuple() > cursor


def iter_live_results_state_pointers(
    *,
    table_name: str,
    allowed_statuses: set[str] | None = None,
    cursor_fetch_ts: str | None = None,
    cursor_s3_key: str | None = None,
    aws_region: str | None = None,
) -> Iterator[LiveResultsStatePointer]:
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    allowed = {status.strip().lower() for status in (allowed_statuses or DEFAULT_ALLOWED_FETCH_STATUSES) if status.strip()}
    if not allowed:
        raise ValueError("allowed_statuses cannot be empty")

    last_evaluated_key = None
    while True:
        scan_kwargs: dict[str, Any] = {
            "FilterExpression": Attr("sk").begins_with("LIVE_RESULTS#"),
            "ProjectionExpression": (
                "event_id, division, round_number, fetch_status, content_sha256, "
                "latest_s3_json_key, latest_s3_meta_key, last_fetched_at, source_url, pk, sk"
            ),
        }
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        resp = table.scan(**scan_kwargs)

        for item in resp.get("Items", []):
            pointer = build_pointer_from_state_item(item)
            if pointer is None:
                continue
            if pointer.fetch_status not in allowed:
                continue
            if not is_after_cursor(pointer, cursor_fetch_ts=cursor_fetch_ts, cursor_s3_key=cursor_s3_key):
                continue
            yield pointer

        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break


def collect_live_results_state_pointers(
    *,
    table_name: str,
    allowed_statuses: set[str] | None = None,
    cursor_fetch_ts: str | None = None,
    cursor_s3_key: str | None = None,
    limit: int | None = None,
    aws_region: str | None = None,
) -> list[LiveResultsStatePointer]:
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive when provided")

    pointers = list(
        iter_live_results_state_pointers(
            table_name=table_name,
            allowed_statuses=allowed_statuses,
            cursor_fetch_ts=cursor_fetch_ts,
            cursor_s3_key=cursor_s3_key,
            aws_region=aws_region,
        )
    )
    pointers.sort(
        key=lambda p: (
            p.last_fetched_at,
            p.latest_s3_json_key,
            p.event_id,
            p.division,
            p.round_number,
        )
    )
    if limit is not None:
        return pointers[:limit]
    return pointers


def latest_pointer_per_round(
    pointers: Iterable[LiveResultsStatePointer],
) -> list[LiveResultsStatePointer]:
    by_unit: dict[tuple[int, str, int], LiveResultsStatePointer] = {}

    for pointer in pointers:
        key = pointer.unit_key()
        existing = by_unit.get(key)
        if existing is None or pointer.cursor_tuple() > existing.cursor_tuple():
            by_unit[key] = pointer

    out = list(by_unit.values())
    out.sort(
        key=lambda p: (
            p.last_fetched_at,
            p.latest_s3_json_key,
            p.event_id,
            p.division,
            p.round_number,
        )
    )
    return out