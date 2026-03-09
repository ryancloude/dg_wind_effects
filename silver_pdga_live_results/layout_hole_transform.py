from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        try:
            if value != value.to_integral_value():
                return None
            return int(value)
        except (InvalidOperation, ValueError):
            return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError:
            return None
    return None


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
        try:
            return float(value)
        except (InvalidOperation, ValueError):
            return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _row_hash_payload(row: dict[str, Any]) -> dict[str, Any]:
    excluded = {"layout_row_hash", "silver_run_id", "silver_loaded_at"}
    return {key: row.get(key) for key in sorted(row.keys()) if key not in excluded}


def _compute_layout_row_hash(row: dict[str, Any]) -> str:
    payload = _row_hash_payload(row)
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def normalize_layout_hole_rows(
    *,
    pointer,
    payload: dict[str, Any] | list[Any],
    run_id: str,
    loaded_at_iso: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    if not isinstance(payload, dict):
        raise ValueError("live_results_payload_must_be_object")

    data = payload.get("data")
    if not isinstance(data, dict):
        raise ValueError("live_results_payload_missing_data_object")

    layouts = data.get("layouts")
    holes = data.get("holes")
    if not isinstance(layouts, list):
        raise ValueError("live_results_payload_data_layouts_must_be_list")
    if not isinstance(holes, list):
        raise ValueError("live_results_payload_data_holes_must_be_list")

    loaded_at = loaded_at_iso or utc_now_iso()
    source_hash = _clean_str(payload.get("hash"))

    stats = {
        "total_layouts": len(layouts),
        "total_holes": len(holes),
        "output_rows": 0,
        "skipped_invalid_layouts": 0,
        "skipped_invalid_holes": 0,
    }

    out: list[dict[str, Any]] = []

    for layout in layouts:
        if not isinstance(layout, dict):
            stats["skipped_invalid_layouts"] += 1
            continue

        layout_id = _coerce_int(layout.get("LayoutID"))
        if layout_id is None or layout_id <= 0:
            stats["skipped_invalid_layouts"] += 1
            continue

        for hole in holes:
            if not isinstance(hole, dict):
                stats["skipped_invalid_holes"] += 1
                continue

            hole_ordinal = _coerce_int(hole.get("HoleOrdinal"))
            if hole_ordinal is None:
                hole_ordinal = _coerce_int(hole.get("Ordinal"))

            hole_par = _coerce_int(hole.get("Par"))
            if hole_ordinal is None or hole_ordinal <= 0 or hole_par is None:
                stats["skipped_invalid_holes"] += 1
                continue

            row = {
                "layout_id": layout_id,
                "hole_ordinal": hole_ordinal,
                "hole_code": _clean_str(hole.get("Hole")),
                "hole_label": _clean_str(hole.get("Label")),
                "hole_par": hole_par,
                "hole_length": _coerce_int(hole.get("Length")),
                "layout_name": _clean_str(layout.get("Name")),
                "layout_hole_count": _coerce_int(layout.get("Holes")),
                "layout_par_total": _coerce_int(layout.get("Par")),
                "layout_length_total": _coerce_int(layout.get("Length")),
                "layout_units": _clean_str(layout.get("Units")),
                "course_id": _coerce_int(layout.get("CourseID")),
                "course_name": _clean_str(layout.get("CourseName")),
                "challenge_factor": _coerce_float(layout.get("ChallengeFactor")),
                "provisional_ssa": _coerce_float(layout.get("ProvisionalSSA")),
                "combined_ssa": _coerce_float(layout.get("CombinedSSA")),
                "layout_update_at_raw": _clean_str(layout.get("UpdateDate")),
                "source_event_id": pointer.event_id,
                "source_division_code": pointer.division,
                "source_round_number": pointer.round_number,
                "source_api_hash": source_hash,
                "source_url": pointer.source_url,
                "source_fetch_ts": pointer.last_fetched_at,
                "source_content_sha256": pointer.content_sha256,
                "bronze_s3_json_key": pointer.latest_s3_json_key,
                "silver_run_id": run_id,
                "silver_loaded_at": loaded_at,
            }

            row["layout_row_hash"] = _compute_layout_row_hash(row)
            out.append(row)
            stats["output_rows"] += 1

    return out, stats


def validate_layout_hole_rows(rows: list[dict[str, Any]]) -> None:
    seen: set[tuple[int, int]] = set()

    for row in rows:
        key = (row.get("layout_id"), row.get("hole_ordinal"))

        if key[0] is None or key[1] is None:
            raise ValueError(f"layout_hole_missing_primary_key_fields:{key}")
        if row.get("hole_par") is None:
            raise ValueError(f"layout_hole_missing_hole_par:{key}")
        if key in seen:
            raise ValueError(f"layout_hole_duplicate_primary_key:{key}")

        seen.add(key)


def group_rows_by_layout(rows: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        layout_id = int(row["layout_id"])
        grouped.setdefault(layout_id, []).append(row)
    return grouped


def compute_layout_group_hash(rows: list[dict[str, Any]]) -> str:
    ordered = sorted(rows, key=lambda row: int(row.get("hole_ordinal") or 0))
    hashes = [str(row.get("layout_row_hash", "")) for row in ordered]
    serialized = json.dumps(hashes, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def transform_layout_hole_rows(
    *,
    pointer,
    payload: dict[str, Any] | list[Any],
    run_id: str,
    loaded_at_iso: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows, stats = normalize_layout_hole_rows(
        pointer=pointer,
        payload=payload,
        run_id=run_id,
        loaded_at_iso=loaded_at_iso,
    )
    validate_layout_hole_rows(rows)
    return rows, stats