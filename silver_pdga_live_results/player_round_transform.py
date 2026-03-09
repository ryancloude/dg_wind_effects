from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from silver_pdga_live_results.candidate_reader import LiveResultsStatePointer


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


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, Decimal)):
        parsed = _coerce_int(value)
        if parsed in (0, 1):
            return bool(parsed)
        return None
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("1", "true", "yes", "y"):
            return True
        if lowered in ("0", "false", "no", "n"):
            return False
    return None


def _player_name(score_item: dict[str, Any]) -> str:
    direct = _clean_str(score_item.get("Name"))
    if direct:
        return direct

    first_name = _clean_str(score_item.get("FirstName"))
    last_name = _clean_str(score_item.get("LastName"))
    full_name = " ".join([part for part in (first_name, last_name) if part]).strip()
    return full_name if full_name else "UNKNOWN"


def _row_hash_payload(row: dict[str, Any]) -> dict[str, Any]:
    excluded = {"silver_row_hash", "silver_run_id", "silver_loaded_at"}
    return {key: row.get(key) for key in sorted(row.keys()) if key not in excluded}


def _compute_row_hash(row: dict[str, Any]) -> str:
    payload = _row_hash_payload(row)
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def normalize_player_round_rows(
    *,
    pointer: LiveResultsStatePointer,
    payload: dict[str, Any] | list[Any],
    run_id: str,
    loaded_at_iso: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    if not isinstance(payload, dict):
        raise ValueError("live_results_payload_must_be_object")

    data = payload.get("data")
    if not isinstance(data, dict):
        raise ValueError("live_results_payload_missing_data_object")

    scores = data.get("scores")
    if not isinstance(scores, list):
        raise ValueError("live_results_payload_data_scores_must_be_list")

    loaded_at = loaded_at_iso or utc_now_iso()
    source_hash = _clean_str(payload.get("hash"))
    live_round_id = _coerce_int(data.get("live_round_id"))

    stats = {
        "total_scores": len(scores),
        "output_rows": 0,
        "skipped_non_object_scores": 0,
        "skipped_missing_result_id": 0,
    }

    out: list[dict[str, Any]] = []

    for score in scores:
        if not isinstance(score, dict):
            stats["skipped_non_object_scores"] += 1
            continue

        result_id = _coerce_int(score.get("ResultID"))
        if result_id is None or result_id <= 0:
            stats["skipped_missing_result_id"] += 1
            continue

        round_number = _coerce_int(score.get("Round")) or pointer.round_number
        if round_number <= 0:
            round_number = pointer.round_number

        round_score = _coerce_int(score.get("RoundScore"))
        round_par = _coerce_int(score.get("Par"))
        round_to_par = _coerce_int(score.get("RoundtoPar"))

        round_par_consistent: bool | None = None
        if round_score is not None and round_par is not None and round_to_par is not None:
            round_par_consistent = (round_score - round_par) == round_to_par

        row = {
            "event_id": pointer.event_id,
            "division_code": pointer.division,
            "round_number": round_number,
            "result_id": result_id,
            "round_id": _coerce_int(score.get("RoundID")),
            "layout_id": _coerce_int(score.get("LayoutID")),
            "live_round_id": live_round_id,
            "pdga_number": _coerce_int(score.get("PDGANum")),
            "player_name": _player_name(score),
            "first_name": _clean_str(score.get("FirstName")),
            "last_name": _clean_str(score.get("LastName")),
            "rating": _coerce_int(score.get("Rating")),
            "city": _clean_str(score.get("City")),
            "state_prov": _clean_str(score.get("StateProv")),
            "country": _clean_str(score.get("Country")),
            "nationality": _clean_str(score.get("Nationality")),
            "pool": _clean_str(score.get("Pool")),
            "card_num": _coerce_int(score.get("CardNum")),
            "tee_start": _clean_str(score.get("TeeStart")),
            "tee_time_raw": _clean_str(score.get("TeeTime")),
            "tee_time_sort_raw": _clean_str(score.get("TeeTimeSort")),
            "round_status": _clean_str(score.get("RoundStatus")),
            "player_throw_status": _clean_str(score.get("PlayerThrowStatus")),
            "played_holes": _coerce_int(score.get("Played")),
            "is_completed": _coerce_bool(score.get("Completed")),
            "round_score": round_score,
            "round_to_par": round_to_par,
            "total_to_par": _coerce_int(score.get("ToPar")),
            "round_par": round_par,
            "round_par_consistent": round_par_consistent,
            "running_place": _coerce_int(score.get("RunningPlace")),
            "previous_place": _coerce_int(score.get("PreviousPlace")),
            "tied": _coerce_bool(score.get("Tied")),
            "won_playoff": _coerce_bool(score.get("WonPlayoff")),
            "grand_total": _coerce_int(score.get("GrandTotal")),
            "prev_rnd_total": _coerce_int(score.get("PrevRndTotal")),
            "source_update_at_raw": _clean_str(score.get("UpdateDate")),
            "scorecard_updated_at_raw": _clean_str(score.get("ScorecardUpdatedAt")),
            "source_api_hash": source_hash,
            "source_url": pointer.source_url,
            "source_fetch_ts": pointer.last_fetched_at,
            "source_content_sha256": pointer.content_sha256,
            "bronze_s3_json_key": pointer.latest_s3_json_key,
            "silver_run_id": run_id,
            "silver_loaded_at": loaded_at,
        }

        row["silver_row_hash"] = _compute_row_hash(row)
        out.append(row)
        stats["output_rows"] += 1

    return out, stats


def validate_player_round_rows(rows: list[dict[str, Any]]) -> None:
    seen: set[tuple[int, str, int, int]] = set()

    for row in rows:
        key = (
            row.get("event_id"),
            row.get("division_code"),
            row.get("round_number"),
            row.get("result_id"),
        )
        if key[0] is None or not key[1] or key[2] is None or key[3] is None:
            raise ValueError(f"player_round_missing_primary_key_fields:{key}")

        if row.get("layout_id") is None:
            raise ValueError(f"player_round_missing_layout_id:{key}")

        if key in seen:
            raise ValueError(f"player_round_duplicate_primary_key:{key}")
        seen.add(key)


def transform_player_round_rows(
    *,
    pointer: LiveResultsStatePointer,
    payload: dict[str, Any] | list[Any],
    run_id: str,
    loaded_at_iso: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows, stats = normalize_player_round_rows(
        pointer=pointer,
        payload=payload,
        run_id=run_id,
        loaded_at_iso=loaded_at_iso,
    )
    validate_player_round_rows(rows)
    return rows, stats