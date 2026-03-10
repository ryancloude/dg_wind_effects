from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from decimal import Decimal
from typing import Any

from silver_pdga_live_results.models import BronzeRoundSource

DATE_PREFIX_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _to_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        try:
            return int(value)
        except Exception:
            return None
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, Decimal)):
        return bool(int(value))
    text = _normalize_text(value).lower()
    if not text:
        return None
    if text in ("1", "true", "yes", "y"):
        return True
    if text in ("0", "false", "no", "n"):
        return False
    return None


def _parse_pdga_timestamp(raw: Any) -> str:
    text = _normalize_text(raw)
    if not text:
        return ""

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    return ""


def _derive_event_year(event_metadata: dict[str, Any], round_sources: list[BronzeRoundSource]) -> int:
    for key in ("end_date", "start_date"):
        value = _normalize_text(event_metadata.get(key))
        if DATE_PREFIX_RE.match(value):
            return int(value[:4])

    for src in round_sources:
        fetched = _normalize_text(src.source_fetched_at_utc)
        if DATE_PREFIX_RE.match(fetched):
            return int(fetched[:4])

    return datetime.utcnow().year


def _derive_player_key(score: dict[str, Any], event_id: int) -> tuple[str, str, int | None, int | None]:
    pdga_num = _to_int(score.get("PDGANum"))
    if pdga_num and pdga_num > 0:
        return f"PDGA#{pdga_num}", "pdga_num", pdga_num, _to_int(score.get("ResultID"))

    result_id = _to_int(score.get("ResultID"))
    if result_id and result_id > 0:
        return f"RESULT#{result_id}", "result_id", None, result_id

    parts = [
        str(int(event_id)),
        _normalize_text(score.get("Name")).lower(),
        _normalize_text(score.get("FirstName")).lower(),
        _normalize_text(score.get("LastName")).lower(),
        _normalize_text(score.get("City")).lower(),
        _normalize_text(score.get("StateProv")).lower(),
        _normalize_text(score.get("Country")).lower(),
    ]
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return f"NAMEHASH#{digest}", "namehash", None, None


def _build_layout_indexes(payload_data: dict[str, Any]) -> tuple[dict[int, dict[str, Any]], dict[int, dict[int, dict[str, Any]]], dict[int, dict[str, Any]]]:
    layout_by_id: dict[int, dict[str, Any]] = {}
    holes_by_layout: dict[int, dict[int, dict[str, Any]]] = {}

    for layout in payload_data.get("layouts", []) if isinstance(payload_data.get("layouts"), list) else []:
        if not isinstance(layout, dict):
            continue

        layout_id = _to_int(layout.get("LayoutID"))
        if layout_id is None:
            continue

        layout_by_id[layout_id] = layout

        hole_map: dict[int, dict[str, Any]] = {}
        details = layout.get("Detail")
        if isinstance(details, list):
            for hole in details:
                if not isinstance(hole, dict):
                    continue
                ord_value = _to_int(hole.get("Ordinal"))
                if ord_value is None:
                    ord_value = _to_int(hole.get("HoleOrdinal"))
                if ord_value is None or ord_value <= 0:
                    continue
                hole_map[ord_value] = hole

        if hole_map:
            holes_by_layout[layout_id] = hole_map

    fallback_hole_map: dict[int, dict[str, Any]] = {}
    for hole in payload_data.get("holes", []) if isinstance(payload_data.get("holes"), list) else []:
        if not isinstance(hole, dict):
            continue
        ord_value = _to_int(hole.get("Ordinal"))
        if ord_value is None:
            ord_value = _to_int(hole.get("HoleOrdinal"))
        if ord_value is None or ord_value <= 0:
            continue
        fallback_hole_map[ord_value] = hole

    return layout_by_id, holes_by_layout, fallback_hole_map


def _extract_hole_scores(score: dict[str, Any], layout_holes: int | None) -> list[int | None]:
    parsed: list[int | None] = []

    hole_scores = score.get("HoleScores")
    if isinstance(hole_scores, list) and hole_scores:
        parsed = [_to_int(value) for value in hole_scores]
    else:
        scores_text = _normalize_text(score.get("Scores"))
        if scores_text:
            parsed = [_to_int(token.strip()) for token in scores_text.split(",")]

    if layout_holes and layout_holes > 0 and len(parsed) > layout_holes:
        parsed = parsed[:layout_holes]

    while parsed and parsed[-1] is None:
        parsed.pop()

    return parsed


def _row_hash(row: dict[str, Any]) -> str:
    ignored = {"row_hash_sha256", "silver_run_id", "silver_processed_at_utc"}
    payload = {k: row.get(k) for k in sorted(row.keys()) if k not in ignored}
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def normalize_event_records(
    *,
    event_metadata: dict[str, Any],
    round_sources: list[BronzeRoundSource],
    event_source_fingerprint: str,
    run_id: str,
    silver_processed_at_utc: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    event_id = int(event_metadata["event_id"])
    event_year = _derive_event_year(event_metadata, round_sources)

    event_location_raw = _normalize_text(event_metadata.get("location_raw")) or _normalize_text(event_metadata.get("raw_location"))
    event_city = _normalize_text(event_metadata.get("city"))
    event_state = _normalize_text(event_metadata.get("state"))
    event_country = _normalize_text(event_metadata.get("country"))

    event_name = _normalize_text(event_metadata.get("name"))
    event_status_text = _normalize_text(event_metadata.get("status_text"))
    event_start_date = _normalize_text(event_metadata.get("start_date"))
    event_end_date = _normalize_text(event_metadata.get("end_date"))

    round_rows: list[dict[str, Any]] = []
    hole_rows: list[dict[str, Any]] = []

    for source in round_sources:
        payload = source.payload if isinstance(source.payload, dict) else {}
        data = payload.get("data") if isinstance(payload, dict) else {}
        if not isinstance(data, dict):
            continue

        scores = data.get("scores")
        if not isinstance(scores, list):
            continue

        layout_by_id, holes_by_layout, fallback_holes = _build_layout_indexes(data)

        for score in scores:
            if not isinstance(score, dict):
                continue

            round_number = _to_int(score.get("Round")) or source.round_number
            if round_number <= 0:
                continue

            division = _normalize_text(score.get("Division")) or source.division
            player_key, player_key_type, pdga_num, result_id = _derive_player_key(score, event_id)

            layout_id = _to_int(score.get("LayoutID"))
            layout = layout_by_id.get(layout_id, {})
            layout_name = _normalize_text(layout.get("Name"))
            course_id = _to_int(layout.get("CourseID"))
            course_name = _normalize_text(layout.get("CourseName"))
            layout_holes = _to_int(score.get("Holes")) or _to_int(layout.get("Holes"))
            layout_par = _to_int(layout.get("Par"))
            layout_length = _to_int(layout.get("Length"))
            layout_units = _normalize_text(layout.get("Units"))

            scorecard_updated_at_raw = _normalize_text(score.get("ScorecardUpdatedAt"))
            update_date_raw = _normalize_text(score.get("UpdateDate"))
            scorecard_updated_at_ts = _parse_pdga_timestamp(scorecard_updated_at_raw)
            update_date_ts = _parse_pdga_timestamp(update_date_raw)

            played_holes = _to_int(score.get("Played"))
            round_score = _to_int(score.get("RoundScore"))
            round_to_par = _to_int(score.get("RoundtoPar"))

            base_round = {
                "event_year": event_year,
                "tourn_id": event_id,
                "round_number": int(round_number),
                "player_key": player_key,
                "player_key_type": player_key_type,
                "pdga_num": pdga_num,
                "result_id": result_id,
                "score_id": _to_int(score.get("ScoreID")),
                "round_id": _to_int(score.get("RoundID")),
                "division": division,
                "player_name": _normalize_text(score.get("Name")),
                "first_name": _normalize_text(score.get("FirstName")),
                "last_name": _normalize_text(score.get("LastName")),
                "short_name": _normalize_text(score.get("ShortName")),
                "profile_url": _normalize_text(score.get("ProfileURL")),
                "player_city": _normalize_text(score.get("City")),
                "player_state_prov": _normalize_text(score.get("StateProv")),
                "player_country": _normalize_text(score.get("Country")),
                "player_full_location": _normalize_text(score.get("FullLocation")),
                "event_name": event_name,
                "event_status_text": event_status_text,
                "event_start_date": event_start_date,
                "event_end_date": event_end_date,
                "event_location_raw": event_location_raw,
                "event_city": event_city,
                "event_state": event_state,
                "event_country": event_country,
                "layout_id": layout_id,
                "layout_name": layout_name,
                "course_id": course_id,
                "course_name": course_name,
                "layout_holes": layout_holes,
                "layout_par": layout_par,
                "layout_length": layout_length,
                "layout_units": layout_units,
                "pool": _normalize_text(score.get("Pool")),
                "round_pool": _normalize_text(score.get("RoundPool")),
                "card_num": _to_int(score.get("CardNum")),
                "tee_start": _normalize_text(score.get("TeeStart")),
                "tee_time_raw": _normalize_text(score.get("TeeTime")),
                "tee_time_sort": _normalize_text(score.get("TeeTimeSort")),
                "played_holes": played_holes,
                "round_score": round_score,
                "round_to_par": round_to_par,
                "round_rating": _to_int(score.get("RoundRating")),
                "grand_total": _to_int(score.get("GrandTotal")),
                "to_par_total": _to_int(score.get("ToPar")),
                "prev_rnd_total": _to_int(score.get("PrevRndTotal")),
                "prev_rounds": _to_int(score.get("PrevRounds")),
                "running_place": _to_int(score.get("RunningPlace")),
                "previous_place": _to_int(score.get("PreviousPlace")),
                "round_status": _normalize_text(score.get("RoundStatus")),
                "completed_flag": _to_bool(score.get("Completed")),
                "round_started_flag": _to_bool(score.get("RoundStarted")),
                "has_round_score_flag": _to_bool(score.get("HasRoundScore")),
                "authoritative_flag": _to_bool(score.get("Authoritative")),
                "tied_flag": _to_bool(score.get("Tied")),
                "won_playoff_flag": _to_bool(score.get("WonPlayoff")),
                "scorecard_updated_at_raw": scorecard_updated_at_raw,
                "update_date_raw": update_date_raw,
                "scorecard_updated_at_ts": scorecard_updated_at_ts,
                "update_date_ts": update_date_ts,
                "source_json_key": source.source_json_key,
                "source_meta_key": source.source_meta_key or "",
                "source_content_sha256": source.source_content_sha256,
                "source_fetched_at_utc": source.source_fetched_at_utc,
                "silver_run_id": run_id,
                "silver_processed_at_utc": silver_processed_at_utc,
                "event_source_fingerprint": event_source_fingerprint,
            }
            base_round["row_hash_sha256"] = _row_hash(base_round)
            round_rows.append(base_round)

            hole_scores = _extract_hole_scores(score, layout_holes)
            hole_map = holes_by_layout.get(layout_id or -1, {}) or fallback_holes

            if played_holes is None:
                played_holes = sum(1 for value in hole_scores if value is not None)

            for hole_number, hole_score in enumerate(hole_scores, start=1):
                if hole_score is None:
                    continue

                hole_spec = hole_map.get(hole_number, {})
                hole_par = _to_int(hole_spec.get("Par"))
                if hole_par is None:
                    hole_par = _to_int(layout.get(f"H{hole_number}"))

                hole_length = _to_int(hole_spec.get("Length"))
                hole_label = _normalize_text(hole_spec.get("Label"))
                hole_code = _normalize_text(hole_spec.get("Hole")) or f"H{hole_number}"
                hole_ordinal = _to_int(hole_spec.get("Ordinal")) or hole_number

                hole_row = {
                    "event_year": event_year,
                    "tourn_id": event_id,
                    "round_number": int(round_number),
                    "hole_number": int(hole_number),
                    "player_key": player_key,
                    "player_key_type": player_key_type,
                    "pdga_num": pdga_num,
                    "result_id": result_id,
                    "score_id": _to_int(score.get("ScoreID")),
                    "round_id": _to_int(score.get("RoundID")),
                    "division": division,
                    "player_name": _normalize_text(score.get("Name")),
                    "event_location_raw": event_location_raw,
                    "event_city": event_city,
                    "event_state": event_state,
                    "event_country": event_country,
                    "event_start_date": event_start_date,
                    "event_end_date": event_end_date,
                    "layout_id": layout_id,
                    "layout_name": layout_name,
                    "course_id": course_id,
                    "course_name": course_name,
                    "layout_holes": layout_holes,
                    "hole_code": hole_code,
                    "hole_label": hole_label,
                    "hole_ordinal": hole_ordinal,
                    "hole_par": hole_par,
                    "hole_length": hole_length,
                    "hole_score": hole_score,
                    "hole_to_par": (hole_score - hole_par) if hole_par is not None else None,
                    "played_holes": played_holes,
                    "round_score": round_score,
                    "round_to_par": round_to_par,
                    "completed_flag": _to_bool(score.get("Completed")),
                    "round_status": _normalize_text(score.get("RoundStatus")),
                    "scorecard_updated_at_ts": scorecard_updated_at_ts,
                    "update_date_ts": update_date_ts,
                    "source_json_key": source.source_json_key,
                    "source_meta_key": source.source_meta_key or "",
                    "source_content_sha256": source.source_content_sha256,
                    "source_fetched_at_utc": source.source_fetched_at_utc,
                    "silver_run_id": run_id,
                    "silver_processed_at_utc": silver_processed_at_utc,
                    "event_source_fingerprint": event_source_fingerprint,
                }
                hole_row["row_hash_sha256"] = _row_hash(hole_row)
                hole_rows.append(hole_row)

    return round_rows, hole_rows