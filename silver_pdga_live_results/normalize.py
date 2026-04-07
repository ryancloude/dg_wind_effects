from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from silver_pdga_live_results.models import BronzeRoundSource

DATE_PREFIX_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")
GLOBAL_MEDIAN_LAG_MINUTES = 449
FIXED_ROUND_DURATION_MINUTES = 240


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


def _parse_ts(text: str) -> datetime | None:
    value = _normalize_text(text)
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _parse_iso_date(raw: Any) -> datetime.date | None:
    text = _normalize_text(raw)
    if not text:
        return None

    if DATE_PREFIX_RE.match(text):
        try:
            return datetime.strptime(text[:10], "%Y-%m-%d").date()
        except ValueError:
            return None

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.date()
    except ValueError:
        return None


def _parse_tee_clock(raw: str) -> tuple[int, int, int] | None:
    value = _normalize_text(raw)
    if not value:
        return None

    for fmt in ("%H:%M:%S", "%H:%M", "%I:%M %p", "%I:%M:%S %p"):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.hour, dt.minute, dt.second
        except ValueError:
            continue
    return None


def _format_ts(dt: datetime | None) -> str:
    if dt is None:
        return ""
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


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


def _build_tee_time_join(
    *,
    tee_time_raw: str,
    round_date_interp: str,
    tee_time_est_ts: str,
    scorecard_updated_at_ts: str,
    event_start_date: str,
) -> dict[str, Any]:
    tee_clock = _parse_tee_clock(tee_time_raw)
    round_date = _parse_iso_date(round_date_interp)

    if tee_clock is not None and round_date is not None:
        hour, minute, second = tee_clock
        join_dt = datetime.combine(round_date, datetime.min.time()).replace(hour=hour, minute=minute, second=second)
        return {
            "tee_time_join_ts": _format_ts(join_dt),
            "tee_time_join_method": "round_date_interp_plus_raw_tee",
            "tee_time_join_confidence": 1.00,
        }

    if not _normalize_text(tee_time_raw):
        if _parse_ts(scorecard_updated_at_ts) is not None and _parse_ts(tee_time_est_ts) is not None:
            return {
                "tee_time_join_ts": tee_time_est_ts,
                "tee_time_join_method": "fallback_score_based",
                "tee_time_join_confidence": 0.55,
            }

        if round_date is not None:
            noon_dt = datetime.combine(round_date, datetime.min.time()).replace(hour=12, minute=0, second=0)
            return {
                "tee_time_join_ts": _format_ts(noon_dt),
                "tee_time_join_method": "round_date_interp_noon_fallback",
                "tee_time_join_confidence": 0.30,
            }

    start_date = _parse_iso_date(event_start_date)
    if start_date is not None:
        noon_dt = datetime.combine(start_date, datetime.min.time()).replace(hour=12, minute=0, second=0)
        return {
            "tee_time_join_ts": _format_ts(noon_dt),
            "tee_time_join_method": "event_start_noon_fallback",
            "tee_time_join_confidence": 0.20,
        }

    return {
        "tee_time_join_ts": "",
        "tee_time_join_method": "missing_inputs",
        "tee_time_join_confidence": 0.00,
    }


def _derive_max_round_number(event_metadata: dict[str, Any], round_sources: list[BronzeRoundSource]) -> int:
    max_round = 1

    division_rounds = event_metadata.get("division_rounds")
    if isinstance(division_rounds, dict):
        for value in division_rounds.values():
            v = _to_int(value)
            if v is not None and v > max_round:
                max_round = v

    for src in round_sources:
        if src.round_number > max_round:
            max_round = int(src.round_number)

        payload = src.payload if isinstance(src.payload, dict) else {}
        data = payload.get("data") if isinstance(payload, dict) else {}
        scores = data.get("scores") if isinstance(data, dict) else None
        if isinstance(scores, list):
            for score in scores:
                if not isinstance(score, dict):
                    continue
                r = _to_int(score.get("Round"))
                if r is not None and r > max_round:
                    max_round = r

    return max_round


def _compute_round_date_interp(
    *,
    start_date_raw: str,
    end_date_raw: str,
    round_number: int,
    max_round_number: int,
) -> tuple[str, str, float]:
    start_date = _parse_iso_date(start_date_raw)
    if start_date is None:
        return "", "missing_start_date", 0.30

    end_date = _parse_iso_date(end_date_raw)
    if end_date is None:
        return start_date.isoformat(), "event_start_fallback_no_end_date", 0.50

    span_days = (end_date - start_date).days
    if span_days <= 0 or max_round_number <= 1:
        return start_date.isoformat(), "event_start_single_day", 1.00

    offset_days = ((int(round_number) - 1) * span_days) // (max_round_number - 1)
    interp_date = start_date + timedelta(days=int(offset_days))
    return interp_date.isoformat(), "event_span_linear", 0.70


def _estimate_tee_time(
    *,
    tee_time_raw: str,
    scorecard_updated_at_ts: str,
    round_date_interp: str,
) -> dict[str, Any]:
    score_dt = _parse_ts(scorecard_updated_at_ts)
    tee_clock = _parse_tee_clock(tee_time_raw)

    if tee_clock is not None:
        hour, minute, second = tee_clock

        if score_dt is not None:
            tee_dt = score_dt.replace(hour=hour, minute=minute, second=second, microsecond=0)
            if tee_dt > score_dt:
                tee_dt = tee_dt - timedelta(days=1)

            lag_min = max(int((score_dt - tee_dt).total_seconds() // 60), 0)
            return {
                "tee_time_est_ts": _format_ts(tee_dt),
                "tee_time_est_method": "raw_tee_time",
                "tee_time_est_confidence": 1.00,
                "lag_minutes_used": lag_min,
                "lag_bucket_used": "raw",
                "lag_sample_size": None,
                "round_duration_est_minutes": FIXED_ROUND_DURATION_MINUTES,
            }

        interp_date = _parse_iso_date(round_date_interp)
        if interp_date is not None:
            tee_dt = datetime.combine(interp_date, datetime.min.time()).replace(hour=hour, minute=minute, second=second)
            return {
                "tee_time_est_ts": _format_ts(tee_dt),
                "tee_time_est_method": "raw_tee_time_no_score_ts",
                "tee_time_est_confidence": 0.90,
                "lag_minutes_used": None,
                "lag_bucket_used": "raw",
                "lag_sample_size": None,
                "round_duration_est_minutes": FIXED_ROUND_DURATION_MINUTES,
            }

    if score_dt is not None:
        tee_dt = score_dt - timedelta(minutes=GLOBAL_MEDIAN_LAG_MINUTES)
        return {
            "tee_time_est_ts": _format_ts(tee_dt),
            "tee_time_est_method": "score_minus_global_median_lag",
            "tee_time_est_confidence": 0.55,
            "lag_minutes_used": GLOBAL_MEDIAN_LAG_MINUTES,
            "lag_bucket_used": "global",
            "lag_sample_size": None,
            "round_duration_est_minutes": FIXED_ROUND_DURATION_MINUTES,
        }

    return {
        "tee_time_est_ts": "",
        "tee_time_est_method": "missing_inputs",
        "tee_time_est_confidence": 0.00,
        "lag_minutes_used": None,
        "lag_bucket_used": "none",
        "lag_sample_size": None,
        "round_duration_est_minutes": FIXED_ROUND_DURATION_MINUTES,
    }


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


def _estimate_hole_times(
    *,
    tee_time_est_ts: str,
    round_duration_est_minutes: int | None,
    hole_numbers: list[int],
) -> dict[int, tuple[str, str]]:
    if not hole_numbers:
        return {}

    round_start_dt = _parse_ts(tee_time_est_ts)
    if round_start_dt is None or round_duration_est_minutes is None or round_duration_est_minutes <= 0:
        return {}

    ordered = sorted(hole_numbers)
    n = len(ordered)
    total_seconds = int(round_duration_est_minutes * 60)
    per_hole_seconds = max(total_seconds // n, 1)

    out: dict[int, tuple[str, str]] = {}
    for idx, hole_num in enumerate(ordered):
        start_dt = round_start_dt + timedelta(seconds=idx * per_hole_seconds)
        if idx == n - 1:
            end_dt = round_start_dt + timedelta(seconds=total_seconds)
        else:
            end_dt = round_start_dt + timedelta(seconds=(idx + 1) * per_hole_seconds)

        out[hole_num] = (_format_ts(start_dt), _format_ts(end_dt))

    return out


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
    max_round_number = _derive_max_round_number(event_metadata, round_sources)

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

            round_date_interp, round_date_interp_method, round_date_interp_confidence = _compute_round_date_interp(
                start_date_raw=event_start_date,
                end_date_raw=event_end_date,
                round_number=int(round_number),
                max_round_number=max_round_number,
            )

            # Dashboard-friendly canonical round date alias.
            round_date = round_date_interp

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

            tee_time_raw = _normalize_text(score.get("TeeTime"))
            tee_est = _estimate_tee_time(
                tee_time_raw=tee_time_raw,
                scorecard_updated_at_ts=scorecard_updated_at_ts,
                round_date_interp=round_date_interp,
            )

            tee_join = _build_tee_time_join(
                tee_time_raw=tee_time_raw,
                round_date_interp=round_date_interp,
                tee_time_est_ts=tee_est["tee_time_est_ts"],
                scorecard_updated_at_ts=scorecard_updated_at_ts,
                event_start_date=event_start_date,
            )

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
                "player_rating": _to_int(score.get("Rating")),
                "player_full_location": _normalize_text(score.get("FullLocation")),
                "event_name": event_name,
                "event_status_text": event_status_text,
                "event_start_date": event_start_date,
                "event_end_date": event_end_date,
                "round_date": round_date,
                "round_date_interp": round_date_interp,
                "round_date_interp_method": round_date_interp_method,
                "round_date_interp_confidence": round_date_interp_confidence,
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
                "tee_time_raw": tee_time_raw,
                "tee_time_sort": _normalize_text(score.get("TeeTimeSort")),
                "tee_time_est_ts": tee_est["tee_time_est_ts"],
                "tee_time_est_method": tee_est["tee_time_est_method"],
                "tee_time_est_confidence": tee_est["tee_time_est_confidence"],
                "tee_time_join_ts": tee_join["tee_time_join_ts"],
                "tee_time_join_method": tee_join["tee_time_join_method"],
                "tee_time_join_confidence": tee_join["tee_time_join_confidence"],
                "lag_minutes_used": tee_est["lag_minutes_used"],
                "lag_bucket_used": tee_est["lag_bucket_used"],
                "lag_sample_size": tee_est["lag_sample_size"],
                "round_duration_est_minutes": tee_est["round_duration_est_minutes"],
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
            scored_hole_numbers = [idx for idx, value in enumerate(hole_scores, start=1) if value is not None]
            hole_time_map = _estimate_hole_times(
                tee_time_est_ts=tee_est["tee_time_est_ts"],
                round_duration_est_minutes=tee_est["round_duration_est_minutes"],
                hole_numbers=scored_hole_numbers,
            )

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
                    "first_name": _normalize_text(score.get("FirstName")),
                    "last_name": _normalize_text(score.get("LastName")),
                    "short_name": _normalize_text(score.get("ShortName")),
                    "profile_url": _normalize_text(score.get("ProfileURL")),
                    "player_city": _normalize_text(score.get("City")),
                    "player_state_prov": _normalize_text(score.get("StateProv")),
                    "player_country": _normalize_text(score.get("Country")),
                    "player_full_location": _normalize_text(score.get("FullLocation")),
                    "player_rating": _to_int(score.get("Rating")),
                    "event_name": event_name,
                    "event_location_raw": event_location_raw,
                    "event_city": event_city,
                    "event_state": event_state,
                    "event_country": event_country,
                    "event_start_date": event_start_date,
                    "event_end_date": event_end_date,
                    "round_date": round_date,
                    "round_date_interp": round_date_interp,
                    "round_date_interp_method": round_date_interp_method,
                    "round_date_interp_confidence": round_date_interp_confidence,
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
                    "tee_time_join_ts": tee_join["tee_time_join_ts"],
                    "tee_time_join_method": tee_join["tee_time_join_method"],
                    "tee_time_join_confidence": tee_join["tee_time_join_confidence"],
                    "tee_time_raw": tee_time_raw,
                    "tee_time_est_ts": tee_est["tee_time_est_ts"],
                    "tee_time_est_method": tee_est["tee_time_est_method"],
                    "tee_time_est_confidence": tee_est["tee_time_est_confidence"],
                    "lag_minutes_used": tee_est["lag_minutes_used"],
                    "lag_bucket_used": tee_est["lag_bucket_used"],
                    "round_duration_est_minutes": tee_est["round_duration_est_minutes"],
                    "hole_start_est_ts": hole_time_map.get(hole_number, ("", ""))[0],
                    "hole_end_est_ts": hole_time_map.get(hole_number, ("", ""))[1],
                    "hole_time_est_method": "uniform_from_round_duration" if hole_number in hole_time_map else "missing_round_time_inputs",
                    "hole_time_est_confidence": 0.60 if hole_number in hole_time_map else 0.00,
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
