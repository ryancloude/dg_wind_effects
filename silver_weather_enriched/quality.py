from __future__ import annotations

from typing import Any

from silver_weather_enriched.models import ENRICHED_HOLE_WEATHER_COLS, ENRICHED_ROUND_WEATHER_COLS

# Keep local to avoid runtime dependency on silver_pdga_live_results package.
ROUND_PK_COLS = ("tourn_id", "round_number", "player_key")
HOLE_PK_COLS = ("tourn_id", "round_number", "hole_number", "player_key")


def _as_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _append_error(errors: list[dict[str, Any]], *, rule: str, message: str, sample: dict[str, Any] | None = None) -> None:
    payload = {"rule": rule, "message": message}
    if sample is not None:
        payload["sample"] = sample
    errors.append(payload)


def _pk_set(rows: list[dict[str, Any]], pk_cols: tuple[str, ...]) -> set[tuple[Any, ...]]:
    return {tuple(r.get(c) for c in pk_cols) for r in rows}


def validate_enriched_quality(
    *,
    round_input_rows: list[dict[str, Any]],
    hole_input_rows: list[dict[str, Any]],
    round_output_rows: list[dict[str, Any]],
    hole_output_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []

    if len(round_input_rows) != len(round_output_rows):
        _append_error(
            errors,
            rule="row_count:round",
            message=f"round output row count mismatch input={len(round_input_rows)} output={len(round_output_rows)}",
        )

    if len(hole_input_rows) != len(hole_output_rows):
        _append_error(
            errors,
            rule="row_count:hole",
            message=f"hole output row count mismatch input={len(hole_input_rows)} output={len(hole_output_rows)}",
        )

    round_input_pks = _pk_set(round_input_rows, ROUND_PK_COLS)
    round_output_pks = _pk_set(round_output_rows, ROUND_PK_COLS)
    if round_input_pks != round_output_pks:
        _append_error(
            errors,
            rule="pk_preservation:round",
            message="round PK set changed during enrichment",
        )

    hole_input_pks = _pk_set(hole_input_rows, HOLE_PK_COLS)
    hole_output_pks = _pk_set(hole_output_rows, HOLE_PK_COLS)
    if hole_input_pks != hole_output_pks:
        _append_error(
            errors,
            rule="pk_preservation:hole",
            message="hole PK set changed during enrichment",
        )

    for row in round_output_rows:
        missing = [c for c in ENRICHED_ROUND_WEATHER_COLS if c not in row]
        if missing:
            _append_error(
                errors,
                rule="columns:round_weather_cols",
                message=f"round row missing weather columns: {missing}",
                sample={k: row.get(k) for k in ROUND_PK_COLS},
            )
            break

    for row in hole_output_rows:
        missing = [c for c in ENRICHED_HOLE_WEATHER_COLS if c not in row]
        if missing:
            _append_error(
                errors,
                rule="columns:hole_weather_cols",
                message=f"hole row missing weather columns: {missing}",
                sample={k: row.get(k) for k in HOLE_PK_COLS},
            )
            break

    bad_round_flag = [r for r in round_output_rows if _as_text(r.get("wx_weather_missing_flag")) == ""]
    if bad_round_flag:
        _append_error(errors, rule="not_null:wx_weather_missing_flag:round", message="round missing weather flag null")

    bad_hole_flag = [r for r in hole_output_rows if _as_text(r.get("wx_weather_missing_flag")) == ""]
    if bad_hole_flag:
        _append_error(errors, rule="not_null:wx_weather_missing_flag:hole", message="hole missing weather flag null")

    return errors