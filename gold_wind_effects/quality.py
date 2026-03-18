from __future__ import annotations

from typing import Any

from gold_wind_effects.models import (
    HOLE_PK_COLS,
    REQUIRED_GOLD_HOLE_COLS,
    REQUIRED_GOLD_ROUND_COLS,
    ROUND_PK_COLS,
)


def _append_error(errors: list[dict[str, Any]], *, rule: str, message: str, sample: dict[str, Any] | None = None) -> None:
    payload = {"rule": rule, "message": message}
    if sample is not None:
        payload["sample"] = sample
    errors.append(payload)


def _pk_set(rows: list[dict[str, Any]], pk_cols: tuple[str, ...]) -> set[tuple[Any, ...]]:
    return {tuple(r.get(c) for c in pk_cols) for r in rows}


def validate_gold_quality(
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

    if _pk_set(round_input_rows, ROUND_PK_COLS) != _pk_set(round_output_rows, ROUND_PK_COLS):
        _append_error(errors, rule="pk_preservation:round", message="round PK set changed during gold transform")
    if _pk_set(hole_input_rows, HOLE_PK_COLS) != _pk_set(hole_output_rows, HOLE_PK_COLS):
        _append_error(errors, rule="pk_preservation:hole", message="hole PK set changed during gold transform")

    for row in round_output_rows:
        missing = [c for c in REQUIRED_GOLD_ROUND_COLS if c not in row]
        if missing:
            _append_error(
                errors,
                rule="columns:round",
                message=f"round row missing required gold columns: {missing}",
                sample={k: row.get(k) for k in ROUND_PK_COLS},
            )
            break

    for row in hole_output_rows:
        missing = [c for c in REQUIRED_GOLD_HOLE_COLS if c not in row]
        if missing:
            _append_error(
                errors,
                rule="columns:hole",
                message=f"hole row missing required gold columns: {missing}",
                sample={k: row.get(k) for k in HOLE_PK_COLS},
            )
            break

    if any(not str(r.get("row_hash_sha256", "")).strip() for r in round_output_rows):
        _append_error(errors, rule="not_null:row_hash_sha256:round", message="round row missing row_hash_sha256")
    if any(not str(r.get("row_hash_sha256", "")).strip() for r in hole_output_rows):
        _append_error(errors, rule="not_null:row_hash_sha256:hole", message="hole row missing row_hash_sha256")

    return errors