from __future__ import annotations

from typing import Any

from gold_wind_model_inputs.models import (
    MODEL_INPUTS_HOLE_REQUIRED_COLS,
    MODEL_INPUTS_ROUND_REQUIRED_COLS,
)

HOLE_PK_COLS = ("tourn_id", "round_number", "hole_number", "player_key")
ROUND_PK_COLS = ("tourn_id", "round_number", "player_key")


def _append_error(
    errors: list[dict[str, Any]],
    *,
    rule: str,
    message: str,
    sample: dict[str, Any] | None = None,
) -> None:
    payload = {"rule": rule, "message": message}
    if sample is not None:
        payload["sample"] = sample
    errors.append(payload)


def _pk_set(rows: list[dict[str, Any]], pk_cols: tuple[str, ...]) -> set[tuple[Any, ...]]:
    return {tuple(r.get(c) for c in pk_cols) for r in rows}


def _find_duplicate_pk(rows: list[dict[str, Any]], pk_cols: tuple[str, ...]) -> tuple[Any, ...] | None:
    seen: set[tuple[Any, ...]] = set()
    for row in rows:
        pk = tuple(row.get(c) for c in pk_cols)
        if pk in seen:
            return pk
        seen.add(pk)
    return None


def validate_model_inputs_quality(
    *,
    hole_input_rows: list[dict[str, Any]],
    hole_output_rows: list[dict[str, Any]],
    round_input_rows: list[dict[str, Any]] | None = None,
    round_output_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """
    Validate event-level Gold model-input outputs.
    Round rows are optional; if round input/output are omitted they are skipped.
    """
    errors: list[dict[str, Any]] = []

    round_input_rows = round_input_rows or []
    round_output_rows = round_output_rows or []

    if len(hole_input_rows) != len(hole_output_rows):
        _append_error(
            errors,
            rule="row_count:hole",
            message=f"hole output row count mismatch input={len(hole_input_rows)} output={len(hole_output_rows)}",
        )

    if round_input_rows or round_output_rows:
        if len(round_input_rows) != len(round_output_rows):
            _append_error(
                errors,
                rule="row_count:round",
                message=f"round output row count mismatch input={len(round_input_rows)} output={len(round_output_rows)}",
            )

    if _pk_set(hole_input_rows, HOLE_PK_COLS) != _pk_set(hole_output_rows, HOLE_PK_COLS):
        _append_error(errors, rule="pk_preservation:hole", message="hole PK set changed during model-input transform")

    if round_input_rows or round_output_rows:
        if _pk_set(round_input_rows, ROUND_PK_COLS) != _pk_set(round_output_rows, ROUND_PK_COLS):
            _append_error(errors, rule="pk_preservation:round", message="round PK set changed during model-input transform")

    dup_hole_pk = _find_duplicate_pk(hole_output_rows, HOLE_PK_COLS)
    if dup_hole_pk is not None:
        _append_error(
            errors,
            rule="uniqueness:hole_pk",
            message="duplicate hole PK detected in output",
            sample=dict(zip(HOLE_PK_COLS, dup_hole_pk)),
        )

    if round_output_rows:
        dup_round_pk = _find_duplicate_pk(round_output_rows, ROUND_PK_COLS)
        if dup_round_pk is not None:
            _append_error(
                errors,
                rule="uniqueness:round_pk",
                message="duplicate round PK detected in output",
                sample=dict(zip(ROUND_PK_COLS, dup_round_pk)),
            )

    for row in hole_output_rows:
        missing = [c for c in MODEL_INPUTS_HOLE_REQUIRED_COLS if c not in row]
        if missing:
            _append_error(
                errors,
                rule="columns:hole_required",
                message=f"hole row missing required columns: {missing}",
                sample={k: row.get(k) for k in HOLE_PK_COLS},
            )
            break

    for row in round_output_rows:
        missing = [c for c in MODEL_INPUTS_ROUND_REQUIRED_COLS if c not in row]
        if missing:
            _append_error(
                errors,
                rule="columns:round_required",
                message=f"round row missing required columns: {missing}",
                sample={k: row.get(k) for k in ROUND_PK_COLS},
            )
            break

    for row in hole_output_rows:
        if row.get("target_strokes_over_par") is None:
            _append_error(
                errors,
                rule="not_null:hole_target_strokes_over_par",
                message="hole row has null target_strokes_over_par",
                sample={k: row.get(k) for k in HOLE_PK_COLS},
            )
            break

    for row in round_output_rows:
        if row.get("target_strokes_over_par") is None:
            _append_error(
                errors,
                rule="not_null:round_target_strokes_over_par",
                message="round row has null target_strokes_over_par",
                sample={k: row.get(k) for k in ROUND_PK_COLS},
            )
            break

    if any(not str(r.get("row_hash_sha256", "")).strip() for r in hole_output_rows):
        _append_error(errors, rule="not_null:row_hash_sha256:hole", message="hole row missing row_hash_sha256")

    if any(not str(r.get("row_hash_sha256", "")).strip() for r in round_output_rows):
        _append_error(errors, rule="not_null:row_hash_sha256:round", message="round row missing row_hash_sha256")

    return errors