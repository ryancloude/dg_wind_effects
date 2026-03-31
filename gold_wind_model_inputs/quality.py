from __future__ import annotations

from collections import defaultdict
from typing import Any

from gold_wind_model_inputs.models import (
    MODEL_INPUTS_ROUND_REQUIRED_COLS,
    ROUND_PK_COLS,
)


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


def _expected_round_pk_set_from_holes(hole_rows: list[dict[str, Any]]) -> set[tuple[Any, ...]]:
    grouped: dict[tuple[Any, ...], int] = defaultdict(int)
    for row in hole_rows:
        pk = tuple(row.get(c) for c in ROUND_PK_COLS)
        grouped[pk] += 1
    return set(grouped.keys())


def validate_model_inputs_quality(
    *,
    hole_input_rows: list[dict[str, Any]],
    round_output_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []

    expected_round_pks = _expected_round_pk_set_from_holes(hole_input_rows)
    output_round_pks = _pk_set(round_output_rows, ROUND_PK_COLS)

    if expected_round_pks != output_round_pks:
        _append_error(
            errors,
            rule="pk_preservation:round_from_holes",
            message="round PK set derived from hole inputs does not match round output PK set",
        )

    dup_round_pk = _find_duplicate_pk(round_output_rows, ROUND_PK_COLS)
    if dup_round_pk is not None:
        _append_error(
            errors,
            rule="uniqueness:round_pk",
            message="duplicate round PK detected in output",
            sample=dict(zip(ROUND_PK_COLS, dup_round_pk)),
        )

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

    for row in round_output_rows:
        if row.get("model_inputs_grain") != "round":
            _append_error(
                errors,
                rule="accepted_values:model_inputs_grain",
                message="round output row has unexpected model_inputs_grain",
                sample={k: row.get(k) for k in ROUND_PK_COLS},
            )
            break

    for row in round_output_rows:
        required_non_null = (
            "event_year",
            "actual_round_strokes",
            "hole_count",
        )
        missing_non_null = [c for c in required_non_null if row.get(c) is None]
        if missing_non_null:
            _append_error(
                errors,
                rule="not_null:round_minimum_core_fields",
                message=f"round row has null minimum core fields: {missing_non_null}",
                sample={k: row.get(k) for k in ROUND_PK_COLS},
            )
            break

    for row in round_output_rows:
        if not str(row.get("row_hash_sha256", "")).strip():
            _append_error(
                errors,
                rule="not_null:row_hash_sha256:round",
                message="round row missing row_hash_sha256",
                sample={k: row.get(k) for k in ROUND_PK_COLS},
            )
            break

    for row in round_output_rows:
        hole_count = row.get("hole_count")
        if isinstance(hole_count, int) and hole_count <= 0:
            _append_error(
                errors,
                rule="accepted_values:hole_count",
                message="round row has non-positive hole_count",
                sample={k: row.get(k) for k in ROUND_PK_COLS},
            )
            break

    for row in round_output_rows:
        bucket = row.get("round_wind_speed_bucket")
        if bucket not in ("calm", "light", "moderate", "strong", "very_strong", "unknown"):
            _append_error(
                errors,
                rule="accepted_values:round_wind_speed_bucket",
                message="round row has invalid round_wind_speed_bucket",
                sample={k: row.get(k) for k in ROUND_PK_COLS},
            )
            break

    return errors
