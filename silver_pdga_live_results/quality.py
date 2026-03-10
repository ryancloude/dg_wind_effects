from __future__ import annotations

from collections import defaultdict
from typing import Any

from silver_pdga_live_results.models import HOLE_PK_COLS, LINEAGE_REQUIRED_COLS, ROUND_PK_COLS


def _duplicate_keys(rows: list[dict[str, Any]], key_cols: tuple[str, ...]) -> list[tuple[Any, ...]]:
    seen: set[tuple[Any, ...]] = set()
    dups: set[tuple[Any, ...]] = set()

    for row in rows:
        key = tuple(row.get(col) for col in key_cols)
        if key in seen:
            dups.add(key)
        else:
            seen.add(key)

    return sorted(dups)


def validate_quality(
    *,
    round_rows: list[dict[str, Any]],
    hole_rows: list[dict[str, Any]],
) -> list[str]:
    errors: list[str] = []

    dup_round_keys = _duplicate_keys(round_rows, ROUND_PK_COLS)
    if dup_round_keys:
        errors.append(f"duplicate player_rounds keys: {len(dup_round_keys)}")

    dup_hole_keys = _duplicate_keys(hole_rows, HOLE_PK_COLS)
    if dup_hole_keys:
        errors.append(f"duplicate player_holes keys: {len(dup_hole_keys)}")

    for idx, row in enumerate(round_rows):
        for col in LINEAGE_REQUIRED_COLS:
            value = row.get(col)
            if value is None or (isinstance(value, str) and not value.strip()):
                errors.append(f"player_rounds lineage missing at row {idx}: {col}")
                break

    for idx, row in enumerate(hole_rows):
        for col in LINEAGE_REQUIRED_COLS:
            value = row.get(col)
            if value is None or (isinstance(value, str) and not value.strip()):
                errors.append(f"player_holes lineage missing at row {idx}: {col}")
                break

    parent_keys = {(row.get("tourn_id"), row.get("round_number"), row.get("player_key")) for row in round_rows}
    orphan_count = 0
    for row in hole_rows:
        parent = (row.get("tourn_id"), row.get("round_number"), row.get("player_key"))
        if parent not in parent_keys:
            orphan_count += 1
    if orphan_count:
        errors.append(f"player_holes parent missing for {orphan_count} rows")

    for idx, row in enumerate(round_rows):
        round_number = row.get("round_number")
        if round_number is None or int(round_number) < 1:
            errors.append(f"invalid round_number in player_rounds at row {idx}")

    for idx, row in enumerate(hole_rows):
        round_number = row.get("round_number")
        hole_number = row.get("hole_number")
        if round_number is None or int(round_number) < 1:
            errors.append(f"invalid round_number in player_holes at row {idx}")
        if hole_number is None or int(hole_number) < 1:
            errors.append(f"invalid hole_number in player_holes at row {idx}")
        layout_holes = row.get("layout_holes")
        if layout_holes is not None and hole_number is not None:
            try:
                if int(hole_number) > int(layout_holes):
                    errors.append(f"hole_number > layout_holes in player_holes at row {idx}")
            except (TypeError, ValueError):
                pass

    divisions_by_player: dict[tuple[Any, Any], set[str]] = defaultdict(set)
    for row in round_rows:
        key = (row.get("tourn_id"), row.get("player_key"))
        division = str(row.get("division", "")).strip()
        if division:
            divisions_by_player[key].add(division)

    collisions = sum(1 for divisions in divisions_by_player.values() if len(divisions) > 1)
    if collisions:
        errors.append(f"division collision for {collisions} event/player keys")

    return errors