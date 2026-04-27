from __future__ import annotations


def format_strokes(value: float | int | None) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}"


def format_mph(value: float | int | None) -> str:
    if value is None:
        return "-"
    return f"{value:.1f} mph"


def format_temp_f(value: float | int | None) -> str:
    if value is None:
        return "-"
    return f"{value:.1f} F"


def format_int(value: float | int | None) -> str:
    if value is None:
        return "-"
    return f"{int(value):,}"
