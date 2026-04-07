from __future__ import annotations

from typing import Iterable

import pandas as pd


def _aggregate(
    df: pd.DataFrame,
    *,
    group_cols: list[str],
    include_players: bool = False,
) -> pd.DataFrame:
    agg_spec: dict[str, tuple[str, str]] = {
        "rounds_scored": ("player_key", "count"),
        "sum_observed_wind_mph": ("observed_wind_mph", "sum"),
        "sum_observed_temp_f": ("observed_temp_f", "sum"),
        "sum_actual_round_strokes": ("actual_round_strokes", "sum"),
        "sum_predicted_round_strokes": ("predicted_round_strokes", "sum"),
        "sum_predicted_round_strokes_wind_reference": ("predicted_round_strokes_wind_reference", "sum"),
        "sum_estimated_wind_impact_strokes": ("estimated_wind_impact_strokes", "sum"),
        "sum_estimated_temperature_impact_strokes": ("estimated_temperature_impact_strokes", "sum"),
        "sum_estimated_total_weather_impact_strokes": ("estimated_total_weather_impact_strokes", "sum"),
    }
    if include_players:
        agg_spec["players_scored"] = ("player_key", pd.Series.nunique)

    out = (
        df.groupby(group_cols, dropna=False)
        .agg(**agg_spec)
        .reset_index()
    )
    out["events_scored"] = 1
    return out


def build_event_contributions(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    base_event_cols = ["event_year", "tourn_id"]

    outputs: dict[str, pd.DataFrame] = {}
    outputs["weather_overview"] = _aggregate(df, group_cols=base_event_cols)
    outputs["weather_by_wind_bucket"] = _aggregate(
        df,
        group_cols=base_event_cols + ["state", "round_year", "round_month", "round_month_label", "division", "rating_band", "round_wind_speed_bucket"],
    )
    outputs["weather_by_temperature_band"] = _aggregate(
        df,
        group_cols=base_event_cols + ["state", "round_year", "round_month", "round_month_label", "division", "rating_band", "temperature_band_f"],
    )
    outputs["weather_by_month"] = _aggregate(
        df,
        group_cols=base_event_cols + ["state", "round_year", "round_month", "round_month_label", "division", "rating_band"],
    )
    outputs["weather_by_event_geo"] = _aggregate(
        df,
        group_cols=base_event_cols + ["event_name", "state", "city", "lat", "lon", "round_year", "round_month", "round_month_label"],
        include_players=True,
    )
    outputs["weather_by_state"] = _aggregate(
        df,
        group_cols=base_event_cols + ["state", "round_year", "round_month", "round_month_label", "division", "rating_band"],
    )
    outputs["weather_by_division"] = _aggregate(
        df,
        group_cols=base_event_cols + ["state", "round_year", "round_month", "round_month_label", "division"],
    )
    outputs["weather_by_rating_band"] = _aggregate(
        df,
        group_cols=base_event_cols + ["state", "round_year", "round_month", "round_month_label", "rating_band"],
    )
    outputs["weather_by_course_layout"] = _aggregate(
        df,
        group_cols=base_event_cols + ["state", "round_year", "round_month", "round_month_label", "division", "course_id", "layout_id"],
    )
    outputs["weather_by_event"] = _aggregate(
        df,
        group_cols=base_event_cols + ["event_name", "state", "city", "round_year", "round_month", "round_month_label"],
        include_players=True,
    )
    outputs["weather_by_event_round"] = _aggregate(
        df,
        group_cols=base_event_cols + ["state", "round_year", "round_month", "round_month_label", "round_number", "division"],
        include_players=True,
    )
    return outputs
