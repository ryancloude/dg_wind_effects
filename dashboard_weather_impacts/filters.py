from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import streamlit as st


@dataclass(frozen=True)
class DashboardFilters:
    years: list[int]
    months: list[int]
    divisions: list[str]
    rating_bands: list[str]
    states: list[str]
    min_rounds: int


def empty_filters() -> DashboardFilters:
    return DashboardFilters(
        years=[],
        months=[],
        divisions=[],
        rating_bands=[],
        states=[],
        min_rounds=0,
    )


def _sorted_unique(df: pd.DataFrame, col: str) -> list:
    if df.empty or col not in df.columns:
        return []
    values = [x for x in df[col].dropna().unique().tolist() if x not in ("", "UNKNOWN", "__MISSING__")]
    return sorted(values)


def render_global_filters(datasets: dict[str, pd.DataFrame]) -> DashboardFilters:
    state_source = datasets.get("weather_by_state", pd.DataFrame())
    event_source = datasets.get("weather_by_event", pd.DataFrame())
    division_source = datasets.get("weather_by_division", pd.DataFrame())
    rating_source = datasets.get("weather_by_rating_band", pd.DataFrame())

    years = _sorted_unique(state_source, "round_year")
    months = _sorted_unique(state_source, "round_month")
    divisions = _sorted_unique(event_source, "division") if "division" in event_source.columns else _sorted_unique(division_source, "division")
    rating_bands = _sorted_unique(rating_source, "rating_band")
    states = _sorted_unique(state_source, "state")

    st.sidebar.header("Filters")

    selected_years = st.sidebar.multiselect("Year", years, default=years) if years else []
    selected_months = st.sidebar.multiselect("Month", months, default=months) if months else []
    selected_divisions = st.sidebar.multiselect("Division", divisions, default=divisions) if divisions else []
    selected_rating_bands = st.sidebar.multiselect("Rating Band", rating_bands, default=rating_bands) if rating_bands else []
    selected_states = st.sidebar.multiselect("State", states, default=states) if states else []

    min_rounds_default = 25
    min_rounds_max = 1000
    min_rounds = st.sidebar.slider(
        "Minimum Rounds",
        min_value=0,
        max_value=min_rounds_max,
        value=min_rounds_default,
        step=5,
    )

    return DashboardFilters(
        years=selected_years,
        months=selected_months,
        divisions=selected_divisions,
        rating_bands=selected_rating_bands,
        states=selected_states,
        min_rounds=min_rounds,
    )


def apply_common_filters(df: pd.DataFrame, filters: DashboardFilters) -> pd.DataFrame:
    out = df.copy()
    if "round_year" in out.columns and filters.years:
        out = out[out["round_year"].isin(filters.years)]
    if "round_month" in out.columns and filters.months:
        out = out[out["round_month"].isin(filters.months)]
    if "division" in out.columns and filters.divisions:
        out = out[out["division"].isin(filters.divisions)]
    if "rating_band" in out.columns and filters.rating_bands:
        out = out[out["rating_band"].isin(filters.rating_bands)]
    if "state" in out.columns and filters.states:
        out = out[out["state"].isin(filters.states)]
    if "rounds_scored" in out.columns:
        out = out[out["rounds_scored"] >= filters.min_rounds]
    return out
