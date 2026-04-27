from __future__ import annotations

import streamlit as st


def render_metric_selector(key: str = "metric_selector") -> str:
    return st.selectbox(
        "Metric",
        [
            "avg_observed_wind_mph",
            "avg_observed_temp_f",
            "avg_estimated_wind_impact_strokes",
            "avg_estimated_temperature_impact_strokes",
            "avg_estimated_total_weather_impact_strokes",
        ],
        key=key,
    )


def render_map_mode_selector(key: str = "map_mode_selector") -> str:
    return st.radio(
        "Map Mode",
        ["Event Points", "State Choropleth"],
        horizontal=True,
        key=key,
    )
