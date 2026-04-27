from __future__ import annotations

import streamlit as st

from dashboard_weather_impacts.charts import course_layout_scatter, division_bar_chart, rating_band_chart
from dashboard_weather_impacts.components.tables import render_table
from dashboard_weather_impacts.filters import DashboardFilters, apply_common_filters


def render_where_it_matters(filters: DashboardFilters, datasets: dict):
    st.title("Where It Matters")

    division_df = apply_common_filters(datasets["weather_by_division"], filters)
    rating_df = apply_common_filters(datasets["weather_by_rating_band"], filters)
    venue_df = apply_common_filters(datasets["weather_by_course_layout"], filters)

    if division_df.empty and rating_df.empty and venue_df.empty:
        st.warning("No subgroup data available for the selected filters.")
        return

    left, right = st.columns(2)
    with left:
        if not division_df.empty:
            st.plotly_chart(division_bar_chart(division_df), use_container_width=True)
    with right:
        if not rating_df.empty:
            st.plotly_chart(rating_band_chart(rating_df), use_container_width=True)

    if not venue_df.empty:
        st.plotly_chart(course_layout_scatter(venue_df), use_container_width=True)

        rank_cols = [
            c for c in [
                "course_id",
                "layout_id",
                "state",
                "rounds_scored",
                "avg_observed_wind_mph",
                "avg_estimated_wind_impact_strokes",
                "avg_estimated_total_weather_impact_strokes",
            ] if c in venue_df.columns
        ]
        ranking = venue_df[rank_cols].sort_values("avg_estimated_wind_impact_strokes", ascending=False)
        st.subheader("Most Weather-Sensitive Venues")
        render_table(ranking)
