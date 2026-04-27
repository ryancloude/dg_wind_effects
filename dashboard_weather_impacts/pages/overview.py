from __future__ import annotations

import pandas as pd
import streamlit as st

from dashboard_weather_impacts.charts import (
    overview_distribution_chart,
    overview_wind_impact_points_chart,
)
from dashboard_weather_impacts.components.kpis import render_kpi_row
from dashboard_weather_impacts.filters import DashboardFilters
from dashboard_weather_impacts.formatters import format_int, format_mph, format_strokes


_REQUIRED_OVERVIEW_COLUMNS = {
    "reference_wind_mph",
    "reference_gust_mph",
    "reference_temp_f",
    "reference_precipitation",
    "rounds_tracked",
    "events_tracked",
    "avg_added_strokes_weather",
    "avg_added_strokes_wind",
    "avg_observed_wind_mph",
    "avg_observed_wind_gust_mph",
}

_REQUIRED_DISTRIBUTION_COLUMNS = {
    "metric_name",
    "metric_label",
    "bin_label",
    "bin_start",
    "bin_end",
    "rounds_tracked",
    "share_of_rounds",
    "sort_order",
}

_REQUIRED_POINTS_COLUMNS = {
    "bucket_metric",
    "bucket_label",
    "sort_order",
    "rounds_tracked",
    "avg_added_strokes_from_wind",
}


def _inject_overview_styles() -> None:
    st.markdown(
        """
        <style>
        .overview-page-title {
            font-size: 2.2rem;
            font-weight: 700;
            line-height: 1.02;
            letter-spacing: -0.03em;
            color: #2f3441;
            margin-bottom: 0.25rem;
        }

        .overview-page-subtitle {
            font-size: 1rem;
            line-height: 1.55;
            color: #5b6170;
            max-width: 860px;
            margin-bottom: 1.6rem;
        }

        div[data-testid="stMetric"] {
            background: white;
            border: 1px solid #e7dfd2;
            border-radius: 20px;
            padding: 0.95rem 1rem;
            box-shadow: 0 1px 0 rgba(47, 52, 65, 0.04);
            min-height: 120px;
        }

        [data-testid="stMetricLabel"] {
            color: #5b6170 !important;
            opacity: 1 !important;
        }

        [data-testid="stMetricLabel"] p {
            color: #5b6170 !important;
            opacity: 1 !important;
            font-size: 0.95rem !important;
            line-height: 1.3rem !important;
            font-weight: 600 !important;
            white-space: normal !important;
        }

        [data-testid="stMetricValue"] {
            color: #2f3441 !important;
            opacity: 1 !important;
        }

        [data-testid="stMetricValue"] > div {
            color: #2f3441 !important;
            opacity: 1 !important;
        }

        .overview-hero {
            background: linear-gradient(135deg, #fffaf3 0%, #ffffff 100%);
            border: 1px solid #e7dfd2;
            border-radius: 24px;
            padding: 1.4rem 1.5rem;
            margin-bottom: 1.15rem;
            box-shadow: 0 1px 0 rgba(47, 52, 65, 0.04);
        }

        .overview-hero-grid {
            display: grid;
            grid-template-columns: 1.45fr 0.9fr;
            gap: 1rem;
            align-items: start;
        }

        .overview-kicker {
            font-size: 0.76rem;
            font-weight: 700;
            letter-spacing: 0.09em;
            text-transform: uppercase;
            color: #b45309;
            margin-bottom: 0.35rem;
        }

        .overview-hero-title {
            font-size: 1.28rem;
            font-weight: 700;
            color: #2f3441;
            margin-bottom: 0.25rem;
        }

        .overview-hero-body {
            font-size: 1rem;
            color: #5b6170;
            line-height: 1.55;
        }

        .overview-refresh-note {
            margin-top: 0.8rem;
            font-size: 0.93rem;
            line-height: 1.45;
            color: #6b7280;
        }

        .overview-baseline-values {
            font-size: 1.05rem;
            color: #2f3441;
            line-height: 1.6;
            font-weight: 600;
        }

        .overview-mini-card {
            background: rgba(180, 83, 9, 0.05);
            border: 1px solid rgba(180, 83, 9, 0.12);
            border-radius: 18px;
            padding: 0.95rem 1rem;
        }

        .overview-mini-label {
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #b45309;
            margin-bottom: 0.35rem;
        }

        .overview-mini-value {
            font-size: 1.25rem;
            font-weight: 700;
            color: #2f3441;
            margin-bottom: 0.15rem;
        }

        .overview-mini-body {
            font-size: 0.92rem;
            line-height: 1.5;
            color: #5b6170;
        }

        .overview-section {
            margin-top: 1.3rem;
            margin-bottom: 0.7rem;
        }

        .overview-section-kicker {
            font-size: 0.76rem;
            font-weight: 700;
            letter-spacing: 0.09em;
            text-transform: uppercase;
            color: #b45309;
            margin-bottom: 0.28rem;
        }

        .overview-section-title {
            font-size: 1.18rem;
            font-weight: 700;
            color: #2f3441;
            margin-bottom: 0.18rem;
        }

        .overview-section-body {
            font-size: 0.98rem;
            line-height: 1.5;
            color: #5b6170;
            max-width: 820px;
        }

        div[data-testid="stVerticalBlockBorderWrapper"] {
            background: white;
            border-color: #e7dfd2;
            border-radius: 24px;
            box-shadow: 0 1px 0 rgba(47, 52, 65, 0.04);
        }

        div[data-testid="stSelectbox"] label,
        div[data-testid="stRadio"] label {
            color: #5b6170 !important;
            font-weight: 600 !important;
            opacity: 1 !important;
        }

        div[data-testid="stRadio"] [role="radiogroup"] label p {
            color: #2f3441 !important;
            opacity: 1 !important;
            font-weight: 600 !important;
        }

        div[data-testid="stRadio"] [role="radiogroup"] label {
            color: #2f3441 !important;
            opacity: 1 !important;
        }

        @media (max-width: 1000px) {
            .overview-hero-grid {
                grid-template-columns: 1fr;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _assert_required_columns(df: pd.DataFrame, required_columns: set[str], table_name: str) -> None:
    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise ValueError(
            f"`{table_name}` is missing required columns for the new dashboard schema: {missing}. "
            "Rebuild `report_round_weather_impacts` with the updated reporting SQL."
        )


def _distribution_subset(df: pd.DataFrame, metric_name: str) -> pd.DataFrame:
    out = df[df["metric_name"] == metric_name].copy()
    if "sort_order" in out.columns:
        out = out.sort_values("sort_order")
    return out


def _render_page_header() -> None:
    st.markdown('<div class="overview-page-title">Overview</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="overview-page-subtitle">A concise view of the weather baseline, overall sample coverage, and how wind-related scoring difficulty changes across tracked rounds.</div>',
        unsafe_allow_html=True,
    )


def _render_weather_baseline(row: pd.Series) -> None:
    st.markdown(
        f"""
        <div class="overview-hero">
            <div class="overview-hero-grid">
                <div>
                    <div class="overview-kicker">Weather Baseline</div>
                    <div class="overview-hero-title">Reference conditions used for all added-strokes comparisons</div>
                    <div class="overview-hero-body">
                        Every added-strokes metric on this page is measured relative to a single modeled baseline so the dashboard stays easy to interpret across events, rounds, and venues.
                    </div>
                    <div class="overview-refresh-note">
                        Dashboard data is refreshed daily as new scored rounds and report tables are published.
                    </div>
                    <div class="overview-baseline-values" style="margin-top: 0.8rem;">
                        {float(row['reference_wind_mph']):.0f} mph wind<br>
                        {float(row['reference_gust_mph']):.0f} mph gust<br>
                        {row['reference_precipitation']}<br>
                        {float(row['reference_temp_f']):.0f} F
                    </div>
                </div>
                <div class="overview-mini-card">
                    <div class="overview-mini-label">Tracked Sample</div>
                    <div class="overview-mini-value">{int(row['rounds_tracked']):,} rounds</div>
                    <div class="overview-mini-body">
                        Built from {int(row['events_tracked']):,} events with model-scored rounds published through the reporting layer.
                    </div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_section(kicker: str, title: str, body: str) -> None:
    st.markdown(
        f"""
        <div class="overview-section">
            <div class="overview-section-kicker">{kicker}</div>
            <div class="overview-section-title">{title}</div>
            <div class="overview-section-body">{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _metric_options() -> dict[str, dict[str, str]]:
    return {
        "Total Added Strokes from Weather": {
            "metric_name": "total_added_strokes_weather",
            "title": "Distribution of Total Added Strokes from Weather",
            "x_label": "Total Added Strokes from Weather",
        },
        "Added Strokes from Wind": {
            "metric_name": "added_strokes_wind",
            "title": "Distribution of Added Strokes from Wind",
            "x_label": "Added Strokes from Wind",
        },
        "Observed Average Wind Speed": {
            "metric_name": "observed_average_wind_speed",
            "title": "Distribution of Observed Average Wind Speed",
            "x_label": "Observed Average Wind Speed (mph)",
        },
        "Observed Average Wind Gust Speed": {
            "metric_name": "observed_average_wind_gust_speed",
            "title": "Distribution of Observed Average Wind Gust Speed",
            "x_label": "Observed Average Wind Gust Speed (mph)",
        },
        "Observed Temperature": {
            "metric_name": "observed_temperature",
            "title": "Distribution of Observed Temperature",
            "x_label": "Observed Temperature (F)",
        },
        "Observed Precipitation": {
            "metric_name": "observed_precipitation",
            "title": "Distribution of Observed Precipitation",
            "x_label": "Observed Precipitation",
        },
    }


def render_overview(filters: DashboardFilters, datasets: dict):
    del filters

    _inject_overview_styles()
    _render_page_header()

    overview_df = datasets.get("weather_overview")
    distribution_df = datasets.get("weather_impact_distribution")
    points_df = datasets.get("weather_wind_impact_points")

    if overview_df is None or overview_df.empty:
        st.warning("No overview data is available yet. Rebuild `weather_overview` and refresh the dashboard.")
        return

    if distribution_df is None or distribution_df.empty:
        st.warning(
            "No distribution data is available yet. Rebuild `weather_impact_distribution` and refresh the dashboard."
        )
        return

    if points_df is None or points_df.empty:
        st.warning(
            "No wind impact point data is available yet. Rebuild `weather_wind_impact_points` and refresh the dashboard."
        )
        return

    _assert_required_columns(overview_df, _REQUIRED_OVERVIEW_COLUMNS, "weather_overview")
    _assert_required_columns(distribution_df, _REQUIRED_DISTRIBUTION_COLUMNS, "weather_impact_distribution")
    _assert_required_columns(points_df, _REQUIRED_POINTS_COLUMNS, "weather_wind_impact_points")

    row = overview_df.iloc[0]

    _render_weather_baseline(row)

    _render_section(
        "Summary Metrics",
        "What the full tracked sample looks like",
        "These top-line metrics summarize the overall weather difficulty signal and the average observed conditions across all scored rounds included in the dashboard.",
    )

    render_kpi_row(
        [
            ("Avg Weather Added Strokes", format_strokes(row.get("avg_added_strokes_weather"))),
            ("Avg Wind Added Strokes", format_strokes(row.get("avg_added_strokes_wind"))),
            ("Avg Wind", format_mph(row.get("avg_observed_wind_mph"))),
            ("Avg Wind Gust", format_mph(row.get("avg_observed_wind_gust_mph"))),
            ("Rounds Tracked", format_int(row.get("rounds_tracked"))),
            ("Events Tracked", format_int(row.get("events_tracked"))),
        ],
        columns_per_row=3,
    )

    st.caption(
        "Positive added strokes means observed conditions were modeled as harder than the weather baseline. Negative values mean easier-than-baseline conditions."
    )

    _render_section(
        "Distribution View",
        "How tracked rounds were distributed",
        "Use the selector to switch between modeled impact metrics and the observed weather variables that fed the scoring workflow.",
    )

    metric_options = _metric_options()

    with st.container(border=True):
        selected_metric_label = st.selectbox(
            "Metric",
            options=list(metric_options.keys()),
            index=0,
        )

        metric_config = metric_options[selected_metric_label]
        selected_distribution = _distribution_subset(distribution_df, metric_config["metric_name"])

        if selected_distribution.empty:
            st.warning(
                f"`weather_impact_distribution` contains no rows for `{selected_metric_label}`. "
                "Rebuild `report_round_weather_impacts` with the updated overview SQL."
            )
            return

        st.plotly_chart(
            overview_distribution_chart(
                selected_distribution,
                title=metric_config["title"],
                x_label=metric_config["x_label"],
            ),
            use_container_width=True,
        )

    _render_section(
        "Wind Impact Trend",
        "Average wind impact rises with stronger buckets",
        "This chart shows the mean added strokes from wind across bucketed sustained-wind or gust conditions.",
    )

    with st.container(border=True):
        bucket_metric = st.radio(
            "Bucket Metric",
            options=["Average Wind Speed", "Average Wind Gust"],
            horizontal=True,
        )

        bucket_metric_key = "wind_speed" if bucket_metric == "Average Wind Speed" else "wind_gust"

        st.plotly_chart(
            overview_wind_impact_points_chart(
                points_df,
                bucket_metric=bucket_metric_key,
            ),
            use_container_width=True,
        )

