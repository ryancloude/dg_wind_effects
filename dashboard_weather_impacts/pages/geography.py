from __future__ import annotations

import pandas as pd
import streamlit as st

from dashboard_weather_impacts.charts import state_choropleth
from dashboard_weather_impacts.components.tables import render_table
from dashboard_weather_impacts.filters import DashboardFilters


_STATE_NAME_TO_CODE = {
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "DISTRICT OF COLUMBIA": "DC",
    "FLORIDA": "FL",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI",
    "WYOMING": "WY",
}

_STATE_CODE_TO_NAME = {code: name.title() for name, code in _STATE_NAME_TO_CODE.items()}

_METRIC_OPTIONS: dict[str, dict[str, str]] = {
    "Average Observed Wind": {
        "column": "avg_observed_wind_mph",
        "chart_title": "Average Observed Wind by State",
        "table_label": "Average Observed Wind (mph)",
    },
    "Average Observed Temperature": {
        "column": "avg_observed_temp_f",
        "chart_title": "Average Observed Temperature by State",
        "table_label": "Average Observed Temperature (F)",
    },
    "Average Wind Impact": {
        "column": "avg_estimated_wind_impact_strokes",
        "chart_title": "Average Wind Impact by State",
        "table_label": "Average Wind Impact (strokes)",
    },
    "Average Total Weather Impact": {
        "column": "avg_estimated_total_weather_impact_strokes",
        "chart_title": "Average Total Weather Impact by State",
        "table_label": "Average Total Weather Impact (strokes)",
    },
    "Number of Events": {
        "column": "events_scored",
        "chart_title": "Number of Events by State",
        "table_label": "Number of Events",
    },
    "Number of Rounds": {
        "column": "rounds_scored",
        "chart_title": "Number of Rounds by State",
        "table_label": "Number of Rounds",
    },
}


def _inject_geography_styles() -> None:
    st.markdown(
        """
        <style>
        .geography-page-title {
            font-size: 2.2rem;
            font-weight: 700;
            line-height: 1.02;
            letter-spacing: -0.03em;
            color: #2f3441;
            margin-bottom: 0.25rem;
        }

        .geography-page-subtitle {
            font-size: 1rem;
            line-height: 1.55;
            color: #5b6170;
            max-width: 860px;
            margin-bottom: 1.4rem;
        }

        .geography-toolbar {
            background: white;
            border: 1px solid #e7dfd2;
            border-radius: 24px;
            padding: 1rem 1.1rem 0.8rem 1.1rem;
            margin-bottom: 1rem;
            box-shadow: 0 1px 0 rgba(47, 52, 65, 0.04);
        }

        .geography-summary-chip {
            display: inline-block;
            background: rgba(180, 83, 9, 0.08);
            color: #8a4b10;
            border: 1px solid rgba(180, 83, 9, 0.14);
            border-radius: 999px;
            padding: 0.35rem 0.75rem;
            font-size: 0.9rem;
            font-weight: 600;
            margin-bottom: 1rem;
        }

        .geography-section-kicker {
            font-size: 0.76rem;
            font-weight: 700;
            letter-spacing: 0.09em;
            text-transform: uppercase;
            color: #b45309;
            margin-bottom: 0.25rem;
        }

        .geography-section-title {
            font-size: 1.18rem;
            font-weight: 700;
            color: #2f3441;
            margin-bottom: 0.15rem;
        }

        .geography-section-body {
            font-size: 0.98rem;
            line-height: 1.5;
            color: #5b6170;
            margin-bottom: 0.9rem;
        }

        div[data-testid="stVerticalBlockBorderWrapper"] {
            background: white;
            border-color: #e7dfd2;
            border-radius: 24px;
            box-shadow: 0 1px 0 rgba(47, 52, 65, 0.04);
        }

        div[data-testid="stSelectbox"] label,
        div[data-testid="stMultiSelect"] label {
            color: #5b6170 !important;
            font-weight: 600 !important;
            opacity: 1 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_page_header() -> None:
    st.markdown('<div class="geography-page-title">Geography</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="geography-page-subtitle">Compare how observed conditions and modeled weather impacts vary across U.S. states, with optional month filtering to focus the map on seasonal patterns.</div>',
        unsafe_allow_html=True,
    )


def _normalize_state_value(value: object) -> tuple[str, str] | None:
    if value is None or pd.isna(value):
        return None

    raw = str(value).strip()
    if not raw:
        return None

    upper = raw.upper()

    if upper in _STATE_CODE_TO_NAME:
        return upper, _STATE_CODE_TO_NAME[upper]

    if upper in _STATE_NAME_TO_CODE:
        code = _STATE_NAME_TO_CODE[upper]
        return code, upper.title()

    return None


def _prepare_us_state_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "state" not in df.columns:
        return df.copy()

    out = df.copy()
    normalized = out["state"].map(_normalize_state_value)

    out["state_code"] = normalized.map(lambda x: x[0] if x else None)
    out["state_name"] = normalized.map(lambda x: x[1] if x else None)

    out = out.dropna(subset=["state_code"]).copy()
    return out


def _month_options(df: pd.DataFrame) -> list[tuple[str, int]]:
    if df.empty or "round_month" not in df.columns or "round_month_label" not in df.columns:
        return []

    month_df = (
        df[["round_month", "round_month_label"]]
        .dropna()
        .drop_duplicates()
        .sort_values("round_month")
    )

    return [(str(row["round_month_label"]), int(row["round_month"])) for _, row in month_df.iterrows()]


def _selected_month_summary(selected_month_options: list[tuple[str, int]], all_month_options: list[tuple[str, int]]) -> str:
    if not selected_month_options:
        return "No months selected"

    if len(selected_month_options) == len(all_month_options):
        return "Showing all months"

    month_labels = [label for label, _ in selected_month_options]

    if len(month_labels) <= 4:
        return "Showing: " + ", ".join(month_labels)

    return f"Showing {len(month_labels)} selected months"


def _weighted_average(group: pd.DataFrame, value_col: str, weight_col: str = "rounds_scored") -> float | None:
    valid = group[[value_col, weight_col]].dropna()
    if valid.empty:
        return None

    total_weight = valid[weight_col].sum()
    if total_weight == 0:
        return None

    return float((valid[value_col] * valid[weight_col]).sum() / total_weight)


def _aggregate_state_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    weighted_metric_cols = [
        "avg_observed_wind_mph",
        "avg_observed_temp_f",
        "avg_estimated_wind_impact_strokes",
        "avg_estimated_total_weather_impact_strokes",
    ]

    records: list[dict] = []

    grouped = df.groupby(["state_code", "state_name"], sort=True)

    for (state_code, state_name), group in grouped:
        record = {
            "state_code": state_code,
            "state_name": state_name,
            "rounds_scored": int(group["rounds_scored"].sum()) if "rounds_scored" in group.columns else 0,
            "events_scored": int(group["events_scored"].sum()) if "events_scored" in group.columns else 0,
            "players_scored": int(group["players_scored"].sum()) if "players_scored" in group.columns else 0,
        }

        for metric_col in weighted_metric_cols:
            if metric_col in group.columns:
                record[metric_col] = _weighted_average(group, metric_col)

        records.append(record)

    return pd.DataFrame(records).sort_values("state_name").reset_index(drop=True)


def _top_states_table(df: pd.DataFrame, metric_col: str, metric_label: str) -> pd.DataFrame:
    base_cols = ["state_name", "events_scored", "rounds_scored"]
    cols = base_cols.copy()

    if metric_col not in cols:
        cols.append(metric_col)

    table_df = df[cols].copy()

    rename_map = {
        "state_name": "State",
        "events_scored": "Number of Events",
        "rounds_scored": "Number of Rounds",
        metric_col: metric_label,
    }
    table_df = table_df.rename(columns=rename_map)

    sort_col = metric_label
    table_df = table_df.sort_values(sort_col, ascending=False).reset_index(drop=True)

    if metric_col not in {"events_scored", "rounds_scored"} and metric_label in table_df.columns:
        table_df[metric_label] = table_df[metric_label].map(
            lambda x: round(float(x), 2) if pd.notna(x) else x
        )

    return table_df


def render_geography(filters: DashboardFilters, datasets: dict):
    del filters

    _inject_geography_styles()
    _render_page_header()

    state_source = datasets.get("weather_by_state", pd.DataFrame())

    if state_source.empty:
        st.warning("No geography data is available yet. Rebuild `weather_by_state` and refresh the dashboard.")
        return

    state_source = _prepare_us_state_rows(state_source)

    if state_source.empty:
        st.warning("No U.S. state data is available after normalizing the geography dataset.")
        return

    month_options = _month_options(state_source)

    st.markdown(
        """
        <div class="geography-toolbar">
            <div class="geography-section-kicker">Map Controls</div>
            <div class="geography-section-title">Explore by state and month</div>
            <div class="geography-section-body">
                This page is restricted to U.S. events only. Select one or more months to focus the map on seasonal patterns.
            </div>
        """,
        unsafe_allow_html=True,
    )

    controls_left, controls_right = st.columns([1, 1], gap="large")

    with controls_left:
        selected_metric_label = st.selectbox(
            "Metric",
            options=list(_METRIC_OPTIONS.keys()),
            index=2,
        )

    with controls_right:
        selected_month_options = st.multiselect(
            "Month",
            options=month_options,
            format_func=lambda x: x[0],
            default=month_options,
        )

    st.markdown("</div>", unsafe_allow_html=True)

    if not selected_month_options:
        st.warning("Select at least one month to display geography results.")
        return

    st.markdown(
        f'<div class="geography-summary-chip">{_selected_month_summary(selected_month_options, month_options)}</div>',
        unsafe_allow_html=True,
    )

    selected_months = [month_value for _, month_value in selected_month_options]

    if "round_month" in state_source.columns:
        state_filtered = state_source[state_source["round_month"].isin(selected_months)].copy()
    else:
        state_filtered = state_source.copy()

    state_df = _aggregate_state_rows(state_filtered)

    metric_config = _METRIC_OPTIONS[selected_metric_label]
    metric_col = metric_config["column"]
    metric_table_label = metric_config["table_label"]

    if state_df.empty:
        st.warning("No state data is available for the selected months.")
        return

    state_df = state_df.dropna(subset=[metric_col]).copy()

    if state_df.empty:
        st.warning(f"No rows contain `{selected_metric_label}` for the selected months.")
        return

    with st.container(border=True):
        fig = state_choropleth(
            state_df,
            metric_col,
            metric_config["chart_title"],
            selected_metric_label,
        )
        st.plotly_chart(
            fig,
            use_container_width=True,
            config={
                "displayModeBar": False,
                "scrollZoom": False,
                "doubleClick": False,
            },
        )

    table_df = _top_states_table(state_df, metric_col, metric_table_label)

    st.subheader(f"Top States by {selected_metric_label}")
    render_table(table_df)

