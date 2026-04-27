from __future__ import annotations

import pandas as pd
import streamlit as st

from dashboard_weather_impacts.charts import event_round_conditions_chart, event_round_impact_chart
from dashboard_weather_impacts.components.kpis import render_kpi_row
from dashboard_weather_impacts.components.tables import render_table
from dashboard_weather_impacts.data_access import load_scored_round_detail
from dashboard_weather_impacts.filters import DashboardFilters
from dashboard_weather_impacts.formatters import format_int, format_mph, format_strokes, format_temp_f


_MPS_TO_MPH = 2.23694
_CONDITION_METRICS = {
    "Wind Speed": "wind_speed",
    "Wind Gust": "wind_gust",
    "Temperature": "temperature",
}


def _inject_event_explorer_styles() -> None:
    st.markdown(
        """
        <style>
        .event-page-title {
            font-size: 2.2rem;
            font-weight: 700;
            line-height: 1.02;
            letter-spacing: -0.03em;
            color: #2f3441;
            margin-bottom: 1.1rem;
        }

        .event-toolbar {
            background: white;
            border: 1px solid #e7dfd2;
            border-radius: 24px;
            padding: 1rem 1.1rem 0.8rem 1.1rem;
            margin-bottom: 1rem;
            box-shadow: 0 1px 0 rgba(47, 52, 65, 0.04);
        }

        .event-kicker {
            font-size: 0.76rem;
            font-weight: 700;
            letter-spacing: 0.09em;
            text-transform: uppercase;
            color: #b45309;
            margin-bottom: 0.25rem;
        }

        .event-section-title {
            font-size: 1.18rem;
            font-weight: 700;
            color: #2f3441;
            margin-bottom: 0.12rem;
        }

        .event-body {
            font-size: 0.98rem;
            line-height: 1.5;
            color: #5b6170;
            margin-bottom: 0.9rem;
        }

        .event-hero {
            background: linear-gradient(135deg, #fffaf3 0%, #ffffff 100%);
            border: 1px solid #e7dfd2;
            border-radius: 24px;
            padding: 1.3rem 1.4rem;
            margin-bottom: 1rem;
            box-shadow: 0 1px 0 rgba(47, 52, 65, 0.04);
        }

        .event-hero-grid {
            display: grid;
            grid-template-columns: 1.45fr 0.9fr;
            gap: 1rem;
            align-items: start;
        }

        .event-name {
            font-size: 1.45rem;
            font-weight: 700;
            color: #2f3441;
            margin-bottom: 0.25rem;
            line-height: 1.2;
        }

        .event-location {
            font-size: 1rem;
            line-height: 1.55;
            color: #5b6170;
            margin-bottom: 0.65rem;
        }

        .event-meta {
            font-size: 0.98rem;
            line-height: 1.6;
            color: #2f3441;
        }

        .event-mini-card {
            background: rgba(180, 83, 9, 0.05);
            border: 1px solid rgba(180, 83, 9, 0.12);
            border-radius: 18px;
            padding: 0.95rem 1rem;
        }

        .event-mini-label {
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #b45309;
            margin-bottom: 0.35rem;
        }

        .event-mini-value {
            font-size: 1.25rem;
            font-weight: 700;
            color: #2f3441;
            margin-bottom: 0.15rem;
        }

        .event-mini-body {
            font-size: 0.92rem;
            line-height: 1.5;
            color: #5b6170;
        }

        .event-section {
            margin-top: 1.3rem;
            margin-bottom: 0.7rem;
        }

        .event-table-filters {
            background: white;
            border: 1px solid #e7dfd2;
            border-radius: 20px;
            padding: 0.9rem 1rem 0.7rem 1rem;
            margin-bottom: 0.9rem;
            box-shadow: 0 1px 0 rgba(47, 52, 65, 0.04);
        }

        div[data-testid="stMetric"] {
            background: white;
            border: 1px solid #e7dfd2;
            border-radius: 20px;
            padding: 0.95rem 1rem;
            box-shadow: 0 1px 0 rgba(47, 52, 65, 0.04);
            min-height: 120px;
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

        div[data-testid="stVerticalBlockBorderWrapper"] {
            background: white;
            border-color: #e7dfd2;
            border-radius: 24px;
            box-shadow: 0 1px 0 rgba(47, 52, 65, 0.04);
        }

        div[data-testid="stSelectbox"] label {
            color: #5b6170 !important;
            font-weight: 600 !important;
            opacity: 1 !important;
        }

        @media (max-width: 1000px) {
            .event-hero-grid {
                grid-template-columns: 1fr;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_page_header() -> None:
    st.markdown('<div class="event-page-title">Event Explorer</div>', unsafe_allow_html=True)


def _render_section(kicker: str, title: str, body: str) -> None:
    st.markdown(
        f"""
        <div class="event-section">
            <div class="event-kicker">{kicker}</div>
            <div class="event-section-title">{title}</div>
            <div class="event-body">{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _format_state_label(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    if len(text) <= 3 and text.replace(".", "").isalpha():
        return text.upper()

    return text.title()


def _display_date(value: object) -> str:
    text = str(value or "").strip()
    return text if text else "Unknown"


def _event_display_label(row: pd.Series) -> str:
    return str(row.get("event_name", "Unknown Event"))


def _derive_event_dates_from_detail(detail_df: pd.DataFrame) -> tuple[str, str]:
    if detail_df.empty or "round_date" not in detail_df.columns:
        return "", ""

    parsed_dates = pd.to_datetime(detail_df["round_date"], errors="coerce").dropna()
    if parsed_dates.empty:
        return "", ""

    start_date = parsed_dates.min().date().isoformat()
    end_date = parsed_dates.max().date().isoformat()
    return start_date, end_date


def _enrich_event_row_from_detail(row: pd.Series, detail_df: pd.DataFrame) -> pd.Series:
    if detail_df.empty:
        return row

    out = row.copy()

    detail_start, detail_end = _derive_event_dates_from_detail(detail_df)
    if detail_start:
        out["event_start_date"] = detail_start
    if detail_end:
        out["event_end_date"] = detail_end

    if "state" in out.index:
        out["state"] = _format_state_label(out.get("state"))

    return out


def _render_event_summary(row: pd.Series) -> None:
    event_name = str(row.get("event_name", "Unknown Event"))
    event_city = str(row.get("event_city", "") or "").strip()
    state = _format_state_label(row.get("state"))
    start_date = _display_date(row.get("event_start_date"))
    end_date = _display_date(row.get("event_end_date"))
    year = int(row.get("event_year", 0))
    rounds_scored = int(row.get("rounds_scored", 0) or 0)
    players_scored = int(row.get("players_scored", 0) or 0)

    location_line = ", ".join([x for x in [event_city, state] if x])

    if start_date != "Unknown" and end_date != "Unknown":
        date_line = f"{start_date} to {end_date}"
    elif start_date != "Unknown":
        date_line = start_date
    elif end_date != "Unknown":
        date_line = end_date
    else:
        date_line = "Unknown"

    st.markdown(
        f"""
        <div class="event-hero">
            <div class="event-hero-grid">
                <div>
                    <div class="event-kicker">Selected Event</div>
                    <div class="event-name">{event_name}</div>
                    <div class="event-location">{location_line if location_line else "Location not available"}</div>
                    <div class="event-meta">
                        <strong>Year:</strong> {year}<br>
                        <strong>Event Dates:</strong> {date_line}<br>
                        <strong>State:</strong> {state if state else "Unknown"}
                    </div>
                </div>
                <div class="event-mini-card">
                    <div class="event-mini-label">Coverage</div>
                    <div class="event-mini-value">{rounds_scored:,} rounds</div>
                    <div class="event-mini-body">
                        Built from {players_scored:,} player-round observations scored for this event.
                    </div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _prepare_detail_df(detail_df: pd.DataFrame) -> pd.DataFrame:
    out = detail_df.copy()

    if "round_wind_speed_mps_mean" in out.columns:
        out["Observed Wind (mph)"] = pd.to_numeric(out["round_wind_speed_mps_mean"], errors="coerce") * _MPS_TO_MPH

    if "round_wind_gust_mps_mean" in out.columns:
        out["Observed Gust (mph)"] = pd.to_numeric(out["round_wind_gust_mps_mean"], errors="coerce") * _MPS_TO_MPH

    if "round_temp_c_mean" in out.columns:
        out["Observed Temp (F)"] = (pd.to_numeric(out["round_temp_c_mean"], errors="coerce") * 9.0 / 5.0) + 32.0

    if "round_precip_mm_sum" in out.columns:
        out["Precipitation"] = (
            pd.to_numeric(out["round_precip_mm_sum"], errors="coerce")
            .fillna(0.0)
            .gt(0.0)
            .map({False: "No", True: "Yes"})
        )

    rename_map = {
        "player_name": "Player",
        "division": "Division",
        "round_number": "Round",
        "round_date": "Round Date",
        "actual_round_strokes": "Actual Strokes",
        "predicted_round_strokes": "Predicted Strokes",
        "predicted_round_strokes_wind_reference": "Predicted Strokes in Ideal Conditions",
        "estimated_wind_impact_strokes": "Wind Added Strokes",
        "estimated_total_weather_impact_strokes": "Total Weather Added Strokes",
    }
    out = out.rename(columns=rename_map)

    preferred_cols = [
        "Player",
        "Division",
        "Round",
        "Round Date",
        "Actual Strokes",
        "Predicted Strokes",
        "Predicted Strokes in Ideal Conditions",
        "Wind Added Strokes",
        "Total Weather Added Strokes",
        "Observed Wind (mph)",
        "Observed Gust (mph)",
        "Observed Temp (F)",
        "Precipitation",
    ]
    visible_cols = [c for c in preferred_cols if c in out.columns]
    out = out[visible_cols].copy()

    for col in [
        "Predicted Strokes",
        "Predicted Strokes in Ideal Conditions",
        "Wind Added Strokes",
        "Total Weather Added Strokes",
        "Observed Wind (mph)",
        "Observed Gust (mph)",
        "Observed Temp (F)",
    ]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(2)

    return out


def render_event_explorer(filters: DashboardFilters, datasets: dict, config):
    del filters

    _inject_event_explorer_styles()
    _render_page_header()

    event_df = datasets["weather_by_event"].copy()
    round_df = datasets["weather_by_event_round"].copy()

    if event_df.empty:
        st.warning("No event data is available yet. Rebuild `weather_by_event` and refresh the dashboard.")
        return

    st.markdown(
        """
        <div class="event-toolbar">
            <div class="event-kicker">Event Selection</div>
            <div class="event-section-title">Choose a single event to inspect</div>
            <div class="event-body">
                Start by selecting a year, then choose an event to see event-level context, round-by-round trends, and detailed scored rounds.
            </div>
        """,
        unsafe_allow_html=True,
    )

    years = sorted(event_df["event_year"].dropna().astype(int).unique().tolist(), reverse=True)
    selected_year = st.selectbox("Year", years, index=0)

    year_event_df = event_df[event_df["event_year"].astype(int) == int(selected_year)].copy()

    event_options = (
        year_event_df[["event_year", "tourn_id", "event_name", "event_city", "state"]]
        .drop_duplicates()
        .sort_values(["event_name", "tourn_id"])
        .reset_index(drop=True)
    )

    selected_event = st.selectbox(
        "Event",
        options=list(event_options.itertuples(index=False, name=None)),
        format_func=lambda x: _event_display_label(
            pd.Series(
                {
                    "event_year": x[0],
                    "tourn_id": x[1],
                    "event_name": x[2],
                    "event_city": x[3],
                    "state": x[4],
                }
            )
        ),
    )

    st.markdown("</div>", unsafe_allow_html=True)

    selected_year = int(selected_event[0])
    selected_event_id = int(selected_event[1])

    selected_event_df = event_df[
        (event_df["event_year"].astype(int) == selected_year) & (event_df["tourn_id"].astype(int) == selected_event_id)
    ].copy()

    selected_round_df = round_df[
        (round_df["event_year"].astype(int) == selected_year) & (round_df["tourn_id"].astype(int) == selected_event_id)
    ].copy()

    detail_df = load_scored_round_detail(config, selected_year, selected_event_id)

    if selected_event_df.empty:
        st.warning("Selected event has no aggregate data.")
        return

    row = _enrich_event_row_from_detail(selected_event_df.iloc[0], detail_df)

    _render_event_summary(row)

    _render_section(
        "Event Metrics",
        "Headline conditions and modeled effects",
        "These KPIs summarize the selected event before you drill into round-by-round trends and scored detail.",
    )

    render_kpi_row(
        [
            ("Avg Wind", format_mph(row.get("avg_observed_wind_mph"))),
            ("Avg Wind Gust", format_mph(row.get("avg_observed_wind_gust_mph"))),
            ("Avg Temperature", format_temp_f(row.get("avg_observed_temp_f"))),
            ("Avg Weather Added Strokes", format_strokes(row.get("avg_estimated_total_weather_impact_strokes"))),
            ("Avg Wind Added Strokes", format_strokes(row.get("avg_estimated_wind_impact_strokes"))),
            ("Rounds", format_int(row.get("rounds_scored"))),
            ("Players", format_int(row.get("players_scored"))),
        ],
        columns_per_row=4,
    )

    _render_section(
        "Round Trends",
        "How weather pressure and conditions changed across rounds",
        "The left chart shows event-level added strokes by round. Use the metric selector on the right to inspect one condition at a time.",
    )

    condition_metric_label = st.selectbox(
        "Conditions Metric",
        options=list(_CONDITION_METRICS.keys()),
        index=0,
    )
    condition_metric_key = _CONDITION_METRICS[condition_metric_label]

    if not selected_round_df.empty:
        left, right = st.columns(2, gap="large")
        with left:
            with st.container(border=True):
                st.plotly_chart(
                    event_round_impact_chart(selected_round_df),
                    use_container_width=True,
                    config={
                        "displayModeBar": False,
                        "scrollZoom": False,
                        "doubleClick": False,
                    },
                )
        with right:
            with st.container(border=True):
                st.plotly_chart(
                    event_round_conditions_chart(selected_round_df, metric_key=condition_metric_key),
                    use_container_width=True,
                    config={
                        "displayModeBar": False,
                        "scrollZoom": False,
                        "doubleClick": False,
                    },
                )
    else:
        st.info("No round-level aggregates are available for this event.")

    _render_section(
        "Scored Detail",
        "Inspect the scored rounds behind the event summary",
        "Use the filters below to narrow the table by round or division for the selected event.",
    )

    if detail_df.empty:
        st.warning("No scored round detail was found for the selected event.")
        return

    prepared_detail_df = _prepare_detail_df(detail_df)

    st.markdown('<div class="event-table-filters">', unsafe_allow_html=True)
    filter_left, filter_right = st.columns(2, gap="large")

    with filter_left:
        round_options = ["All"]
        if "Round" in prepared_detail_df.columns:
            round_options.extend(sorted([int(x) for x in prepared_detail_df["Round"].dropna().unique().tolist()]))
        selected_round = st.selectbox("Round", options=round_options, index=0)

    with filter_right:
        division_options = ["All"]
        if "Division" in prepared_detail_df.columns:
            division_options.extend(sorted([str(x) for x in prepared_detail_df["Division"].dropna().unique().tolist()]))
        selected_division = st.selectbox("Division", options=division_options, index=0)

    st.markdown("</div>", unsafe_allow_html=True)

    filtered_detail_df = prepared_detail_df.copy()

    if "Round" in filtered_detail_df.columns and selected_round != "All":
        filtered_detail_df = filtered_detail_df[filtered_detail_df["Round"] == selected_round].copy()

    if "Division" in filtered_detail_df.columns and selected_division != "All":
        filtered_detail_df = filtered_detail_df[filtered_detail_df["Division"] == selected_division].copy()

    render_table(filtered_detail_df, height=460)
