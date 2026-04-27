from __future__ import annotations

import streamlit as st

from dashboard_weather_impacts.config import load_dashboard_config
from dashboard_weather_impacts.data_access import load_page_datasets
from dashboard_weather_impacts.filters import empty_filters
from dashboard_weather_impacts.pages.event_explorer import render_event_explorer
from dashboard_weather_impacts.pages.geography import render_geography
from dashboard_weather_impacts.pages.methodology import render_methodology
from dashboard_weather_impacts.pages.overview import render_overview


def _inject_app_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background: #f6f2eb;
            color: #2f3441;
        }

        [data-testid="stAppViewContainer"] > .main {
            background: #f6f2eb;
        }

        [data-testid="stSidebar"] {
            background: #efe7db;
            border-right: 1px solid #e1d6c5;
        }

        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] {
            color: #2f3441;
        }

        [data-testid="stSidebarNav"] {
            padding-top: 0.5rem;
        }

        [data-testid="stSidebarNav"] a {
            border-radius: 14px;
            margin-bottom: 0.25rem;
        }

        [data-testid="stSidebarNav"] a:hover {
            background: rgba(180, 83, 9, 0.08);
        }

        [data-testid="block-container"] {
            max-width: 1320px;
            padding-top: 2rem;
            padding-bottom: 3rem;
        }

        h1, h2, h3 {
            color: #2f3441;
            letter-spacing: -0.02em;
        }

        .app-shell-kicker {
            font-size: 0.76rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #b45309;
            margin-bottom: 0.2rem;
        }

        .app-shell-title {
            font-size: 1.95rem;
            font-weight: 700;
            line-height: 1.05;
            color: #2f3441;
            margin-bottom: 0.35rem;
        }

        .app-shell-subtitle {
            font-size: 1rem;
            line-height: 1.5;
            color: #5b6170;
            max-width: 900px;
            margin-bottom: 1.5rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _run_overview():
    config = load_dashboard_config()
    datasets = load_page_datasets(config, "Overview")
    render_overview(empty_filters(), datasets)


def _run_geography():
    config = load_dashboard_config()
    datasets = load_page_datasets(config, "Geography")
    render_geography(empty_filters(), datasets)


def _run_event_explorer():
    config = load_dashboard_config()
    datasets = load_page_datasets(config, "Event Explorer")
    render_event_explorer(empty_filters(), datasets, config)


def _run_methodology():
    render_methodology()


def main():
    st.set_page_config(
        page_title="Disc Golf Weather Impact Dashboard",
        layout="wide",
    )

    _inject_app_styles()

    st.markdown('<div class="app-shell-kicker">Disc Golf Wind Effects</div>', unsafe_allow_html=True)
    st.markdown('<div class="app-shell-title">Weather Impact Dashboard</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="app-shell-subtitle">Model-based estimates of how weather conditions change scoring difficulty across tracked disc golf rounds, events, and venues.</div>',
        unsafe_allow_html=True,
    )

    navigation = st.navigation(
        [
            st.Page(_run_overview, title="Overview", default=True),
            st.Page(_run_geography, title="Geography"),
            st.Page(_run_event_explorer, title="Event Explorer"),
            st.Page(_run_methodology, title="Methodology"),
        ],
        position="sidebar",
    )

    navigation.run()


if __name__ == "__main__":
    main()
