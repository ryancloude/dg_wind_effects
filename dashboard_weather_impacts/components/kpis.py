from __future__ import annotations

import streamlit as st


def render_kpi_row(kpis: list[tuple[str, str]], *, columns_per_row: int | None = None):
    if not kpis:
        return

    per_row = columns_per_row or len(kpis)

    for start_idx in range(0, len(kpis), per_row):
        row_kpis = kpis[start_idx : start_idx + per_row]
        cols = st.columns(len(row_kpis), gap="large")
        for col, (label, value) in zip(cols, row_kpis):
            with col:
                st.metric(label, value)
