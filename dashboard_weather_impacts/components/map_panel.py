from __future__ import annotations

import streamlit as st


def render_map_panel(fig, table_title: str):
    map_col, side_col = st.columns([3, 2])
    with map_col:
        st.plotly_chart(fig, use_container_width=True)
    with side_col:
        st.subheader(table_title)
