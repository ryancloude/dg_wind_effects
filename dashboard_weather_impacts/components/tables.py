from __future__ import annotations

import pandas as pd
import streamlit as st


def render_table(df: pd.DataFrame, *, height: int = 350):
    st.dataframe(df, use_container_width=True, height=height, hide_index=True)
