from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


_WIND_BUCKET_ORDER = ["calm", "light", "moderate", "strong", "very_strong", "unknown"]
_ACCENT = "#B45309"
_ACCENT_LIGHT = "#D97706"
_GRID = "#E8E0D3"
_TEXT = "#2F3441"
_MUTED = "#5B6170"
_SECONDARY = "#2563EB"
_TERTIARY = "#0F766E"


def effect_distribution_chart(df: pd.DataFrame, *, title: str, metric_label: str):
    plot_df = df.copy()

    if "sort_order" in plot_df.columns:
        plot_df = plot_df.sort_values("sort_order")
    elif "impact_bin_start" in plot_df.columns:
        plot_df = plot_df.sort_values("impact_bin_start", na_position="first")

    return px.bar(
        plot_df,
        x="impact_bin_label",
        y="rounds_scored",
        hover_data=["impact_bin_start", "impact_bin_end"],
        title=title,
        labels={
            "impact_bin_label": metric_label,
            "rounds_scored": "Rounds Scored",
        },
    )


def overview_distribution_chart(df: pd.DataFrame, *, title: str, x_label: str):
    plot_df = df.copy()

    if "sort_order" in plot_df.columns:
        plot_df = plot_df.sort_values("sort_order")

    plot_df["share_label"] = plot_df["share_of_rounds"].map(
        lambda x: f"{x:.1%}" if pd.notna(x) else ""
    )

    fig = px.bar(
        plot_df,
        x="bin_label",
        y="share_of_rounds",
        text="share_label",
        title=title,
        labels={
            "bin_label": x_label,
            "share_of_rounds": "Share of Rounds",
        },
        color_discrete_sequence=[_ACCENT],
        hover_data={
            "rounds_tracked": True,
            "share_of_rounds": ":.2%",
            "bin_start": True,
            "bin_end": True,
            "sort_order": False,
        },
    )

    fig.update_traces(
        textposition="outside",
        cliponaxis=False,
        marker_line_width=0,
        textfont=dict(color=_TEXT, size=16),
    )

    fig.update_yaxes(
        tickformat=".0%",
        title_text="Share of Rounds",
        title_font=dict(color=_MUTED, size=15),
        tickfont=dict(color=_MUTED, size=13),
        gridcolor=_GRID,
        zeroline=False,
    )

    fig.update_xaxes(
        title_text=x_label,
        title_font=dict(color=_MUTED, size=15),
        tickfont=dict(color=_MUTED, size=13),
        showgrid=False,
        tickangle=-25 if len(plot_df) > 6 else 0,
    )

    fig.update_layout(
        showlegend=False,
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color=_TEXT),
        margin=dict(l=20, r=20, t=72, b=50),
        title=dict(
            text=title,
            font=dict(size=22, color=_TEXT),
            x=0.0,
            xanchor="left",
        ),
    )
    return fig


def overview_wind_impact_points_chart(df: pd.DataFrame, *, bucket_metric: str):
    plot_df = df[df["bucket_metric"] == bucket_metric].copy().sort_values("sort_order")

    x_title = "Average Wind Speed Bucket" if bucket_metric == "wind_speed" else "Average Wind Gust Bucket"

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=plot_df["bucket_label"],
            y=plot_df["avg_added_strokes_from_wind"],
            mode="lines+markers",
            line=dict(color=_ACCENT, width=4),
            marker=dict(
                size=13,
                color=_ACCENT,
                line=dict(color="white", width=2),
            ),
            customdata=plot_df[["rounds_tracked"]],
            hovertemplate=(
                f"{x_title}: %{{x}}<br>"
                "Avg Added Strokes from Wind: %{y:.2f}<br>"
                "Rounds Tracked: %{customdata[0]:,.0f}<extra></extra>"
            ),
            showlegend=False,
        )
    )

    fig.add_hline(y=0, line_dash="dash", line_color="#9CA3AF", line_width=1.5)

    fig.update_layout(
        title=dict(
            text="Average Added Strokes from Wind by Bucket",
            font=dict(size=22, color=_TEXT),
            x=0.0,
            xanchor="left",
        ),
        xaxis_title="Average Wind Speed Bucket" if bucket_metric == "wind_speed" else "Average Wind Gust Bucket",
        yaxis_title="Avg Added Strokes from Wind",
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color=_TEXT),
        margin=dict(l=20, r=20, t=72, b=40),
    )

    fig.update_yaxes(
        gridcolor=_GRID,
        zeroline=False,
        title_font=dict(color=_MUTED, size=15),
        tickfont=dict(color=_MUTED, size=13),
    )

    fig.update_xaxes(
        showgrid=False,
        type="category",
        title_font=dict(color=_MUTED, size=15),
        tickfont=dict(color=_MUTED, size=13),
    )

    return fig


def _event_round_summary(df: pd.DataFrame) -> pd.DataFrame:
    plot_df = df.copy()

    if plot_df.empty:
        return plot_df

    grouped_rows: list[dict[str, object]] = []

    for round_number, group in plot_df.groupby("round_number", sort=True):
        rounds_weight = (
            pd.to_numeric(group["rounds_scored"], errors="coerce")
            if "rounds_scored" in group.columns
            else None
        )

        def weighted_avg(col: str) -> float | None:
            if col not in group.columns:
                return None

            values = pd.to_numeric(group[col], errors="coerce")
            valid = values.notna()

            if not valid.any():
                return None

            if rounds_weight is not None:
                weights = rounds_weight[valid]
                vals = values[valid]
                if weights.notna().any() and float(weights.fillna(0).sum()) > 0:
                    return float((vals * weights.fillna(0)).sum() / weights.fillna(0).sum())

            return float(values[valid].mean())

        round_date = ""
        if "round_date" in group.columns:
            non_null_dates = [str(x).strip() for x in group["round_date"].tolist() if str(x).strip()]
            if non_null_dates:
                round_date = non_null_dates[0]

        grouped_rows.append(
            {
                "round_number": int(round_number),
                "round_date": round_date,
                "avg_estimated_wind_impact_strokes": weighted_avg("avg_estimated_wind_impact_strokes"),
                "avg_estimated_total_weather_impact_strokes": weighted_avg("avg_estimated_total_weather_impact_strokes"),
                "avg_observed_wind_mph": weighted_avg("avg_observed_wind_mph"),
                "avg_observed_wind_gust_mph": weighted_avg("avg_observed_wind_gust_mph"),
                "avg_observed_temp_f": weighted_avg("avg_observed_temp_f"),
            }
        )

    return pd.DataFrame(grouped_rows).sort_values("round_number").reset_index(drop=True)


def event_round_impact_chart(df: pd.DataFrame):
    plot_df = _event_round_summary(df)

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=plot_df["round_number"],
            y=plot_df["avg_estimated_wind_impact_strokes"],
            mode="lines+markers",
            name="Avg Wind Added Strokes",
            line=dict(color=_ACCENT, width=3),
            marker=dict(size=16, color=_ACCENT, line=dict(color="white", width=2.5)),
            customdata=plot_df[["round_date"]] if "round_date" in plot_df.columns else None,
            hovertemplate=(
                "Round %{x}<br>"
                "Round Date: %{customdata[0]}<br>"
                "Avg Wind Added Strokes: %{y:.2f}<extra></extra>"
            ),
        )
    )

    fig.add_trace(
        go.Scatter(
            x=plot_df["round_number"],
            y=plot_df["avg_estimated_total_weather_impact_strokes"],
            mode="lines+markers",
            name="Avg Total Weather Added Strokes",
            line=dict(color=_SECONDARY, width=3),
            marker=dict(size=16, color=_SECONDARY, line=dict(color="white", width=2.5)),
            customdata=plot_df[["round_date"]] if "round_date" in plot_df.columns else None,
            hovertemplate=(
                "Round %{x}<br>"
                "Round Date: %{customdata[0]}<br>"
                "Avg Total Weather Added Strokes: %{y:.2f}<extra></extra>"
            ),
        )
    )

    fig.add_hline(y=0, line_dash="dash", line_color="#9CA3AF", line_width=1.2)

    fig.update_layout(
        title=dict(
            text="Round-by-Round Weather Impact",
            font=dict(size=20, color=_TEXT),
            x=0.0,
            xanchor="left",
        ),
        xaxis_title="Round Number",
        yaxis_title="Added Strokes",
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color=_TEXT),
        margin=dict(l=20, r=20, t=92, b=45),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0.0,
            font=dict(size=12),
            bgcolor="rgba(0,0,0,0)",
        ),
        dragmode=False,
    )

    fig.update_yaxes(
        gridcolor=_GRID,
        zeroline=False,
        fixedrange=True,
        title_font=dict(color=_MUTED, size=15),
        tickfont=dict(color=_MUTED, size=13),
    )

    fig.update_xaxes(
        dtick=1,
        showgrid=False,
        fixedrange=True,
        title_font=dict(color=_MUTED, size=15),
        tickfont=dict(color=_MUTED, size=13),
    )

    return fig


def event_round_conditions_chart(df: pd.DataFrame, *, metric_key: str):
    plot_df = _event_round_summary(df)

    metric_config = {
        "wind_speed": {
            "column": "avg_observed_wind_mph",
            "title": "Round-by-Round Wind Speed",
            "label": "Avg Wind (mph)",
            "yaxis_title": "Wind Speed (mph)",
            "color": _ACCENT,
            "format": "%{y:.1f} mph",
        },
        "wind_gust": {
            "column": "avg_observed_wind_gust_mph",
            "title": "Round-by-Round Wind Gust",
            "label": "Avg Wind Gust (mph)",
            "yaxis_title": "Wind Gust (mph)",
            "color": _SECONDARY,
            "format": "%{y:.1f} mph",
        },
        "temperature": {
            "column": "avg_observed_temp_f",
            "title": "Round-by-Round Temperature",
            "label": "Avg Temperature (F)",
            "yaxis_title": "Temperature (F)",
            "color": _TERTIARY,
            "format": "%{y:.1f} F",
        },
    }[metric_key]

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=plot_df["round_number"],
            y=plot_df[metric_config["column"]],
            mode="lines+markers",
            name=metric_config["label"],
            line=dict(color=metric_config["color"], width=3),
            marker=dict(size=16, color=metric_config["color"], line=dict(color="white", width=2.5)),
            customdata=plot_df[["round_date"]] if "round_date" in plot_df.columns else None,
            hovertemplate=(
                "Round %{x}<br>"
                "Round Date: %{customdata[0]}<br>"
                f"{metric_config['label']}: {metric_config['format']}<extra></extra>"
            ),
            showlegend=False,
        )
    )

    fig.update_layout(
        title=dict(
            text=metric_config["title"],
            font=dict(size=20, color=_TEXT),
            x=0.0,
            xanchor="left",
        ),
        xaxis=dict(
            title="Round Number",
            dtick=1,
            showgrid=False,
            fixedrange=True,
            title_font=dict(color=_MUTED, size=15),
            tickfont=dict(color=_MUTED, size=13),
        ),
        yaxis=dict(
            title=metric_config["yaxis_title"],
            gridcolor=_GRID,
            zeroline=False,
            fixedrange=True,
            title_font=dict(color=_MUTED, size=15),
            tickfont=dict(color=_MUTED, size=13),
        ),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color=_TEXT),
        margin=dict(l=20, r=20, t=92, b=45),
        dragmode=False,
    )

    return fig


def wind_bucket_bar_chart(df: pd.DataFrame):
    plot_df = df.copy()

    if "round_wind_speed_bucket" in plot_df.columns:
        plot_df["round_wind_speed_bucket"] = pd.Categorical(
            plot_df["round_wind_speed_bucket"],
            categories=_WIND_BUCKET_ORDER,
            ordered=True,
        )
        plot_df = plot_df.sort_values("round_wind_speed_bucket")

    plot_df["wind_effect_label"] = plot_df["avg_estimated_wind_impact_strokes"].map(
        lambda x: f"{x:.2f}" if pd.notna(x) else ""
    )

    fig = px.bar(
        plot_df,
        x="round_wind_speed_bucket",
        y="avg_estimated_wind_impact_strokes",
        text="wind_effect_label",
        hover_data=["rounds_scored", "avg_observed_wind_mph", "avg_estimated_total_weather_impact_strokes"],
        title="Average Wind Effect vs Reference by Wind Bucket",
        labels={
            "round_wind_speed_bucket": "Wind Bucket",
            "avg_estimated_wind_impact_strokes": "Avg Wind Effect vs Ref (strokes)",
        },
    )
    fig.update_traces(textposition="outside", cliponaxis=False)
    fig.update_layout(showlegend=False)
    return fig


def temperature_band_bar_chart(df: pd.DataFrame):
    return px.bar(
        df,
        x="temperature_band_f",
        y="avg_estimated_temperature_impact_strokes",
        hover_data=["rounds_scored", "avg_observed_temp_f", "avg_estimated_total_weather_impact_strokes"],
        title="Average Estimated Temperature Effect vs Reference by Temperature Band",
    )


def monthly_weather_line_chart(df: pd.DataFrame):
    return px.line(
        df.sort_values(["round_year", "round_month"]),
        x="round_month_label",
        y="avg_estimated_total_weather_impact_strokes",
        markers=True,
        title="Average Estimated Total Weather Effect vs Reference by Month",
    )


def division_bar_chart(df: pd.DataFrame):
    return px.bar(
        df.sort_values("avg_estimated_wind_impact_strokes", ascending=False),
        x="division",
        y="avg_estimated_wind_impact_strokes",
        hover_data=["rounds_scored", "avg_estimated_total_weather_impact_strokes"],
        title="Average Estimated Wind Effect vs Reference by Division",
    )


def rating_band_chart(df: pd.DataFrame):
    return px.bar(
        df,
        x="rating_band",
        y="avg_estimated_wind_impact_strokes",
        hover_data=["rounds_scored", "avg_estimated_total_weather_impact_strokes"],
        title="Average Estimated Wind Effect vs Reference by Rating Band",
    )


def course_layout_scatter(df: pd.DataFrame):
    return px.scatter(
        df,
        x="avg_observed_wind_mph",
        y="avg_estimated_wind_impact_strokes",
        size="rounds_scored",
        color="state" if "state" in df.columns else None,
        hover_data=["course_id", "layout_id", "avg_estimated_total_weather_impact_strokes"],
        title="Venue Weather Sensitivity Scatter",
    )


def state_choropleth(df: pd.DataFrame, metric_col: str, title: str, metric_label: str):
    plot_df = df.copy()

    if metric_label == "Average Observed Wind":
        metric_display = "Average Observed Wind (mph)"
        metric_format = "%{customdata[2]:.2f}"
    elif metric_label == "Average Observed Temperature":
        metric_display = "Average Observed Temperature (F)"
        metric_format = "%{customdata[2]:.2f}"
    elif metric_label == "Average Wind Impact":
        metric_display = "Average Wind Impact (strokes)"
        metric_format = "%{customdata[2]:.2f}"
    elif metric_label == "Average Total Weather Impact":
        metric_display = "Average Total Weather Impact (strokes)"
        metric_format = "%{customdata[2]:.2f}"
    elif metric_label == "Number of Events":
        metric_display = "Number of Events"
        metric_format = "%{customdata[2]:,.0f}"
    elif metric_label == "Number of Rounds":
        metric_display = "Number of Rounds"
        metric_format = "%{customdata[2]:,.0f}"
    else:
        metric_display = metric_label
        metric_format = "%{customdata[2]:.2f}"

    plot_df["hover_events_scored"] = plot_df["events_scored"]
    plot_df["hover_rounds_scored"] = plot_df["rounds_scored"]
    plot_df["hover_metric_value"] = plot_df[metric_col]

    fig = px.choropleth(
        plot_df,
        locations="state_code",
        locationmode="USA-states",
        color=metric_col,
        scope="usa",
        hover_name="state_name",
        custom_data=["hover_events_scored", "hover_rounds_scored", "hover_metric_value"],
        title=title,
        color_continuous_scale="YlOrBr",
    )

    fig.update_traces(
        hovertemplate=(
            "<b>%{hovertext}</b><br>"
            "Number of Events: %{customdata[0]:,.0f}<br>"
            "Number of Rounds: %{customdata[1]:,.0f}<br>"
            f"{metric_display}: {metric_format}"
            "<extra></extra>"
        )
    )

    fig.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color=_TEXT),
        margin=dict(l=20, r=20, t=72, b=20),
        title=dict(
            text=title,
            font=dict(size=22, color=_TEXT),
            x=0.0,
            xanchor="left",
        ),
        coloraxis_colorbar=dict(
            title=dict(
                text=metric_display,
                font=dict(color=_MUTED, size=14),
            ),
            tickfont=dict(color=_MUTED, size=12),
        ),
        geo=dict(
            bgcolor="white",
            scope="usa",
            projection_type="albers usa",
            center=dict(lat=38, lon=-96),
            showlakes=False,
            lakecolor="white",
            domain=dict(x=[0.0, 0.86], y=[0.0, 1.0]),
        ),
        dragmode=False,
    )
    return fig


def event_point_map(df: pd.DataFrame, metric_col: str, title: str):
    return px.scatter_geo(
        df.dropna(subset=["lat", "lon"]),
        lat="lat",
        lon="lon",
        color=metric_col,
        size="rounds_scored",
        hover_name="event_name" if "event_name" in df.columns else None,
        hover_data=["tourn_id", "state", "round_month_label"],
        scope="usa",
        title=title,
    )


def event_round_trend_chart(df: pd.DataFrame):
    return px.line(
        df.sort_values("round_number"),
        x="round_number",
        y="avg_estimated_wind_impact_strokes",
        markers=True,
        title="Average Estimated Wind Effect vs Reference by Round",
    )


def actual_vs_predicted_chart(df: pd.DataFrame):
    plot_df = df.sort_values("round_number").copy()
    fig = go.Figure()
    fig.add_bar(x=plot_df["round_number"], y=plot_df["avg_actual_round_strokes"], name="Actual")
    fig.add_bar(x=plot_df["round_number"], y=plot_df["avg_predicted_round_strokes"], name="Predicted")
    fig.add_bar(
        x=plot_df["round_number"],
        y=plot_df["avg_predicted_round_strokes_wind_reference"],
        name="Wind Reference Predicted",
    )
    fig.update_layout(
        barmode="group",
        title="Actual vs Predicted by Round",
        xaxis_title="Round Number",
        yaxis_title="Strokes",
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color=_TEXT),
        margin=dict(l=20, r=20, t=72, b=40),
        title_font=dict(size=22, color=_TEXT),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0.0,
        ),
    )
    fig.update_yaxes(
        gridcolor=_GRID,
        zeroline=False,
        title_font=dict(color=_MUTED, size=15),
        tickfont=dict(color=_MUTED, size=13),
    )
    fig.update_xaxes(
        showgrid=False,
        title_font=dict(color=_MUTED, size=15),
        tickfont=dict(color=_MUTED, size=13),
    )
    return fig
