from __future__ import annotations

import pandas as pd

from report_round_weather_impacts.models import MPS_TO_MPH, REQUIRED_SCORED_COLS


def _rating_band(rating: float | int | None) -> str:
    if rating is None or pd.isna(rating):
        return "Unknown"
    if rating < 900:
        return "<900"
    if rating < 940:
        return "900-939"
    if rating < 970:
        return "940-969"
    return "970+"


def _temperature_band_f(temp_f: float | None) -> str:
    if temp_f is None or pd.isna(temp_f):
        return "Unknown"
    if temp_f < 41:
        return "<41F"
    if temp_f < 50:
        return "41-49F"
    if temp_f < 60:
        return "50-59F"
    if temp_f < 70:
        return "60-69F"
    if temp_f < 80:
        return "70-79F"
    return "80F+"


def prepare_reporting_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    missing_cols = [c for c in REQUIRED_SCORED_COLS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Scored input is missing required columns: {missing_cols}")

    out = df.copy()

    numeric_cols = [
        "player_rating",
        "actual_round_strokes",
        "predicted_round_strokes",
        "predicted_round_strokes_wind_reference",
        "estimated_wind_impact_strokes",
        "estimated_temperature_impact_strokes",
        "estimated_total_weather_impact_strokes",
        "round_wind_speed_mps_mean",
        "round_temp_c_mean",
    ]
    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(
        subset=[
            "actual_round_strokes",
            "predicted_round_strokes",
            "predicted_round_strokes_wind_reference",
            "estimated_wind_impact_strokes",
            "estimated_temperature_impact_strokes",
            "estimated_total_weather_impact_strokes",
            "round_wind_speed_mps_mean",
            "round_temp_c_mean",
        ]
    ).copy()

    if out.empty:
        raise ValueError("No rows remain after reporting dataframe preparation.")

    out["observed_wind_mph"] = out["round_wind_speed_mps_mean"] * MPS_TO_MPH
    out["observed_temp_f"] = (out["round_temp_c_mean"] * 9.0 / 5.0) + 32.0
    out["rating_band"] = out["player_rating"].apply(_rating_band)
    out["temperature_band_f"] = out["observed_temp_f"].apply(_temperature_band_f)
    out["precip_flag"] = "No Precip"

    if "round_precip_mm_sum" in out.columns:
        precip = pd.to_numeric(out["round_precip_mm_sum"], errors="coerce").fillna(0.0)
        out["precip_flag"] = precip.gt(0).map({True: "Precip", False: "No Precip"})

    out["round_year"] = pd.to_numeric(out["event_year"], errors="coerce").fillna(0).astype(int)

    if "round_date" in out.columns:
        dt = pd.to_datetime(out["round_date"], errors="coerce")
        out["round_month"] = dt.dt.month.fillna(0).astype(int)
    else:
        out["round_month"] = 0

    month_map = {
        1: "Jan",
        2: "Feb",
        3: "Mar",
        4: "Apr",
        5: "May",
        6: "Jun",
        7: "Jul",
        8: "Aug",
        9: "Sep",
        10: "Oct",
        11: "Nov",
        12: "Dec",
    }
    out["round_month_label"] = out["round_month"].map(month_map).fillna("Unknown")

    text_cols = ["division", "course_id", "layout_id", "player_key"]
    for col in text_cols:
        out[col] = out[col].astype("string").fillna("__MISSING__").astype(str)

    if "state" in out.columns:
        out["state"] = out["state"].astype("string").str.upper().fillna("UNKNOWN").astype(str)
    else:
        out["state"] = "UNKNOWN"

    if "city" not in out.columns:
        out["city"] = ""
    if "event_name" not in out.columns:
        out["event_name"] = ""
    if "lat" not in out.columns:
        out["lat"] = pd.NA
    if "lon" not in out.columns:
        out["lon"] = pd.NA

    return out
