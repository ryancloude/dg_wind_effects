from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

import pandas as pd
from catboost import Pool

from score_round_wind_model.models import MPH_TO_MPS, REQUIRED_SCORE_INPUT_COLS, SCORE_POLICY_VERSION


@dataclass(frozen=True)
class ScoringResult:
    scored_df: pd.DataFrame
    scoring_manifest: dict[str, Any]


def _stable_sha256(payload: dict[str, Any]) -> str:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _wind_speed_bucket(speed_mps: float | None) -> str:
    if speed_mps is None:
        return "unknown"
    if speed_mps < 2.0:
        return "calm"
    if speed_mps < 5.0:
        return "light"
    if speed_mps < 8.0:
        return "moderate"
    if speed_mps < 12.0:
        return "strong"
    return "very_strong"


def _wind_gust_bucket(speed_mps: float | None) -> str:
    if speed_mps is None:
        return "unknown"
    if speed_mps < 3.0:
        return "low"
    if speed_mps < 6.0:
        return "mild"
    if speed_mps < 10.0:
        return "high"
    return "very_high"


def compute_scoring_request_fingerprint(
    *,
    event_object: dict[str, Any],
    training_request_fingerprint: str,
) -> str:
    payload = {
        "score_policy_version": SCORE_POLICY_VERSION,
        "training_request_fingerprint": training_request_fingerprint,
        "event_object": {
            "key": str(event_object.get("key", "")),
            "etag": str(event_object.get("etag", "")),
            "size": int(event_object.get("size", 0) or 0),
            "last_modified": str(event_object.get("last_modified", "")),
        },
    }
    return _stable_sha256(payload)


def prepare_scoring_dataframe(
    *,
    df: pd.DataFrame,
    feature_columns: list[str],
    categorical_feature_columns: list[str],
) -> pd.DataFrame:
    if df.empty:
        raise ValueError("No rows found for scoring.")

    missing_cols = [c for c in REQUIRED_SCORE_INPUT_COLS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Scoring input is missing required columns: {missing_cols}")

    score_df = df.copy()

    for col in feature_columns:
        if col not in score_df.columns:
            raise ValueError(f"Scoring dataframe is missing model feature column: {col}")

    numeric_feature_cols = [c for c in feature_columns if c not in set(categorical_feature_columns)]
    for col in numeric_feature_cols:
        score_df[col] = pd.to_numeric(score_df[col], errors="coerce")

    score_df = score_df.dropna(subset=numeric_feature_cols).copy()

    for col in categorical_feature_columns:
        score_df[col] = score_df[col].astype("string").fillna("__MISSING__").astype(str)

    if score_df.empty:
        raise ValueError("No rows remain after scoring input preparation.")

    return score_df


def _prepare_pool(
    *,
    df: pd.DataFrame,
    feature_columns: list[str],
    categorical_feature_columns: list[str],
) -> Pool:
    cat_idx = [feature_columns.index(c) for c in categorical_feature_columns]
    return Pool(
        data=df[feature_columns],
        cat_features=cat_idx,
    )


def _build_wind_reference_df(
    df: pd.DataFrame,
    *,
    wind_speed_reference_mph: float,
    wind_gust_reference_mph: float,
) -> pd.DataFrame:
    out = df.copy()
    wind_speed_mps = wind_speed_reference_mph * MPH_TO_MPS
    wind_gust_mps = wind_gust_reference_mph * MPH_TO_MPS

    out["round_wind_speed_mps_mean"] = wind_speed_mps
    out["round_wind_speed_mps_max"] = wind_speed_mps
    out["round_wind_gust_mps_mean"] = wind_gust_mps
    out["round_wind_gust_mps_max"] = wind_gust_mps
    out["round_wind_speed_bucket"] = _wind_speed_bucket(wind_speed_mps)
    out["round_wind_gust_bucket"] = _wind_gust_bucket(wind_gust_mps)
    return out


def _build_temperature_reference_df(
    df: pd.DataFrame,
    *,
    temperature_reference_c: float,
) -> pd.DataFrame:
    out = df.copy()
    out["round_temp_c_mean"] = float(temperature_reference_c)
    return out


def _build_total_weather_reference_df(
    df: pd.DataFrame,
    *,
    wind_speed_reference_mph: float,
    wind_gust_reference_mph: float,
    temperature_reference_c: float,
    precip_reference_mm: float,
    pressure_reference_hpa: float,
    humidity_reference_pct: float,
) -> pd.DataFrame:
    out = _build_wind_reference_df(
        df,
        wind_speed_reference_mph=wind_speed_reference_mph,
        wind_gust_reference_mph=wind_gust_reference_mph,
    )
    out["round_temp_c_mean"] = float(temperature_reference_c)
    out["round_precip_mm_sum"] = float(precip_reference_mm)
    out["round_precip_mm_mean"] = float(precip_reference_mm)
    out["round_pressure_hpa_mean"] = float(pressure_reference_hpa)
    out["round_humidity_pct_mean"] = float(humidity_reference_pct)
    return out


def score_round_rows(
    *,
    df: pd.DataFrame,
    model,
    training_manifest: dict[str, Any],
    feature_columns: list[str],
    categorical_feature_columns: list[str],
    training_request_fingerprint: str,
    scoring_run_id: str,
    scored_at_utc: str,
    scoring_request_fingerprint: str,
    model_artifact_prefix: str,
) -> ScoringResult:
    score_df = prepare_scoring_dataframe(
        df=df,
        feature_columns=feature_columns,
        categorical_feature_columns=categorical_feature_columns,
    )

    actual_pool = _prepare_pool(
        df=score_df,
        feature_columns=feature_columns,
        categorical_feature_columns=categorical_feature_columns,
    )
    actual_pred = model.predict(actual_pool)

    wind_reference_df = _build_wind_reference_df(
        score_df,
        wind_speed_reference_mph=float(training_manifest["wind_speed_reference_mph"]),
        wind_gust_reference_mph=float(training_manifest["wind_gust_reference_mph"]),
    )
    wind_reference_pool = _prepare_pool(
        df=wind_reference_df,
        feature_columns=feature_columns,
        categorical_feature_columns=categorical_feature_columns,
    )
    wind_reference_pred = model.predict(wind_reference_pool)

    temperature_reference_df = _build_temperature_reference_df(
        score_df,
        temperature_reference_c=float(training_manifest["temperature_reference_c"]),
    )
    temperature_reference_pool = _prepare_pool(
        df=temperature_reference_df,
        feature_columns=feature_columns,
        categorical_feature_columns=categorical_feature_columns,
    )
    temperature_reference_pred = model.predict(temperature_reference_pool)

    total_weather_reference_df = _build_total_weather_reference_df(
        score_df,
        wind_speed_reference_mph=float(training_manifest["wind_speed_reference_mph"]),
        wind_gust_reference_mph=float(training_manifest["wind_gust_reference_mph"]),
        temperature_reference_c=float(training_manifest["temperature_reference_c"]),
        precip_reference_mm=float(training_manifest["precip_reference_mm"]),
        pressure_reference_hpa=float(training_manifest["pressure_reference_hpa"]),
        humidity_reference_pct=float(training_manifest["humidity_reference_pct"]),
    )
    total_weather_reference_pool = _prepare_pool(
        df=total_weather_reference_df,
        feature_columns=feature_columns,
        categorical_feature_columns=categorical_feature_columns,
    )
    total_weather_reference_pred = model.predict(total_weather_reference_pool)

    scored_df = score_df.copy()
    scored_df["predicted_round_strokes"] = actual_pred
    scored_df["predicted_round_strokes_wind_reference"] = wind_reference_pred
    scored_df["predicted_round_strokes_temperature_reference"] = temperature_reference_pred
    scored_df["predicted_round_strokes_total_weather_reference"] = total_weather_reference_pred

    scored_df["estimated_wind_impact_strokes"] = (
        scored_df["predicted_round_strokes"] - scored_df["predicted_round_strokes_wind_reference"]
    )
    scored_df["estimated_temperature_impact_strokes"] = (
        scored_df["predicted_round_strokes"] - scored_df["predicted_round_strokes_temperature_reference"]
    )
    scored_df["estimated_total_weather_impact_strokes"] = (
        scored_df["predicted_round_strokes"] - scored_df["predicted_round_strokes_total_weather_reference"]
    )

    scored_df["model_name"] = str(training_manifest["model_name"])
    scored_df["model_version"] = str(training_manifest["model_version"])
    scored_df["training_request_fingerprint"] = training_request_fingerprint
    scored_df["scoring_run_id"] = scoring_run_id
    scored_df["scored_at_utc"] = scored_at_utc
    scored_df["scoring_request_fingerprint"] = scoring_request_fingerprint
    scored_df["model_artifact_prefix"] = model_artifact_prefix

    scoring_manifest = {
        "model_name": str(training_manifest["model_name"]),
        "model_version": str(training_manifest["model_version"]),
        "training_request_fingerprint": training_request_fingerprint,
        "scoring_request_fingerprint": scoring_request_fingerprint,
        "wind_speed_reference_mph": float(training_manifest["wind_speed_reference_mph"]),
        "wind_gust_reference_mph": float(training_manifest["wind_gust_reference_mph"]),
        "temperature_reference_c": float(training_manifest["temperature_reference_c"]),
        "precip_reference_mm": float(training_manifest["precip_reference_mm"]),
        "pressure_reference_hpa": float(training_manifest["pressure_reference_hpa"]),
        "humidity_reference_pct": float(training_manifest["humidity_reference_pct"]),
        "rows_scored": int(len(scored_df)),
    }

    return ScoringResult(
        scored_df=scored_df,
        scoring_manifest=scoring_manifest,
    )