from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor, Pool
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from train_round_wind_model.models import (
    CATBOOST_PARAMS,
    CATEGORICAL_FEATURES,
    EARLY_STOPPING_ROUNDS,
    FEATURE_COLUMNS,
    MIN_HOLES_PLAYED,
    MODEL_NAME,
    MODEL_VERSION,
    NUMERIC_FEATURES,
    RANDOM_STATE,
    REFERENCE_PRECIP_FLAG,
    REFERENCE_TEMPERATURE_C,
    REFERENCE_WIND_GUST_MPH,
    REFERENCE_WIND_SPEED_MPH,
    REQUIRE_WEATHER_AVAILABLE,
    REQUIRED_INPUT_COLS,
    TARGET_COL,
    TEST_SIZE,
    VALID_SIZE_WITHIN_TRAIN,
)


@dataclass(frozen=True)
class TrainingResult:
    model: CatBoostRegressor
    metrics: dict[str, Any]
    training_manifest: dict[str, Any]
    feature_importance_rows: list[dict[str, Any]]


def _stable_sha256(payload: dict[str, Any]) -> str:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def regression_metrics(y_true: pd.Series, y_pred: np.ndarray) -> dict[str, float]:
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "r2": float(r2_score(y_true, y_pred)),
    }


def compute_dataset_fingerprint(source_objects: list[dict[str, Any]]) -> str:
    payload = {
        "source_objects": [
            {
                "key": str(obj.get("key", "")),
                "etag": str(obj.get("etag", "")),
                "size": int(obj.get("size", 0) or 0),
                "last_modified": str(obj.get("last_modified", "")),
            }
            for obj in sorted(source_objects, key=lambda x: str(x.get("key", "")))
        ]
    }
    return _stable_sha256(payload)


def compute_training_request_fingerprint(
    *,
    dataset_fingerprint: str,
    event_ids: list[int] | None = None,
) -> str:
    payload = {
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "dataset_fingerprint": dataset_fingerprint,
        "target_col": TARGET_COL,
        "numeric_features": list(NUMERIC_FEATURES),
        "categorical_features": list(CATEGORICAL_FEATURES),
        "require_weather_available": REQUIRE_WEATHER_AVAILABLE,
        "min_holes_played": MIN_HOLES_PLAYED,
        "test_size": TEST_SIZE,
        "valid_size_within_train": VALID_SIZE_WITHIN_TRAIN,
        "random_state": RANDOM_STATE,
        "catboost_params": CATBOOST_PARAMS,
        "event_ids": sorted(int(x) for x in event_ids) if event_ids else None,
    }
    return _stable_sha256(payload)


def prepare_training_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    if df.empty:
        raise ValueError("No round model-input rows were loaded for training.")

    missing_cols = [c for c in REQUIRED_INPUT_COLS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Training input is missing required columns: {missing_cols}")

    df = df.copy()

    stats = {
        "input_rows": int(len(df)),
    }

    if REQUIRE_WEATHER_AVAILABLE:
        df = df[df["weather_available_flag"] == True].copy()  # noqa: E712
    stats["rows_after_weather_filter"] = int(len(df))

    df["hole_count"] = pd.to_numeric(df["hole_count"], errors="coerce")
    df = df[df["hole_count"] >= MIN_HOLES_PLAYED].copy()
    stats["rows_after_hole_filter"] = int(len(df))

    raw_numeric_required = [
        TARGET_COL,
        "player_rating",
        "round_number",
        "hole_count",
        "round_total_hole_length",
        "round_avg_hole_length",
        "round_total_par",
        "round_avg_hole_par",
        "round_length_over_par",
        "round_wind_speed_mps_mean",
        "round_wind_gust_mps_mean",
        "round_temp_c_mean",
        "round_precip_mm_sum",
    ]

    for col in raw_numeric_required:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=raw_numeric_required).copy()
    stats["rows_after_numeric_not_null_filter"] = int(len(df))

    df["precip_during_round_flag"] = df["round_precip_mm_sum"].fillna(0.0).gt(0.0).astype(int)

    for col in CATEGORICAL_FEATURES:
        df[col] = df[col].astype("string").fillna("__MISSING__").astype(str)

    if df.empty:
        raise ValueError("No rows remain after training filters.")

    return df, stats


def _prepare_split_frames(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str], list[int]]:
    train_full_df, test_df = train_test_split(
        df,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE,
    )
    train_df, valid_df = train_test_split(
        train_full_df,
        test_size=VALID_SIZE_WITHIN_TRAIN,
        random_state=RANDOM_STATE,
    )

    feature_cols = list(FEATURE_COLUMNS)

    for col in CATEGORICAL_FEATURES:
        train_df[col] = train_df[col].astype("string").fillna("__MISSING__").astype(str)
        valid_df[col] = valid_df[col].astype("string").fillna("__MISSING__").astype(str)
        test_df[col] = test_df[col].astype("string").fillna("__MISSING__").astype(str)

        train_levels = set(train_df[col].unique())
        valid_df[col] = valid_df[col].where(valid_df[col].isin(train_levels), "__MISSING__")
        test_df[col] = test_df[col].where(test_df[col].isin(train_levels), "__MISSING__")

    cat_idx = [feature_cols.index(c) for c in CATEGORICAL_FEATURES]
    return train_df, valid_df, test_df, feature_cols, cat_idx


def train_round_model(
    *,
    df: pd.DataFrame,
    dataset_fingerprint: str,
    training_request_fingerprint: str,
    source_key_count: int,
    event_ids: list[int] | None = None,
) -> TrainingResult:
    df, filter_stats = prepare_training_dataframe(df)

    train_df, valid_df, test_df, feature_cols, cat_idx = _prepare_split_frames(df)

    train_pool = Pool(
        data=train_df[feature_cols],
        label=train_df[TARGET_COL],
        cat_features=cat_idx,
    )
    valid_pool = Pool(
        data=valid_df[feature_cols],
        label=valid_df[TARGET_COL],
        cat_features=cat_idx,
    )
    test_pool = Pool(
        data=test_df[feature_cols],
        label=test_df[TARGET_COL],
        cat_features=cat_idx,
    )

    model = CatBoostRegressor(**CATBOOST_PARAMS)
    model.fit(
        train_pool,
        eval_set=valid_pool,
        use_best_model=True,
        early_stopping_rounds=EARLY_STOPPING_ROUNDS,
    )

    test_pred = model.predict(test_pool)
    metrics = {
        **regression_metrics(test_df[TARGET_COL], test_pred),
        "best_iteration": int(getattr(model, "best_iteration_", -1)),
        "input_rows": int(filter_stats["input_rows"]),
        "rows_after_weather_filter": int(filter_stats["rows_after_weather_filter"]),
        "rows_after_hole_filter": int(filter_stats["rows_after_hole_filter"]),
        "rows_after_numeric_not_null_filter": int(filter_stats["rows_after_numeric_not_null_filter"]),
        "train_rows": int(len(train_df)),
        "valid_rows": int(len(valid_df)),
        "test_rows": int(len(test_df)),
    }

    feature_importance_rows = [
        {
            "feature": feature_name,
            "importance": float(importance),
        }
        for feature_name, importance in zip(
            feature_cols,
            model.get_feature_importance(train_pool),
        )
    ]
    feature_importance_rows.sort(key=lambda r: r["importance"], reverse=True)

    training_manifest = {
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "target_col": TARGET_COL,
        "numeric_features": list(NUMERIC_FEATURES),
        "categorical_features": list(CATEGORICAL_FEATURES),
        "feature_cols": feature_cols,
        "dataset_fingerprint": dataset_fingerprint,
        "training_request_fingerprint": training_request_fingerprint,
        "source_key_count": int(source_key_count),
        "event_ids": sorted(int(x) for x in event_ids) if event_ids else None,
        "require_weather_available": REQUIRE_WEATHER_AVAILABLE,
        "min_holes_played": MIN_HOLES_PLAYED,
        "test_size": TEST_SIZE,
        "valid_size_within_train": VALID_SIZE_WITHIN_TRAIN,
        "random_state": RANDOM_STATE,
        "early_stopping_rounds": EARLY_STOPPING_ROUNDS,
        "catboost_params": CATBOOST_PARAMS,
        "wind_speed_reference_mph": REFERENCE_WIND_SPEED_MPH,
        "wind_gust_reference_mph": REFERENCE_WIND_GUST_MPH,
        "temperature_reference_c": REFERENCE_TEMPERATURE_C,
        "precip_reference_flag": REFERENCE_PRECIP_FLAG,
        **metrics,
    }

    return TrainingResult(
        model=model,
        metrics=metrics,
        training_manifest=training_manifest,
        feature_importance_rows=feature_importance_rows,
    )


