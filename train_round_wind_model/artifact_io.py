from __future__ import annotations

import csv
import json
import os
import tempfile
from typing import Any

import boto3

from train_round_wind_model.models import ARTIFACT_BASE_PREFIX, MODEL_NAME, MODEL_VERSION


def _s3_client(s3_client=None):
    return s3_client or boto3.client("s3")


def _put_json(*, bucket: str, key: str, payload: dict[str, Any], s3_client=None) -> None:
    s3 = _s3_client(s3_client)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )


def _put_csv_rows(
    *,
    bucket: str,
    key: str,
    rows: list[dict[str, Any]],
    fieldnames: list[str],
    s3_client=None,
) -> None:
    s3 = _s3_client(s3_client)

    with tempfile.NamedTemporaryFile("w+", newline="", encoding="utf-8", delete=False) as tmp:
        try:
            writer = csv.DictWriter(tmp, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
            tmp.flush()
            tmp.seek(0)
            with open(tmp.name, "rb") as fh:
                s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=fh.read(),
                    ContentType="text/csv",
                )
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass


def build_artifact_prefix(*, training_request_fingerprint: str) -> str:
    return (
        f"{ARTIFACT_BASE_PREFIX}"
        f"model_name={MODEL_NAME}/"
        f"model_version={MODEL_VERSION}/"
        f"training_fingerprint={training_request_fingerprint}/"
    )


def write_training_artifacts(
    *,
    bucket: str,
    training_request_fingerprint: str,
    model,
    training_manifest: dict[str, Any],
    metrics: dict[str, Any],
    feature_importance_rows: list[dict[str, Any]],
    s3_client=None,
) -> dict[str, str]:
    s3 = _s3_client(s3_client)
    prefix = build_artifact_prefix(training_request_fingerprint=training_request_fingerprint)

    with tempfile.NamedTemporaryFile(suffix=".cbm", delete=False) as tmp:
        model_path = tmp.name

    try:
        model.save_model(model_path)
        with open(model_path, "rb") as fh:
            model_key = f"{prefix}model.cbm"
            s3.put_object(
                Bucket=bucket,
                Key=model_key,
                Body=fh.read(),
                ContentType="application/octet-stream",
            )
    finally:
        try:
            os.unlink(model_path)
        except OSError:
            pass

    manifest_key = f"{prefix}training_manifest.json"
    metrics_key = f"{prefix}metrics.json"
    feature_cols_key = f"{prefix}feature_columns.json"
    cat_cols_key = f"{prefix}categorical_feature_columns.json"
    feature_importance_key = f"{prefix}feature_importance.csv"

    _put_json(bucket=bucket, key=manifest_key, payload=training_manifest, s3_client=s3_client)
    _put_json(bucket=bucket, key=metrics_key, payload=metrics, s3_client=s3_client)
    _put_json(
        bucket=bucket,
        key=feature_cols_key,
        payload={"feature_columns": training_manifest["feature_cols"]},
        s3_client=s3_client,
    )
    _put_json(
        bucket=bucket,
        key=cat_cols_key,
        payload={"categorical_feature_columns": training_manifest["categorical_features"]},
        s3_client=s3_client,
    )
    _put_csv_rows(
        bucket=bucket,
        key=feature_importance_key,
        rows=feature_importance_rows,
        fieldnames=["feature", "importance"],
        s3_client=s3_client,
    )

    return {
        "artifact_prefix": prefix,
        "model_key": model_key,
        "manifest_key": manifest_key,
        "metrics_key": metrics_key,
        "feature_columns_key": feature_cols_key,
        "categorical_feature_columns_key": cat_cols_key,
        "feature_importance_key": feature_importance_key,
    }
