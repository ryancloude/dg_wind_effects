from __future__ import annotations

import json
import os
import tempfile
from io import BytesIO
from typing import Any

import boto3
from catboost import CatBoostRegressor

from train_round_wind_model.artifact_io import build_artifact_prefix


def _s3_client(s3_client=None):
    return s3_client or boto3.client("s3")


def _load_json(*, bucket: str, key: str, s3_client=None) -> dict[str, Any]:
    s3 = _s3_client(s3_client)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body.decode("utf-8"))


def load_model_bundle(
    *,
    bucket: str,
    training_request_fingerprint: str,
    s3_client=None,
) -> dict[str, Any]:
    prefix = build_artifact_prefix(training_request_fingerprint=training_request_fingerprint)
    s3 = _s3_client(s3_client)

    model_key = f"{prefix}model.cbm"
    manifest_key = f"{prefix}training_manifest.json"
    feature_columns_key = f"{prefix}feature_columns.json"
    categorical_feature_columns_key = f"{prefix}categorical_feature_columns.json"

    model_bytes = s3.get_object(Bucket=bucket, Key=model_key)["Body"].read()
    manifest = _load_json(bucket=bucket, key=manifest_key, s3_client=s3_client)
    feature_columns = _load_json(bucket=bucket, key=feature_columns_key, s3_client=s3_client)["feature_columns"]
    categorical_feature_columns = _load_json(
        bucket=bucket,
        key=categorical_feature_columns_key,
        s3_client=s3_client,
    )["categorical_feature_columns"]

    with tempfile.NamedTemporaryFile(suffix=".cbm", delete=False) as tmp:
        tmp.write(model_bytes)
        model_path = tmp.name

    try:
        model = CatBoostRegressor()
        model.load_model(model_path)
    finally:
        try:
            os.unlink(model_path)
        except OSError:
            pass

    return {
        "artifact_prefix": prefix,
        "model_key": model_key,
        "model": model,
        "training_manifest": manifest,
        "feature_columns": feature_columns,
        "categorical_feature_columns": categorical_feature_columns,
    }