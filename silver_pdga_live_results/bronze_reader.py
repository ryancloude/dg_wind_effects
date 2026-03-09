from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import boto3

from silver_pdga_live_results.candidate_reader import LiveResultsStatePointer


@dataclass(frozen=True)
class BronzeLiveResultsPayload:
    pointer: LiveResultsStatePointer
    payload: dict[str, Any] | list[Any]


def _decode_payload(raw_bytes: bytes, *, s3_key: str) -> dict[str, Any] | list[Any]:
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except Exception as exc:
        raise ValueError(f"invalid_json_payload_for_key:{s3_key}") from exc

    if not isinstance(payload, (dict, list)):
        raise ValueError(f"unexpected_json_root_type_for_key:{s3_key}:type={type(payload).__name__}")

    return payload


def get_live_results_payload_from_s3(
    *,
    bucket: str,
    s3_key: str,
    aws_region: str | None = None,
    s3_client=None,
) -> dict[str, Any] | list[Any]:
    s3 = s3_client or (boto3.client("s3", region_name=aws_region) if aws_region else boto3.client("s3"))
    resp = s3.get_object(Bucket=bucket, Key=s3_key)
    raw = resp["Body"].read()
    return _decode_payload(raw, s3_key=s3_key)


def load_payload_for_pointer(
    *,
    bucket: str,
    pointer: LiveResultsStatePointer,
    aws_region: str | None = None,
    s3_client=None,
) -> BronzeLiveResultsPayload:
    payload = get_live_results_payload_from_s3(
        bucket=bucket,
        s3_key=pointer.latest_s3_json_key,
        aws_region=aws_region,
        s3_client=s3_client,
    )
    return BronzeLiveResultsPayload(pointer=pointer, payload=payload)