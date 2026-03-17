from __future__ import annotations

import hashlib
import json
from typing import Any

import boto3

from silver_weather_observations.models import BronzeWeatherRoundSource


def _canonical_json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256_obj(value: Any) -> str:
    return hashlib.sha256(_canonical_json_dumps(value).encode("utf-8")).hexdigest()


def _parse_state_sk(sk: str) -> tuple[int | None, str | None, str | None]:
    # Expected:
    # WEATHER_OBS#ROUND#<round_number>#PROV#<provider>#SRC#<source_id>
    text = str(sk or "")
    parts = text.split("#")

    # Minimal safe parsing to avoid brittle regex with source_id containing delimiters.
    # WEATHER_OBS, ROUND, <n>, PROV, <provider>, SRC, <source_id...>
    if len(parts) < 7:
        return None, None, None
    if parts[0] != "WEATHER_OBS" or parts[1] != "ROUND":
        return None, None, None
    if parts[3] != "PROV":
        return None, None, None

    try:
        round_number = int(parts[2])
    except ValueError:
        round_number = None

    provider = parts[4] if parts[4] else None

    source_id = None
    if "SRC" in parts:
        src_idx = parts.index("SRC")
        tail = parts[src_idx + 1 :]
        if tail:
            source_id = "#".join(tail)

    return round_number, provider, source_id


def _load_json_from_s3(*, s3_client, bucket: str, key: str) -> Any:
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body.decode("utf-8"))


def build_weather_round_sources(
    *,
    bucket: str,
    event_id: int,
    state_items: list[dict[str, Any]],
    s3_client=None,
) -> list[BronzeWeatherRoundSource]:
    s3 = s3_client or boto3.client("s3")
    out: list[BronzeWeatherRoundSource] = []

    for item in state_items:
        json_key = str(item.get("latest_s3_json_key", "")).strip()
        if not json_key:
            continue

        payload = _load_json_from_s3(s3_client=s3, bucket=bucket, key=json_key)

        meta_key_raw = str(item.get("latest_s3_meta_key", "")).strip()
        meta_key = meta_key_raw if meta_key_raw else None
        meta: dict[str, Any] = {}
        if meta_key:
            try:
                loaded_meta = _load_json_from_s3(s3_client=s3, bucket=bucket, key=meta_key)
                if isinstance(loaded_meta, dict):
                    meta = loaded_meta
            except Exception:
                # Missing/corrupt sidecar should not crash event processing.
                meta = {}

        sk_round, sk_provider, sk_source_id = _parse_state_sk(str(item.get("sk", "")))

        round_number_raw = item.get("round_number", meta.get("round_number", sk_round))
        try:
            round_number = int(round_number_raw)
        except (TypeError, ValueError):
            continue

        provider = str(item.get("provider", meta.get("provider", sk_provider or "open_meteo_archive"))).strip()
        if not provider:
            provider = "open_meteo_archive"

        source_id = str(item.get("source_id", meta.get("source_id", sk_source_id or ""))).strip()
        if not source_id:
            # We keep this strict so downstream PKs remain deterministic.
            continue

        source_content_sha256 = str(
            item.get("content_sha256", meta.get("content_sha256", _sha256_obj(payload)))
        ).strip()

        source_fetched_at_utc = str(
            item.get("last_fetched_at", meta.get("fetched_at", ""))
        ).strip()

        request_fingerprint = str(
            item.get("request_fingerprint", meta.get("request_fingerprint", ""))
        ).strip()

        tee_time_source_fingerprint = str(
            item.get("tee_time_source_fingerprint", meta.get("tee_time_source_fingerprint", ""))
        ).strip()

        out.append(
            BronzeWeatherRoundSource(
                event_id=int(event_id),
                round_number=round_number,
                provider=provider,
                source_id=source_id,
                source_json_key=json_key,
                source_meta_key=meta_key,
                source_content_sha256=source_content_sha256,
                source_fetched_at_utc=source_fetched_at_utc,
                request_fingerprint=request_fingerprint,
                tee_time_source_fingerprint=tee_time_source_fingerprint,
                payload=payload if isinstance(payload, dict) else {"raw_payload": payload},
            )
        )

    out.sort(key=lambda s: (s.round_number, s.provider, s.source_id, s.source_fetched_at_utc, s.source_json_key))
    return out


def compute_event_source_fingerprint(round_sources: list[BronzeWeatherRoundSource]) -> str:
    records = [
        {
            "event_id": int(src.event_id),
            "round_number": int(src.round_number),
            "provider": src.provider,
            "source_id": src.source_id,
            "source_json_key": src.source_json_key,
            "source_content_sha256": src.source_content_sha256,
            "source_fetched_at_utc": src.source_fetched_at_utc,
            "request_fingerprint": src.request_fingerprint,
            "tee_time_source_fingerprint": src.tee_time_source_fingerprint,
        }
        for src in round_sources
    ]
    records.sort(
        key=lambda r: (
            r["round_number"],
            r["provider"],
            r["source_id"],
            r["source_fetched_at_utc"],
            r["source_json_key"],
        )
    )
    return _sha256_obj(records)