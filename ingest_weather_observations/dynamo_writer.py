from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

import boto3


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ddb_resource(aws_region: Optional[str]):
    return boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")


def _state_sk(*, round_number: int, provider: str, source_id: str) -> str:
    return f"WEATHER_OBS#ROUND#{int(round_number)}#PROV#{provider}#SRC#{source_id}"


def _to_ddb_decimal(value: float | int | str) -> Decimal:
    return Decimal(str(value))


def _to_ddb_compatible(value: Any) -> Any:
    """
    Recursively convert Python values into DynamoDB-compatible values.

    In particular, boto3's DynamoDB serializer rejects native Python floats,
    so we convert them to Decimal via string conversion.
    """
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [_to_ddb_compatible(x) for x in value]
    if isinstance(value, tuple):
        return [_to_ddb_compatible(x) for x in value]
    if isinstance(value, dict):
        return {k: _to_ddb_compatible(v) for k, v in value.items()}
    return value


def get_existing_weather_state(
    *,
    table_name: str,
    event_id: int,
    round_number: int,
    provider: str,
    source_id: str,
    aws_region: Optional[str] = None,
) -> dict[str, Any] | None:
    table = _ddb_resource(aws_region).Table(table_name)
    resp = table.get_item(
        Key={"pk": f"EVENT#{int(event_id)}", "sk": _state_sk(round_number=round_number, provider=provider, source_id=source_id)},
        ConsistentRead=False,
    )
    return resp.get("Item")


def upsert_weather_state(
    *,
    table_name: str,
    event_id: int,
    round_number: int,
    provider: str,
    source_id: str,
    source_url: str,
    request_fingerprint: str,
    tee_time_source_fingerprint: str,
    fetch_status: str,
    content_sha256: str,
    s3_ptrs: Dict[str, Any],
    run_id: str,
    aws_region: Optional[str] = None,
) -> dict[str, Any]:
    table = _ddb_resource(aws_region).Table(table_name)
    now = utc_now_iso()

    resp = table.update_item(
        Key={"pk": f"EVENT#{int(event_id)}", "sk": _state_sk(round_number=round_number, provider=provider, source_id=source_id)},
        UpdateExpression="""
        SET
            event_id = :event_id,
            round_number = :round_number,
            provider = :provider,
            source_id = :source_id,
            source_url = :source_url,
            request_fingerprint = :request_fingerprint,
            tee_time_source_fingerprint = :tee_time_source_fingerprint,
            fetch_status = :fetch_status,
            content_sha256 = :content_sha256,
            latest_s3_json_key = :latest_s3_json_key,
            latest_s3_meta_key = :latest_s3_meta_key,
            last_fetched_at = :last_fetched_at,
            last_run_id = :last_run_id,
            first_seen_at = if_not_exists(first_seen_at, :first_seen_at)
        """,
        ExpressionAttributeValues={
            ":event_id": int(event_id),
            ":round_number": int(round_number),
            ":provider": provider,
            ":source_id": source_id,
            ":source_url": source_url,
            ":request_fingerprint": request_fingerprint,
            ":tee_time_source_fingerprint": tee_time_source_fingerprint,
            ":fetch_status": fetch_status,
            ":content_sha256": content_sha256,
            ":latest_s3_json_key": s3_ptrs.get("s3_json_key", ""),
            ":latest_s3_meta_key": s3_ptrs.get("s3_meta_key", ""),
            ":last_fetched_at": s3_ptrs.get("fetched_at", now),
            ":last_run_id": run_id,
            ":first_seen_at": now,
        },
        ReturnValues="ALL_NEW",
    )
    return resp.get("Attributes", {})


def put_cached_geocode(
    *,
    table_name: str,
    query_fingerprint: str,
    query_text: str,
    latitude: float,
    longitude: float,
    source_name: str,
    source_admin1: str,
    source_country: str,
    source_country_code: str,
    run_id: str,
    aws_region: Optional[str] = None,
) -> dict[str, Any]:
    table = _ddb_resource(aws_region).Table(table_name)

    item = {
        "pk": f"GEO#QUERY#{query_fingerprint}",
        "sk": "WEATHER_GEO#CACHE",
        "query_fingerprint": query_fingerprint,
        "query_text": query_text,
        "latitude": _to_ddb_decimal(latitude),
        "longitude": _to_ddb_decimal(longitude),
        "source_name": source_name,
        "source_admin1": source_admin1,
        "source_country": source_country,
        "source_country_code": source_country_code,
        "updated_at": utc_now_iso(),
        "last_run_id": run_id,
    }
    table.put_item(Item=item)
    return item


def upsert_event_geocode_resolution(
    *,
    table_name: str,
    event_id: int,
    query_fingerprint: str,
    query_text: str,
    latitude: float,
    longitude: float,
    resolution_source: str,
    run_id: str,
    aws_region: Optional[str] = None,
) -> dict[str, Any]:
    table = _ddb_resource(aws_region).Table(table_name)
    now = utc_now_iso()

    resp = table.update_item(
        Key={"pk": f"EVENT#{int(event_id)}", "sk": "WEATHER_GEO#RESOLVED"},
        UpdateExpression="""
        SET
            event_id = :event_id,
            query_fingerprint = :query_fingerprint,
            query_text = :query_text,
            latitude = :latitude,
            longitude = :longitude,
            resolution_source = :resolution_source,
            last_run_id = :last_run_id,
            updated_at = :updated_at,
            first_seen_at = if_not_exists(first_seen_at, :first_seen_at)
        """,
        ExpressionAttributeValues={
            ":event_id": int(event_id),
            ":query_fingerprint": query_fingerprint,
            ":query_text": query_text,
            ":latitude": _to_ddb_decimal(latitude),
            ":longitude": _to_ddb_decimal(longitude),
            ":resolution_source": resolution_source,
            ":last_run_id": run_id,
            ":updated_at": now,
            ":first_seen_at": now,
        },
        ReturnValues="ALL_NEW",
    )
    return resp.get("Attributes", {})


def upsert_event_weather_summary(
    *,
    table_name: str,
    event_id: int,
    run_id: str,
    silver_checkpoint_updated_at: str,
    status: str,
    stats: dict[str, int],
    aws_region: Optional[str] = None,
    error_type: str = "",
    error_message: str = "",
) -> dict[str, Any]:
    table = _ddb_resource(aws_region).Table(table_name)
    item = {
        "pk": f"EVENT#{int(event_id)}",
        "sk": "WEATHER_OBS#SUMMARY",
        "event_id": int(event_id),
        "pipeline": "ingest_weather_observations",
        "last_run_id": run_id,
        "updated_at": utc_now_iso(),
        "last_silver_checkpoint_updated_at": silver_checkpoint_updated_at,
        "status": status,
        "error_type": error_type,
        "error_message": error_message,
        **stats,
    }
    table.put_item(Item=_to_ddb_compatible(item))
    return item


def put_weather_run_summary(
    *,
    table_name: str,
    run_id: str,
    stats: dict[str, Any],
    aws_region: Optional[str] = None,
) -> dict[str, Any]:
    table = _ddb_resource(aws_region).Table(table_name)
    item = {
        "pk": f"RUN#{run_id}",
        "sk": "WEATHER_OBS#SUMMARY",
        "run_id": run_id,
        "created_at": utc_now_iso(),
        **stats,
    }
    table.put_item(Item=_to_ddb_compatible(item))
    return item
