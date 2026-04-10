from __future__ import annotations

import time
from typing import Any
from urllib.parse import urlparse

import boto3


def _athena_client(aws_region: str | None):
    return boto3.client("athena", region_name=aws_region) if aws_region else boto3.client("athena")


def _s3_client(aws_region: str | None):
    return boto3.client("s3", region_name=aws_region) if aws_region else boto3.client("s3")


def split_s3_uri(s3_uri: str) -> tuple[str, str]:
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"invalid s3 uri: {s3_uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def delete_s3_prefix(*, s3_uri: str, aws_region: str | None) -> int:
    bucket, prefix = split_s3_uri(s3_uri)
    if not prefix:
        raise ValueError(f"refusing to delete entire bucket prefix for {s3_uri}")

    client = _s3_client(aws_region)
    paginator = client.get_paginator("list_objects_v2")
    deleted = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        if not contents:
            continue

        for start in range(0, len(contents), 1000):
            batch = contents[start : start + 1000]
            client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in batch], "Quiet": True},
            )
            deleted += len(batch)

    return deleted


def start_athena_query(
    *,
    sql: str,
    database: str,
    workgroup: str,
    output_location: str,
    aws_region: str | None,
) -> str:
    client = _athena_client(aws_region)
    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
        ResultConfiguration={"OutputLocation": output_location},
    )
    return resp["QueryExecutionId"]


def wait_for_query(
    *,
    query_execution_id: str,
    aws_region: str | None,
    poll_seconds: float = 2.0,
    timeout_seconds: float = 1800.0,
) -> dict[str, Any]:
    client = _athena_client(aws_region)
    started = time.perf_counter()

    while True:
        resp = client.get_query_execution(QueryExecutionId=query_execution_id)
        execution = resp["QueryExecution"]
        status = execution["Status"]["State"]

        if status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            if status != "SUCCEEDED":
                reason = execution["Status"].get("StateChangeReason", "unknown Athena failure")
                raise RuntimeError(f"Athena query {query_execution_id} {status.lower()}: {reason}")

            stats = execution.get("Statistics", {})
            result_cfg = execution.get("ResultConfiguration", {})
            return {
                "query_execution_id": query_execution_id,
                "state": status,
                "scanned_bytes": int(stats.get("DataScannedInBytes", 0) or 0),
                "engine_execution_time_ms": int(stats.get("EngineExecutionTimeInMillis", 0) or 0),
                "total_execution_time_ms": int(stats.get("TotalExecutionTimeInMillis", 0) or 0),
                "output_location": result_cfg.get("OutputLocation", ""),
            }

        if time.perf_counter() - started > timeout_seconds:
            raise TimeoutError(f"Athena query {query_execution_id} timed out after {timeout_seconds} seconds")

        time.sleep(poll_seconds)


def execute_athena_query(
    *,
    sql: str,
    database: str,
    workgroup: str,
    output_location: str,
    aws_region: str | None,
    poll_seconds: float = 2.0,
    timeout_seconds: float = 1800.0,
) -> dict[str, Any]:
    query_execution_id = start_athena_query(
        sql=sql,
        database=database,
        workgroup=workgroup,
        output_location=output_location,
        aws_region=aws_region,
    )
    result = wait_for_query(
        query_execution_id=query_execution_id,
        aws_region=aws_region,
        poll_seconds=poll_seconds,
        timeout_seconds=timeout_seconds,
    )
    result["sql"] = sql
    return result
