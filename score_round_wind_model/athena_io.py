from __future__ import annotations

import time
from typing import Any

import boto3


def _athena_client(aws_region: str | None):
    return boto3.client("athena", region_name=aws_region) if aws_region else boto3.client("athena")


def build_add_partition_sql(
    *,
    database: str,
    table_name: str,
    event_year: int,
    event_id: int,
    partition_location: str,
) -> str:
    return f"""
ALTER TABLE {database}.{table_name}
ADD IF NOT EXISTS
PARTITION (
  event_year = {int(event_year)},
  tourn_id = {int(event_id)}
)
LOCATION '{partition_location}'
""".strip()


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


def register_scored_round_partition(
    *,
    database: str,
    table_name: str,
    workgroup: str,
    output_location: str,
    aws_region: str | None,
    event_year: int,
    event_id: int,
    partition_location: str,
) -> dict[str, Any]:
    sql = build_add_partition_sql(
        database=database,
        table_name=table_name,
        event_year=event_year,
        event_id=event_id,
        partition_location=partition_location,
    )
    result = execute_athena_query(
        sql=sql,
        database=database,
        workgroup=workgroup,
        output_location=output_location,
        aws_region=aws_region,
    )
    result.update(
        {
            "event_year": int(event_year),
            "event_id": int(event_id),
            "table_name": table_name,
            "partition_location": partition_location,
        }
    )
    return result
