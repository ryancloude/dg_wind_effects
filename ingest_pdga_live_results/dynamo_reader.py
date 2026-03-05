from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class LiveResultsTask:
    event_id: str
    division: str
    round_number: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "division": self.division,
            "round_number": self.round_number,
        }


def expand_tasks_from_metadata_item(metadata_item: dict[str, Any]) -> list[LiveResultsTask]:
    """
    Expand a single METADATA DynamoDB item into live-results fetch tasks.

    Required fields:
      - event_id: str
      - division_rounds: dict[str, int] where value is max round for that division

    Example:
      {"event_id": "86076", "division_rounds": {"MP40": 3, "MA1": 2}}
    -> tasks:
      (86076, MP40, 1..3), (86076, MA1, 1..2)
    """
    event_id = str(metadata_item.get("event_id", "")).strip()
    if not event_id:
        return []

    division_rounds = metadata_item.get("division_rounds")
    if not isinstance(division_rounds, dict):
        return []

    tasks: list[LiveResultsTask] = []
    for division, max_round in division_rounds.items():
        division_code = str(division).strip()
        if not division_code:
            continue

        if not isinstance(max_round, int):
            continue
        if max_round < 1:
            continue

        for round_number in range(1, max_round + 1):
            tasks.append(
                LiveResultsTask(
                    event_id=event_id,
                    division=division_code,
                    round_number=round_number,
                )
            )

    return tasks


def expand_tasks_from_metadata_items(metadata_items: Iterable[dict[str, Any]]) -> list[LiveResultsTask]:
    """
    Expand many METADATA items into one flat task list.
    """
    all_tasks: list[LiveResultsTask] = []
    for item in metadata_items:
        all_tasks.extend(expand_tasks_from_metadata_item(item))
    return all_tasks


def list_metadata_items(
    dynamodb_client: Any,
    table_name: str,
    metadata_sk_value: str = "METADATA",
) -> list[dict[str, Any]]:
    """
    Read METADATA items from pdga-event-index.
    This function assumes the table has an 'SK' attribute and METADATA rows use SK='METADATA'.

    If your key attribute names differ (e.g., sk), adapt this filter to match your existing table.
    """
    scan_kwargs: dict[str, Any] = {
        "TableName": table_name,
        "FilterExpression": "SK = :metadata_sk",
        "ExpressionAttributeValues": {
            ":metadata_sk": {"S": metadata_sk_value},
        },
    }

    items: list[dict[str, Any]] = []
    while True:
        response = dynamodb_client.scan(**scan_kwargs)
        items.extend(response.get("Items", []))
        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

    return items


def deserialize_metadata_items(
    dynamodb_deserializer: Any,
    raw_items: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Convert low-level DynamoDB AttributeValue maps into plain Python dicts.

    Expects dynamodb_deserializer to expose:
      deserialize(attribute_value) -> python_value
    Example: boto3.dynamodb.types.TypeDeserializer()
    """
    output: list[dict[str, Any]] = []
    for item in raw_items:
        parsed: dict[str, Any] = {}
        for key, value in item.items():
            parsed[key] = dynamodb_deserializer.deserialize(value)
        output.append(parsed)
    return output


def load_live_results_tasks(
    dynamodb_client: Any,
    dynamodb_deserializer: Any,
    table_name: str,
) -> list[LiveResultsTask]:
    """
    End-to-end reader entrypoint:
      1) read METADATA items from DynamoDB
      2) deserialize items
      3) expand into (event_id, division, round) tasks
    """
    raw_items = list_metadata_items(dynamodb_client=dynamodb_client, table_name=table_name)
    metadata_items = deserialize_metadata_items(dynamodb_deserializer=dynamodb_deserializer, raw_items=raw_items)
    return expand_tasks_from_metadata_items(metadata_items)