from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Iterator, Optional

import boto3
from boto3.dynamodb.conditions import Attr


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


def _coerce_positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, Decimal):
        try:
            if value != value.to_integral_value():
                return None
            parsed = int(value)
            return parsed if parsed > 0 else None
        except (InvalidOperation, ValueError):
            return None
    return None


def expand_tasks_from_metadata_item(metadata_item: dict[str, Any]) -> list[LiveResultsTask]:
    event_id_raw = metadata_item.get("event_id", "")
    event_id = str(event_id_raw).strip()
    if not event_id:
        return []

    division_rounds = metadata_item.get("division_rounds")
    if not isinstance(division_rounds, dict):
        return []

    tasks: list[LiveResultsTask] = []
    for division, max_round_raw in division_rounds.items():
        division_code = str(division).strip()
        if not division_code:
            continue

        max_round = _coerce_positive_int(max_round_raw)
        if max_round is None:
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
    tasks: list[LiveResultsTask] = []
    for item in metadata_items:
        tasks.extend(expand_tasks_from_metadata_item(item))
    return tasks


def iter_metadata_items(
    *,
    table_name: str,
    event_ids: Optional[list[int]] = None,
    aws_region: Optional[str] = None,
) -> Iterator[dict[str, Any]]:
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    if event_ids:
        for event_id in event_ids:
            resp = table.get_item(Key={"pk": f"EVENT#{int(event_id)}", "sk": "METADATA"}, ConsistentRead=False)
            item = resp.get("Item")
            if item:
                yield item
        return

    last_evaluated_key = None
    while True:
        scan_kwargs: dict[str, Any] = {
            "FilterExpression": Attr("sk").eq("METADATA"),
            "ProjectionExpression": "event_id, division_rounds, pk, sk",
        }
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        resp = table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            yield item

        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break


def load_live_results_tasks(
    *,
    table_name: str,
    event_ids: Optional[list[int]] = None,
    aws_region: Optional[str] = None,
) -> list[LiveResultsTask]:
    metadata_items = list(iter_metadata_items(table_name=table_name, event_ids=event_ids, aws_region=aws_region))
    return expand_tasks_from_metadata_items(metadata_items)