from __future__ import annotations

from typing import Optional

import boto3


def get_existing_content_sha256(*, table_name: str, event_id: int, aws_region: Optional[str] = None) -> Optional[str]:
    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    pk = f"EVENT#{int(event_id)}"
    sk = "METADATA"

    resp = table.get_item(Key={"pk": pk, "sk": sk}, ConsistentRead=False)
    item = resp.get("Item")
    if not item:
        return None
    return item.get("idempotency_sha256")