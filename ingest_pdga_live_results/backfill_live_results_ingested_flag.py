from __future__ import annotations

import os
from collections import defaultdict
import boto3
from boto3.dynamodb.conditions import Attr, Key
from dotenv import load_dotenv

load_dotenv()


def main() -> int:
    table_name = os.environ["PDGA_DDB_TABLE"]
    region = os.getenv("AWS_REGION")

    ddb = boto3.resource("dynamodb", region_name=region) if region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    # 1) collect event_ids that already have any LIVE_RESULTS state item
    live_results_event_ids: set[int] = set()
    last_key = None
    while True:
        scan_kwargs = {
            "FilterExpression": Attr("sk").begins_with("LIVE_RESULTS#"),
            "ProjectionExpression": "pk, sk",
        }
        if last_key:
            scan_kwargs["ExclusiveStartKey"] = last_key
        resp = table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            pk = str(item.get("pk", ""))
            if pk.startswith("EVENT#"):
                try:
                    live_results_event_ids.add(int(pk.split("#", 1)[1]))
                except ValueError:
                    pass
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    # 2) set flag on METADATA for those event_ids
    updated = 0
    missing_metadata = 0
    for event_id in sorted(live_results_event_ids):
        resp = table.get_item(Key={"pk": f"EVENT#{event_id}", "sk": "METADATA"}, ConsistentRead=False)
        if "Item" not in resp:
            missing_metadata += 1
            continue

        table.update_item(
            Key={"pk": f"EVENT#{event_id}", "sk": "METADATA"},
            UpdateExpression="""
            SET
                live_results_ingested = :true_value,
                live_results_ingested_at = if_not_exists(live_results_ingested_at, :ts),
                live_results_ingested_run_id = if_not_exists(live_results_ingested_run_id, :run_id)
            """,
            ExpressionAttributeValues={
                ":true_value": True,
                ":ts": "manual-backfill",
                ":run_id": "manual-backfill",
            },
        )
        updated += 1

    print(
        {
            "live_results_event_ids_found": len(live_results_event_ids),
            "metadata_updated": updated,
            "missing_metadata": missing_metadata,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
