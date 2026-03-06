# backfill_live_results_audit.py
from collections import defaultdict
import os
import boto3
from boto3.dynamodb.conditions import Attr

from dotenv import load_dotenv
load_dotenv()

EXCLUDED = {
    "Sanctioned",
    "Event report received; official ratings pending.",
    "Event complete; waiting for report.",
    "In progress.",
    "Errata pending.",
}

ddb = boto3.resource("dynamodb", region_name=os.getenv("AWS_REGION"))
table = ddb.Table(os.environ["PDGA_DDB_TABLE"])

by_status = defaultdict(lambda: {"total": 0, "no_division_rounds": 0, "with_division_rounds": 0, "candidate": 0})
candidate_event_ids = []

last_key = None
while True:
    kwargs = {
        "FilterExpression": Attr("sk").eq("METADATA"),
        "ProjectionExpression": "event_id, status_text, division_rounds, pk, sk",
    }
    if last_key:
        kwargs["ExclusiveStartKey"] = last_key

    resp = table.scan(**kwargs)
    for item in resp.get("Items", []):
        status = item.get("status_text", "UNKNOWN")
        division_rounds = item.get("division_rounds") or {}
        has_rounds = isinstance(division_rounds, dict) and len(division_rounds) > 0

        by_status[status]["total"] += 1
        if has_rounds:
            by_status[status]["with_division_rounds"] += 1
        else:
            by_status[status]["no_division_rounds"] += 1

        if status not in EXCLUDED and has_rounds:
            by_status[status]["candidate"] += 1
            candidate_event_ids.append(int(item["event_id"]))

    last_key = resp.get("LastEvaluatedKey")
    if not last_key:
        break

print("=== STATUS SUMMARY ===")
for status in sorted(by_status):
    row = by_status[status]
    print(
        f"{status}: total={row['total']} "
        f"with_division_rounds={row['with_division_rounds']} "
        f"no_division_rounds={row['no_division_rounds']} "
        f"candidate={row['candidate']}"
    )

print(f"\nTOTAL_CANDIDATE_EVENTS={len(candidate_event_ids)}")
print("CANDIDATE_EVENT_IDS_CSV=" + ",".join(str(x) for x in sorted(set(candidate_event_ids))))