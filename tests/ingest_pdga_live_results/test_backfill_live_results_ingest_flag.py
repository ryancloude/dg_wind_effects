from types import SimpleNamespace

import ingest_pdga_live_results.backfill_live_results_ingested_flag as backfill_script


class FakeTable:
    def __init__(self):
        self.scan_calls = 0
        self.update_calls = []
        self.metadata_items = {
            1001: True,   # exists
            1002: False,  # missing METADATA
        }

    def scan(self, **kwargs):
        self.scan_calls += 1
        if self.scan_calls == 1:
            return {
                "Items": [
                    {"pk": "EVENT#1001", "sk": "LIVE_RESULTS#DIV#MPO#ROUND#1"},
                    {"pk": "EVENT#1002", "sk": "LIVE_RESULTS#DIV#FPO#ROUND#1"},
                    {"pk": "RUN#abc", "sk": "LIVE_RESULTS#SUMMARY"},
                ],
                "LastEvaluatedKey": {"pk": "next"},
            }
        return {"Items": []}

    def get_item(self, **kwargs):
        key = kwargs["Key"]
        event_id = int(key["pk"].split("#", 1)[1])
        if self.metadata_items.get(event_id):
            return {"Item": {"pk": key["pk"], "sk": "METADATA"}}
        return {}

    def update_item(self, **kwargs):
        self.update_calls.append(kwargs)
        return {"Attributes": {"ok": True}}


def test_main_backfills_ingested_flag(monkeypatch):
    table = FakeTable()
    resource = SimpleNamespace(Table=lambda _name: table)

    monkeypatch.setenv("PDGA_DDB_TABLE", "pdga-event-index")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setattr(backfill_script.boto3, "resource", lambda *args, **kwargs: resource)

    printed = []
    monkeypatch.setattr(backfill_script, "print", lambda obj: printed.append(obj), raising=False)

    exit_code = backfill_script.main()

    assert exit_code == 0
    assert len(table.update_calls) == 1  # only EVENT#1001 had METADATA
    assert printed[0]["live_results_event_ids_found"] == 2
    assert printed[0]["metadata_updated"] == 1
    assert printed[0]["missing_metadata"] == 1