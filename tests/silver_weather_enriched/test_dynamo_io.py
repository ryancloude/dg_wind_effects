import silver_weather_enriched.dynamo_io as dio


class FakeTable:
    def __init__(self):
        self.get_items = {}
        self.query_pages = []
        self.put_calls = []

    def get_item(self, **kwargs):
        key = kwargs["Key"]
        item = self.get_items.get((key["pk"], key["sk"]))
        return {"Item": item} if item is not None else {}

    def query(self, **kwargs):
        if self.query_pages:
            return self.query_pages.pop(0)
        return {"Items": []}

    def put_item(self, **kwargs):
        self.put_calls.append(kwargs["Item"])


class FakeDdb:
    def __init__(self, table):
        self._table = table

    def Table(self, name):
        return self._table


def test_load_enriched_event_candidates_intersection(monkeypatch):
    table = FakeTable()
    table.query_pages = [
        {"Items": [{"event_id": 90008, "status": "success", "round_s3_key": "r", "hole_s3_key": "h"}]},  # live
        {"Items": [{"event_id": 90008, "status": "success", "observations_s3_key": "w"}]},  # weather
    ]
    table.get_items[("EVENT#90008", "METADATA")] = {"start_date": "2026-03-10"}

    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))
    out = dio.load_enriched_event_candidates(table_name="tbl", aws_region="us-east-1")
    assert len(out) == 1
    assert out[0].event_id == 90008
    assert out[0].event_year == 2026


def test_put_enriched_event_checkpoint(monkeypatch):
    table = FakeTable()
    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))
    monkeypatch.setattr(dio, "utc_now_iso", lambda: "2026-03-17T12:00:00Z")

    item = dio.put_enriched_event_checkpoint(
        table_name="tbl",
        event_id=90008,
        run_id="run-1",
        status="success",
        event_source_fingerprint="fp",
        aws_region="us-east-1",
        extra_attributes={"round_rows": 100},
    )

    assert item["pk"] == "PIPELINE#SILVER_WEATHER_ENRICHED"
    assert item["round_rows"] == 100
    assert len(table.put_calls) == 1