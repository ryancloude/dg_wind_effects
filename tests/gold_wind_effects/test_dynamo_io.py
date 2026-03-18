import gold_wind_effects.dynamo_io as dio


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


def test_load_gold_event_candidates(monkeypatch):
    table = FakeTable()
    table.query_pages = [
        {
            "Items": [
                {
                    "event_id": 90008,
                    "status": "success",
                    "round_s3_key": "silver/pdga/live_results_enriched/player_rounds_weather/event_year=2026/tourn_id=90008/player_rounds_weather.parquet",
                    "hole_s3_key": "silver/pdga/live_results_enriched/player_holes_weather/event_year=2026/tourn_id=90008/player_holes_weather.parquet",
                    "event_year": 2026,
                }
            ]
        }
    ]
    table.get_items[("EVENT#90008", "METADATA")] = {"start_date": "2026-03-10"}

    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))
    out = dio.load_gold_event_candidates(table_name="tbl", aws_region="us-east-1")
    assert len(out) == 1
    assert out[0].event_id == 90008
    assert out[0].event_year == 2026


def test_put_gold_event_checkpoint(monkeypatch):
    table = FakeTable()
    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))
    monkeypatch.setattr(dio, "utc_now_iso", lambda: "2026-03-18T12:00:00Z")

    item = dio.put_gold_event_checkpoint(
        table_name="tbl",
        event_id=90008,
        run_id="run-1",
        status="success",
        event_source_fingerprint="fp",
        aws_region="us-east-1",
        extra_attributes={"round_rows": 100},
    )
    assert item["pk"] == "PIPELINE#GOLD_WIND_EFFECTS"
    assert item["round_rows"] == 100
    assert len(table.put_calls) == 1