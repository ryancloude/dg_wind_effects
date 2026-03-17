import silver_weather_observations.dynamo_io as dio


class FakeTable:
    def __init__(self):
        self.get_items = {}
        self.query_pages = []
        self.scan_pages = []
        self.put_calls = []

    def get_item(self, **kwargs):
        key = kwargs["Key"]
        item = self.get_items.get((key["pk"], key["sk"]))
        return {"Item": item} if item is not None else {}

    def query(self, **kwargs):
        if self.query_pages:
            return self.query_pages.pop(0)
        return {"Items": []}

    def scan(self, **kwargs):
        if self.scan_pages:
            return self.scan_pages.pop(0)
        return {"Items": []}

    def put_item(self, **kwargs):
        self.put_calls.append(kwargs["Item"])


class FakeDdb:
    def __init__(self, table):
        self._table = table

    def Table(self, name):
        return self._table


def test_get_silver_weather_event_checkpoint(monkeypatch):
    table = FakeTable()
    table.get_items[("PIPELINE#SILVER_WEATHER_OBSERVATIONS", "EVENT#90008")] = {
        "pk": "PIPELINE#SILVER_WEATHER_OBSERVATIONS",
        "sk": "EVENT#90008",
        "event_id": 90008,
        "status": "success",
    }
    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))

    item = dio.get_silver_weather_event_checkpoint(
        table_name="tbl",
        event_id=90008,
        aws_region="us-east-1",
    )
    assert item is not None
    assert item["event_id"] == 90008


def test_get_event_metadata(monkeypatch):
    table = FakeTable()
    table.get_items[("EVENT#90008", "METADATA")] = {"event_id": 90008, "name": "Test"}
    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))

    item = dio.get_event_metadata(table_name="tbl", event_id=90008, aws_region="us-east-1")
    assert item is not None
    assert item["name"] == "Test"


def test_load_weather_event_summaries_scan(monkeypatch):
    table = FakeTable()
    table.scan_pages = [
        {
            "Items": [{"pk": "EVENT#1", "sk": "WEATHER_OBS#SUMMARY", "event_id": 1}],
            "LastEvaluatedKey": {"pk": "x"},
        },
        {
            "Items": [{"pk": "EVENT#2", "sk": "WEATHER_OBS#SUMMARY", "event_id": 2}],
        },
    ]
    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))

    items = dio.load_weather_event_summaries(table_name="tbl", aws_region="us-east-1")
    assert [i["event_id"] for i in items] == [1, 2]


def test_load_weather_state_items_paginates(monkeypatch):
    table = FakeTable()
    table.query_pages = [
        {"Items": [{"pk": "EVENT#1", "sk": "WEATHER_OBS#ROUND#1"}], "LastEvaluatedKey": {"k": "1"}},
        {"Items": [{"pk": "EVENT#1", "sk": "WEATHER_OBS#ROUND#2"}]},
    ]
    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))

    items = dio.load_weather_state_items(table_name="tbl", event_id=1, aws_region="us-east-1")
    assert len(items) == 2


def test_put_silver_weather_event_checkpoint_writes_item(monkeypatch):
    table = FakeTable()
    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))
    monkeypatch.setattr(dio, "utc_now_iso", lambda: "2026-03-16T20:00:00Z")

    item = dio.put_silver_weather_event_checkpoint(
        table_name="tbl",
        event_id=90008,
        run_id="silver-weather-1",
        status="success",
        event_source_fingerprint="fp-abc",
        aws_region="us-east-1",
        extra_attributes={"row_count": 42},
    )

    assert item["pk"] == "PIPELINE#SILVER_WEATHER_OBSERVATIONS"
    assert item["event_id"] == 90008
    assert item["row_count"] == 42
    assert len(table.put_calls) == 1


def test_put_silver_weather_run_summary_writes_item(monkeypatch):
    table = FakeTable()
    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))
    monkeypatch.setattr(dio, "utc_now_iso", lambda: "2026-03-16T20:00:00Z")

    item = dio.put_silver_weather_run_summary(
        table_name="tbl",
        run_id="silver-weather-1",
        stats={"attempted_events": 10, "processed_events": 9},
        aws_region="us-east-1",
    )

    assert item["sk"] == "SILVER_WEATHER_OBSERVATIONS#SUMMARY"
    assert item["processed_events"] == 9
    assert len(table.put_calls) == 1