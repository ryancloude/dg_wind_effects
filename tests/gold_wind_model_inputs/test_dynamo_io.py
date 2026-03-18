import gold_wind_model_inputs.dynamo_io as dio


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


def test_load_model_inputs_event_candidates_from_gold_success(monkeypatch):
    table = FakeTable()
    table.query_pages = [
        {
            "Items": [
                {
                    "event_id": 90008,
                    "status": "success",
                    "event_year": 2026,
                    "hole_s3_key": "gold/pdga/wind_effects/player_holes_features/event_year=2026/tourn_id=90008/player_holes_features.parquet",
                    "round_s3_key": "gold/pdga/wind_effects/player_rounds_features/event_year=2026/tourn_id=90008/player_rounds_features.parquet",
                }
            ]
        }
    ]
    table.get_items[("EVENT#90008", "METADATA")] = {"start_date": "2026-03-10"}

    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))
    out = dio.load_model_inputs_event_candidates(table_name="tbl", aws_region="us-east-1")

    assert len(out) == 1
    assert out[0].event_id == 90008
    assert out[0].event_year == 2026
    assert out[0].hole_s3_key.endswith("player_holes_features.parquet")


def test_load_model_inputs_event_candidates_filters_event_ids(monkeypatch):
    table = FakeTable()
    table.query_pages = [
        {
            "Items": [
                {
                    "event_id": 90008,
                    "status": "success",
                    "event_year": 2026,
                    "hole_s3_key": "k1",
                },
                {
                    "event_id": 90009,
                    "status": "success",
                    "event_year": 2026,
                    "hole_s3_key": "k2",
                },
            ]
        }
    ]

    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))
    out = dio.load_model_inputs_event_candidates(
        table_name="tbl",
        aws_region="us-east-1",
        event_ids=[90009],
    )
    assert [c.event_id for c in out] == [90009]


def test_put_model_inputs_event_checkpoint(monkeypatch):
    table = FakeTable()

    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))
    monkeypatch.setattr(dio, "utc_now_iso", lambda: "2026-03-18T12:00:00Z")

    item = dio.put_model_inputs_event_checkpoint(
        table_name="tbl",
        event_id=90008,
        run_id="run-1",
        status="success",
        event_source_fingerprint="fp-1",
        aws_region="us-east-1",
        extra_attributes={"hole_rows": 1234},
    )

    assert item["pk"] == "PIPELINE#GOLD_WIND_MODEL_INPUTS"
    assert item["sk"] == "EVENT#90008"
    assert item["status"] == "success"
    assert item["hole_rows"] == 1234
    assert len(table.put_calls) == 1


def test_put_model_inputs_run_summary(monkeypatch):
    table = FakeTable()

    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))
    monkeypatch.setattr(dio, "utc_now_iso", lambda: "2026-03-18T12:00:00Z")

    item = dio.put_model_inputs_run_summary(
        table_name="tbl",
        run_id="run-1",
        stats={"attempted_events": 5, "processed_events": 4},
        aws_region="us-east-1",
    )

    assert item["pk"] == "RUN#run-1"
    assert item["sk"] == "GOLD_WIND_MODEL_INPUTS#SUMMARY"
    assert item["attempted_events"] == 5
    assert len(table.put_calls) == 1