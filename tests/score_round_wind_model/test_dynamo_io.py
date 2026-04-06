from decimal import Decimal

import score_round_wind_model.dynamo_io as dio


class FakeTable:
    def __init__(self):
        self.items = {}

    def get_item(self, **kwargs):
        key = (kwargs["Key"]["pk"], kwargs["Key"]["sk"])
        if key in self.items:
            return {"Item": self.items[key]}
        return {}

    def put_item(self, Item):
        key = (Item["pk"], Item["sk"])
        self.items[key] = Item


class FakeDdb:
    def __init__(self, table):
        self._table = table

    def Table(self, name):
        return self._table


def test_put_and_get_score_checkpoint(monkeypatch):
    table = FakeTable()
    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))

    dio.put_score_checkpoint(
        table_name="table",
        event_id=90008,
        training_request_fingerprint="train-fp",
        run_id="run-1",
        status="success",
        aws_region="us-east-1",
        extra_attributes={"rows_scored": 10, "rmse": 1.2},
    )

    item = dio.get_score_checkpoint(
        table_name="table",
        event_id=90008,
        training_request_fingerprint="train-fp",
        aws_region="us-east-1",
    )

    assert item is not None
    assert item["status"] == "success"
    assert item["rmse"] == Decimal("1.2")
