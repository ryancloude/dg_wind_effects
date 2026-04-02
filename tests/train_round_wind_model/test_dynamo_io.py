from decimal import Decimal

import train_round_wind_model.dynamo_io as dio


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


def test_get_and_put_training_checkpoint(monkeypatch):
    table = FakeTable()
    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))

    dio.put_training_checkpoint(
        table_name="table",
        training_request_fingerprint="fp-1",
        run_id="run-1",
        status="success",
        aws_region="us-east-1",
        extra_attributes={
            "artifact_prefix": "artifacts/...",
            "rmse": 1.234,
            "r2": 0.456,
        },
    )

    item = dio.get_training_checkpoint(
        table_name="table",
        training_request_fingerprint="fp-1",
        aws_region="us-east-1",
    )

    assert item is not None
    assert item["status"] == "success"
    assert item["artifact_prefix"] == "artifacts/..."
    assert item["rmse"] == Decimal("1.234")
    assert item["r2"] == Decimal("0.456")


def test_put_training_run_summary(monkeypatch):
    table = FakeTable()
    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))

    out = dio.put_training_run_summary(
        table_name="table",
        run_id="run-1",
        stats={
            "processed_trainings": 1,
            "rmse": 1.5,
            "mae": 1.2,
        },
        aws_region="us-east-1",
    )

    assert out["run_id"] == "run-1"
    assert out["processed_trainings"] == 1
    assert out["rmse"] == Decimal("1.5")
    assert out["mae"] == Decimal("1.2")


def test_to_dynamodb_safe_converts_nested_floats():
    payload = {
        "rmse": 1.25,
        "metrics": {
            "mae": 0.99,
            "history": [1.0, 2.5, 3],
        },
    }

    out = dio._to_dynamodb_safe(payload)

    assert out["rmse"] == Decimal("1.25")
    assert out["metrics"]["mae"] == Decimal("0.99")
    assert out["metrics"]["history"][0] == Decimal("1.0")
    assert out["metrics"]["history"][1] == Decimal("2.5")
    assert out["metrics"]["history"][2] == 3
