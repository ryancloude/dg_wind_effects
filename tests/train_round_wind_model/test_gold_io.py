from io import BytesIO
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

import train_round_wind_model.gold_io as gio


class _Body:
    def __init__(self, data: bytes):
        self._data = data

    def read(self):
        return self._data


class FakeS3:
    def __init__(self, payloads: dict[str, bytes], objects: list[dict]):
        self.payloads = payloads
        self.objects = objects

    def list_objects_v2(self, **kwargs):
        prefix = kwargs["Prefix"]
        return {
            "Contents": [o for o in self.objects if str(o["Key"]).startswith(prefix)],
            "IsTruncated": False,
        }

    def get_object(self, Bucket, Key):
        return {"Body": _Body(self.payloads[Key])}


def _to_bytes(rows):
    t = pa.Table.from_pylist(rows)
    b = BytesIO()
    pq.write_table(t, b, compression="snappy")
    return b.getvalue()


def test_load_model_input_round_dataframe():
    key = "gold/pdga/wind_effects/model_inputs_round/event_year=2026/tourn_id=90008/model_inputs_round.parquet"
    payloads = {
        key: _to_bytes(
            [
                {"tourn_id": 90008, "round_number": 1, "player_key": "P1", "row_hash_sha256": "a"},
                {"tourn_id": 90008, "round_number": 1, "player_key": "P2", "row_hash_sha256": "b"},
            ]
        )
    }
    fake_s3 = FakeS3(
        payloads=payloads,
        objects=[
            {
                "Key": key,
                "ETag": '"etag-1"',
                "Size": 123,
                "LastModified": datetime(2026, 4, 2, 12, 0, 0, tzinfo=timezone.utc),
            }
        ],
    )

    df, objects = gio.load_model_input_round_dataframe(
        bucket="bucket",
        s3_client=fake_s3,
    )

    assert len(df) == 2
    assert len(objects) == 1
    assert objects[0]["key"] == key
    assert objects[0]["etag"] == "etag-1"


def test_list_model_input_round_objects_filters_event_ids():
    keep_key = "gold/pdga/wind_effects/model_inputs_round/event_year=2026/tourn_id=90008/model_inputs_round.parquet"
    drop_key = "gold/pdga/wind_effects/model_inputs_round/event_year=2026/tourn_id=90009/model_inputs_round.parquet"
    fake_s3 = FakeS3(
        payloads={},
        objects=[
            {"Key": keep_key, "ETag": '"a"', "Size": 1, "LastModified": datetime(2026, 4, 2, tzinfo=timezone.utc)},
            {"Key": drop_key, "ETag": '"b"', "Size": 1, "LastModified": datetime(2026, 4, 2, tzinfo=timezone.utc)},
        ],
    )

    objects = gio.list_model_input_round_objects(
        bucket="bucket",
        event_ids=[90008],
        s3_client=fake_s3,
    )

    assert [o["key"] for o in objects] == [keep_key]

