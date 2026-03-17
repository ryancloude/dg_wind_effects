from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq

import silver_weather_enriched.silver_io as sio


class FakeBody:
    def __init__(self, payload: bytes):
        self._payload = payload

    def read(self):
        return self._payload


class FakeS3Client:
    def __init__(self, objects: dict[str, bytes]):
        self.objects = objects

    def get_object(self, **kwargs):
        key = kwargs["Key"]
        return {"Body": FakeBody(self.objects[key])}


def _parquet_bytes(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows)
    buf = BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def test_load_event_input_tables():
    r_key, h_key, w_key = "r.parquet", "h.parquet", "w.parquet"
    s3 = FakeS3Client(
        {
            r_key: _parquet_bytes([{"tourn_id": 1, "round_number": 1, "player_key": "P1"}]),
            h_key: _parquet_bytes([{"tourn_id": 1, "round_number": 1, "hole_number": 1, "player_key": "P1"}]),
            w_key: _parquet_bytes([{"event_id": 1, "round_number": 1, "observation_hour_utc": "2026-03-17T08:00:00Z"}]),
        }
    )

    rr, hr, wr = sio.load_event_input_tables(
        bucket="b",
        round_s3_key=r_key,
        hole_s3_key=h_key,
        weather_s3_key=w_key,
        s3_client=s3,
    )
    assert len(rr) == 1 and len(hr) == 1 and len(wr) == 1