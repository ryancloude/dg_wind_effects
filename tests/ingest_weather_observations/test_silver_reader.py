from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq

import ingest_weather_observations.silver_reader as silver_reader


class FakeBody:
    def __init__(self, payload: bytes):
        self._payload = payload

    def read(self):
        return self._payload


class FakeS3Client:
    def __init__(self, payload: bytes):
        self._payload = payload

    def get_object(self, **kwargs):
        return {"Body": FakeBody(self._payload)}


def _build_parquet_bytes() -> bytes:
    table = pa.table(
        {
            "round_number": [1, 1, 2],
            "player_key": ["A", "B", "A"],
            "tee_time_join_ts": ["2026-03-10T08:15:00", "2026-03-10T08:20:00", "2026-03-11T09:00:00"],
        }
    )
    buf = BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def test_load_player_round_rows():
    payload = _build_parquet_bytes()
    rows = silver_reader.load_player_round_rows(
        bucket="x",
        key="y",
        s3_client=FakeS3Client(payload),
    )
    assert len(rows) == 3
    assert rows[0]["round_number"] == 1


def test_compute_tee_time_source_fingerprint_deterministic():
    rows_a = [
        {"round_number": 1, "tee_time_join_ts": "2026-03-10T08:15:00", "player_key": "A"},
        {"round_number": 1, "tee_time_join_ts": "2026-03-10T08:20:00", "player_key": "B"},
    ]
    rows_b = list(reversed(rows_a))

    fp_a = silver_reader.compute_tee_time_source_fingerprint(rows_a)
    fp_b = silver_reader.compute_tee_time_source_fingerprint(rows_b)
    assert fp_a == fp_b