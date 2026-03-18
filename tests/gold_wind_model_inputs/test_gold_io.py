import gold_wind_model_inputs.gold_io as gio


class _Body:
    def __init__(self, data: bytes):
        self._data = data

    def read(self):
        return self._data


class FakeS3:
    def __init__(self, payloads: dict[str, bytes]):
        self.payloads = payloads

    def get_object(self, Bucket, Key):
        return {"Body": _Body(self.payloads[Key])}


def test_load_event_input_tables_reads_hole_and_optional_round(monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from io import BytesIO

    def _to_bytes(rows):
        t = pa.Table.from_pylist(rows)
        b = BytesIO()
        pq.write_table(t, b, compression="snappy")
        return b.getvalue()

    payloads = {
        "hole.parquet": _to_bytes([{"tourn_id": 90008, "hole_number": 1}]),
        "round.parquet": _to_bytes([{"tourn_id": 90008, "round_number": 1}]),
    }
    fake_s3 = FakeS3(payloads)

    holes, rounds = gio.load_event_input_tables(
        bucket="bucket",
        hole_s3_key="hole.parquet",
        round_s3_key="round.parquet",
        s3_client=fake_s3,
    )

    assert len(holes) == 1
    assert len(rounds) == 1
    assert holes[0]["tourn_id"] == 90008


def test_load_event_input_tables_hole_only(monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from io import BytesIO

    def _to_bytes(rows):
        t = pa.Table.from_pylist(rows)
        b = BytesIO()
        pq.write_table(t, b, compression="snappy")
        return b.getvalue()

    payloads = {"hole.parquet": _to_bytes([{"tourn_id": 90008, "hole_number": 1}])}
    fake_s3 = FakeS3(payloads)

    holes, rounds = gio.load_event_input_tables(
        bucket="bucket",
        hole_s3_key="hole.parquet",
        round_s3_key=None,
        s3_client=fake_s3,
    )

    assert len(holes) == 1
    assert rounds == []