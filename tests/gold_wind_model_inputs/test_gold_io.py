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


def test_load_hole_feature_rows_reads_hole_parquet(monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from io import BytesIO

    def _to_bytes(rows):
        t = pa.Table.from_pylist(rows)
        b = BytesIO()
        pq.write_table(t, b, compression="snappy")
        return b.getvalue()

    payloads = {
        "hole.parquet": _to_bytes(
            [
                {"tourn_id": 90008, "round_number": 1, "hole_number": 1, "player_key": "P1"},
                {"tourn_id": 90008, "round_number": 1, "hole_number": 2, "player_key": "P1"},
            ]
        )
    }
    fake_s3 = FakeS3(payloads)

    holes = gio.load_hole_feature_rows(
        bucket="bucket",
        hole_s3_key="hole.parquet",
        s3_client=fake_s3,
    )

    assert len(holes) == 2
    assert holes[0]["tourn_id"] == 90008
    assert holes[0]["hole_number"] == 1
    assert holes[1]["hole_number"] == 2


def test_load_parquet_rows_reads_generic_parquet(monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from io import BytesIO

    def _to_bytes(rows):
        t = pa.Table.from_pylist(rows)
        b = BytesIO()
        pq.write_table(t, b, compression="snappy")
        return b.getvalue()

    payloads = {
        "rows.parquet": _to_bytes(
            [
                {"tourn_id": 90008, "value": "a"},
                {"tourn_id": 90009, "value": "b"},
            ]
        )
    }
    fake_s3 = FakeS3(payloads)

    rows = gio.load_parquet_rows(
        bucket="bucket",
        key="rows.parquet",
        s3_client=fake_s3,
    )

    assert len(rows) == 2
    assert rows[0]["value"] == "a"
    assert rows[1]["tourn_id"] == 90009
