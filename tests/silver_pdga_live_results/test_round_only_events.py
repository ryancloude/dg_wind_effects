from types import SimpleNamespace

import silver_pdga_live_results.runner as runner
from silver_pdga_live_results.models import BronzeRoundSource
from silver_pdga_live_results.parquet_io import overwrite_event_tables


class FakeS3Client:
    def __init__(self):
        self.put_calls = []
        self.copy_calls = []
        self.delete_calls = []

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)

    def copy_object(self, **kwargs):
        self.copy_calls.append(kwargs)

    def delete_object(self, **kwargs):
        self.delete_calls.append(kwargs)


def test_runner_allows_round_only_event_in_dry_run(monkeypatch):
    args = SimpleNamespace(
        event_ids="90008",
        bucket=None,
        ddb_table=None,
        dry_run=True,
        force_events=False,
        run_mode="pending_only",
        include_dq_failed_in_pending=False,
        progress_every=1,
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(
            s3_bucket="bucket",
            ddb_table="table",
            aws_region="us-east-1",
            ddb_status_end_date_gsi="gsi_status_end_date",
        ),
    )
    monkeypatch.setattr(
        runner,
        "load_candidate_event_metadata",
        lambda **kwargs: [{"event_id": 90008, "division_rounds": {"MA3": 1}}],
    )
    monkeypatch.setattr(runner, "load_live_results_state_items", lambda **kwargs: [{}])

    source = BronzeRoundSource(
        event_id=90008,
        division="MA3",
        round_number=1,
        source_json_key="bronze/pdga/live_results/event_id=90008/division=MA3/round=1/fetch_date=2025-05-17/fetch_ts=2025-05-17T22:35:04Z.json",
        source_meta_key="",
        source_content_sha256="sha-1",
        source_fetched_at_utc="2025-05-17T22:35:04Z",
        payload={},
    )
    monkeypatch.setattr(runner, "build_round_sources", lambda **kwargs: [source])
    monkeypatch.setattr(runner, "compute_event_source_fingerprint", lambda _sources: "fp-1")
    monkeypatch.setattr(runner, "get_silver_event_checkpoint", lambda **kwargs: None)

    # Round rows present, hole rows empty -> should be allowed.
    monkeypatch.setattr(
        runner,
        "normalize_event_records",
        lambda **kwargs: (
            [
                {
                    "event_year": 2025,
                    "tourn_id": 90008,
                    "round_number": 1,
                    "player_key": "PDGA#123",
                    "source_fetched_at_utc": "2025-05-17T22:35:04Z",
                    "scorecard_updated_at_ts": "2025-05-17T15:35:29",
                    "update_date_ts": "2025-05-17T22:35:04",
                    "source_json_key": "k1",
                }
            ],
            [],
        ),
    )
    monkeypatch.setattr(runner, "validate_quality", lambda **kwargs: [])

    overwrite_calls = []
    monkeypatch.setattr(runner, "overwrite_event_tables", lambda **kwargs: overwrite_calls.append(kwargs))
    monkeypatch.setattr(runner, "put_silver_event_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(runner, "put_silver_run_summary", lambda **kwargs: None)

    printed = []
    monkeypatch.setattr(runner, "print", lambda obj: printed.append(obj), raising=False)

    exit_code = runner.main()

    assert exit_code == 0
    assert overwrite_calls == []  # dry-run: no writes
    summary = next(obj["silver_summary"] for obj in printed if "silver_summary" in obj)
    assert summary["processed_events"] == 1
    assert summary["failed_events"] == 0
    assert summary["events_without_hole_detail"] == 1
    assert summary["hole_rows_written"] == 0


def test_overwrite_event_tables_deletes_stale_hole_file_when_no_holes(monkeypatch):
    fake_s3 = FakeS3Client()
    monkeypatch.setattr(
        "silver_pdga_live_results.parquet_io._write_rows_to_parquet_bytes",
        lambda rows: b"parquet-bytes",
    )

    result = overwrite_event_tables(
        bucket="bucket",
        event_year=2025,
        event_id=90008,
        run_id="run-1",
        round_rows=[{"tourn_id": 90008, "round_number": 1, "player_key": "PDGA#123"}],
        hole_rows=[],
        s3_client=fake_s3,
    )

    assert result["round_key"].endswith("/player_rounds.parquet")
    assert result["hole_key"] == ""

    # Only round parquet gets put/copied; no hole parquet put/copy.
    assert len(fake_s3.put_calls) == 1
    assert len(fake_s3.copy_calls) == 1

    # Final hole key should be deleted to prevent stale data.
    deleted_keys = [call["Key"] for call in fake_s3.delete_calls]
    assert any(key.endswith("/player_holes.parquet") for key in deleted_keys)