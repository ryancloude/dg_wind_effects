from ingest_pdga_event_pages.repair_metadata_dates import compute_repaired_dates


def test_compute_repaired_dates_returns_none_when_dates_already_valid():
    item = {
        "raw_date_str": "12-Apr-2025 to 14-Apr-2025",
        "start_date": "2025-04-12",
        "end_date": "2025-04-14",
    }
    assert compute_repaired_dates(item) is None


def test_compute_repaired_dates_repairs_cross_year_when_start_gt_end():
    item = {
        "raw_date_str": "08-Nov to 17-Jan-2026",
        "start_date": "2026-11-08",
        "end_date": "2026-01-17",
    }
    repaired = compute_repaired_dates(item)
    assert repaired == ("2025-11-08", "2026-01-17")


def test_compute_repaired_dates_returns_none_when_raw_date_missing():
    item = {
        "raw_date_str": "",
        "start_date": "2026-11-08",
        "end_date": "2026-01-17",
    }
    assert compute_repaired_dates(item) is None