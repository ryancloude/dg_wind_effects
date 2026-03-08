from pathlib import Path

import pytest

from ingest_pdga_event_pages.event_page_parser import (
    idempotency_sha256,
    parse_date_range,
    parse_event_page,
)


FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


def test_parse_date_range_single_day():
    start, end = parse_date_range("12-Apr-2025")
    assert start == "2025-04-12"
    assert end == "2025-04-12"


def test_parse_date_range_multi_day_with_missing_start_year():
    start, end = parse_date_range("12-Apr to 14-Apr-2025")
    assert start == "2025-04-12"
    assert end == "2025-04-14"


def test_parse_date_range_raises_for_invalid_format():
    with pytest.raises(ValueError):
        parse_date_range("April 12, 2025")


def test_idempotency_hash_is_stable_for_same_payload():
    payload = {
        "name": "Sample Event",
        "raw_date_str": "12-Apr-2025",
        "start_date": "2025-04-12",
        "end_date": "2025-04-12",
        "status_text": "Official",
        "division_rounds": {"MA1": 2},
        "is_unscheduled_placeholder": False,
        "location_raw": "Austin, TX, United States",
        "city": "Austin",
        "state": "TX",
        "country": "United States",
    }

    assert idempotency_sha256(payload) == idempotency_sha256(payload)


def test_idempotency_hash_changes_for_meaningful_field_change():
    payload_a = {
        "name": "Sample Event",
        "raw_date_str": "12-Apr-2025",
        "start_date": "2025-04-12",
        "end_date": "2025-04-12",
        "status_text": "Official",
        "division_rounds": {"MA1": 2},
        "is_unscheduled_placeholder": False,
        "location_raw": "Austin, TX, United States",
        "city": "Austin",
        "state": "TX",
        "country": "United States",
    }
    payload_b = {
        **payload_a,
        "country": "Canada",
    }

    assert idempotency_sha256(payload_a) != idempotency_sha256(payload_b)


def test_parse_normal_event_page():
    html = """
    <html>
      <body>
        <h1>Sample B-Tier</h1>
        <ul>
          <li class="tournament-date">Dates: 12-Apr-2025 to 13-Apr-2025</li>
          <li class="tournament-location">Location: Austin, TX, United States</li>
        </ul>
        <table>
          <tr><td class="status">Official</td></tr>
        </table>
        <h2>MA1 Â· Advanced (50)</h2>
        <table>
          <tr>
            <th>Place</th>
            <th>Rd1</th>
            <th>Rd2</th>
          </tr>
        </table>
      </body>
    </html>
    """

    parsed = parse_event_page(event_id=100001, html=html)

    assert parsed["event_id"] == 100001
    assert parsed["name"] == "Sample B-Tier"
    assert parsed["start_date"] == "2025-04-12"
    assert parsed["end_date"] == "2025-04-13"
    assert parsed["status_text"] == "Official"
    assert parsed["division_rounds"] == {"MA1": 2}
    assert parsed["location_raw"] == "Austin, TX, United States"
    assert parsed["city"] == "Austin"
    assert parsed["state"] == "TX"
    assert parsed["country"] == "United States"
    assert parsed["is_unscheduled_placeholder"] is False
    assert parsed["parse_warnings"] == []


def test_parse_location_fallback_from_text_line():
    html = """
    <html>
      <body>
        <h1>Sample C-Tier</h1>
        <div>Location: Calgary, AB, Canada</div>
        <ul>
          <li class="tournament-date">Dates: 12-Apr-2025</li>
        </ul>
        <table>
          <tr><td class="status">Official</td></tr>
        </table>
        <h2>MPO Â· Mixed Pro Open (20)</h2>
        <table>
          <tr>
            <th>Place</th>
            <th>Rd1</th>
          </tr>
        </table>
      </body>
    </html>
    """

    parsed = parse_event_page(event_id=100006, html=html)

    assert parsed["location_raw"] == "Calgary, AB, Canada"
    assert parsed["city"] == "Calgary"
    assert parsed["state"] == "AB"
    assert parsed["country"] == "Canada"


def test_parse_unscheduled_placeholder_page():
    html = """
    <html>
      <body>
        <h1>Future Event</h1>
        <div>This tournament has not been scheduled yet.</div>
      </body>
    </html>
    """

    parsed = parse_event_page(event_id=101752, html=html)

    assert parsed["event_id"] == 101752
    assert parsed["name"] == "Future Event"
    assert parsed["start_date"] == ""
    assert parsed["end_date"] == ""
    assert parsed["status_text"] == ""
    assert parsed["division_rounds"] == {}
    assert parsed["location_raw"] == ""
    assert parsed["city"] == ""
    assert parsed["state"] == ""
    assert parsed["country"] == ""
    assert parsed["is_unscheduled_placeholder"] is True
    assert "status_not_found" in parsed["parse_warnings"]
    assert "division_rounds_empty" in parsed["parse_warnings"]
    assert any(warning.startswith("date_parse_failed:") for warning in parsed["parse_warnings"])


def test_parse_normal_event_page_fixture():
    html = load_fixture("event_page_normal.html")
    parsed = parse_event_page(event_id=100002, html=html)

    assert parsed["event_id"] == 100002
    assert parsed["name"] == "Sample B-Tier"
    assert parsed["start_date"] == "2025-04-12"
    assert parsed["end_date"] == "2025-04-12"
    assert parsed["status_text"] == "Official"
    assert parsed["division_rounds"] == {"MA1": 2, "FA1": 1}
    assert parsed["is_unscheduled_placeholder"] is False
    assert parsed["parse_warnings"] == []


def test_parse_multi_day_event_page_fixture():
    html = load_fixture("event_page_multi_day.html")
    parsed = parse_event_page(event_id=100003, html=html)

    assert parsed["event_id"] == 100003
    assert parsed["name"] == "Sample A-Tier"
    assert parsed["start_date"] == "2025-04-12"
    assert parsed["end_date"] == "2025-04-14"
    assert parsed["status_text"] == "Final"
    assert parsed["division_rounds"] == {"MPO": 3}
    assert parsed["is_unscheduled_placeholder"] is False
    assert parsed["parse_warnings"] == []


def test_parse_placeholder_fixture():
    html = load_fixture("event_page_placeholder_unscheduled.html")
    parsed = parse_event_page(event_id=101752, html=html)

    assert parsed["event_id"] == 101752
    assert parsed["name"] == "Future PDGA Event"
    assert parsed["start_date"] == ""
    assert parsed["end_date"] == ""
    assert parsed["status_text"] == ""
    assert parsed["division_rounds"] == {}
    assert parsed["is_unscheduled_placeholder"] is True
    assert "status_not_found" in parsed["parse_warnings"]
    assert "division_rounds_empty" in parsed["parse_warnings"]
    assert any(warning.startswith("date_parse_failed:") for warning in parsed["parse_warnings"])


def test_parse_missing_status_fixture():
    html = load_fixture("event_page_missing_status.html")
    parsed = parse_event_page(event_id=100004, html=html)

    assert parsed["event_id"] == 100004
    assert parsed["name"] == "Event Missing Status"
    assert parsed["start_date"] == "2025-05-27"
    assert parsed["end_date"] == "2025-05-27"
    assert parsed["status_text"] == ""
    assert parsed["division_rounds"] == {"MA1": 2}
    assert parsed["is_unscheduled_placeholder"] is False
    assert "status_not_found" in parsed["parse_warnings"]


def test_parse_no_divisions_fixture():
    html = load_fixture("event_page_no_divisions.html")
    parsed = parse_event_page(event_id=100005, html=html)

    assert parsed["event_id"] == 100005
    assert parsed["name"] == "Event Without Divisions"
    assert parsed["start_date"] == "2025-06-01"
    assert parsed["end_date"] == "2025-06-02"
    assert parsed["status_text"] == "Official"
    assert parsed["division_rounds"] == {}
    assert parsed["is_unscheduled_placeholder"] is False
    assert "division_rounds_empty" in parsed["parse_warnings"]