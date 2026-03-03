from ingest_pdga_event_pages.event_page_parser import parse_event_page


def test_parse_normal_event_page():
    html = """
    <html>
      <body>
        <h1>Sample B-Tier</h1>
        <ul>
          <li class="tournament-date">Dates: 12-Apr-2025 to 13-Apr-2025</li>
        </ul>
        <table>
          <tr><td class="status">Official</td></tr>
        </table>
        <h2>MA1 · Advanced (50)</h2>
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
    assert parsed["is_unscheduled_placeholder"] is False
    assert parsed["parse_warnings"] == []


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
    assert parsed["is_unscheduled_placeholder"] is True
    assert "status_not_found" in parsed["parse_warnings"]
    assert "division_rounds_empty" in parsed["parse_warnings"]
    assert any(warning.startswith("date_parse_failed:") for warning in parsed["parse_warnings"])