# event_page_parser.py
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from bs4 import BeautifulSoup as bs


UNSCHEDULED_PLACEHOLDER_PATTERNS = (
    re.compile(r"\bnot\s+scheduled\s+yet\b", re.IGNORECASE),
    re.compile(r"\bevent\s+has\s+not\s+been\s+scheduled\b", re.IGNORECASE),
    re.compile(r"\btournament\s+has\s+not\sF+been\s+scheduled\b", re.IGNORECASE),
    re.compile(r"\bdetails\s+for\s+this\s+event\s+are\s+not\s+available\s+yet\b", re.IGNORECASE),
)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def safe_text(node, default: str = "") -> str:
    return node.get_text(" ", strip=True) if node else default


def normalize_ws(value: str) -> str:
    """Collapse repeated whitespace so text matching is less brittle."""
    return re.sub(r"\s+", " ", value).strip()


def idempotency_sha256(parsed: Dict[str, Any]) -> str:
    """
    Build a stable hash from the parsed payload.

    This hash intentionally ignores storage metadata and focuses on fields that
    define whether the event page meaningfully changed.
    """
    payload = {
        "name": parsed.get("name", ""),
        "raw_date_str": parsed.get("raw_date_str", ""),
        "start_date": parsed.get("start_date", ""),
        "end_date": parsed.get("end_date", ""),
        "status_text": parsed.get("status_text", ""),
        "division_rounds": parsed.get("division_rounds", {}),
        "is_unscheduled_placeholder": parsed.get("is_unscheduled_placeholder", False),
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sha256_text(serialized)


def parse_date_range(raw: str) -> Tuple[str, str]:
    """
    Parse PDGA event date text into ISO start and end dates.

    Supported inputs include:
      - 12-Apr-2025
      - 12-Apr-2025 to 13-Apr-2025
      - 12-Apr to 13-Apr-2025
      - 12-Apr-2025 - 13-Apr-2025
    """
    normalized = normalize_ws(raw.replace("â€“", "to").replace("â€”", "to"))
    normalized = re.sub(r"\s*-\s*", " to ", normalized, count=1) if " - " in normalized else normalized

    if " to " not in normalized:
        dt = datetime.strptime(normalized, "%d-%b-%Y").strftime("%Y-%m-%d")
        return dt, dt

    left, right = [part.strip() for part in normalized.split(" to ", 1)]

    if re.search(r"-\d{4}$", right) is None:
        raise ValueError(f"End date missing year: {raw}")

    end_year = right.split("-")[-1]
    if re.search(r"-\d{4}$", left) is None:
        left = f"{left}-{end_year}"

    start = datetime.strptime(left, "%d-%b-%Y").strftime("%Y-%m-%d")
    end = datetime.strptime(right, "%d-%b-%Y").strftime("%Y-%m-%d")
    return start, end


def extract_date_str(soup: bs) -> str:
    """
    Extract the raw PDGA date string from the page.

    Prefer the structured tournament-date element. Fall back to page text
    scanning because older or inconsistent PDGA pages do not always render
    the same HTML structure.
    """
    node = soup.select_one("li.tournament-date")
    if node:
        raw = node.get_text(" ", strip=True)
        if ":" in raw:
            return raw.split(":", 1)[1].strip()
        return raw.strip()

    text = soup.get_text("\n", strip=True)
    for line in text.splitlines():
        lower = line.strip().lower()
        if lower.startswith("dates:") or lower.startswith("date:"):
            return line.split(":", 1)[1].strip()

    raise ValueError("Could not find date on page")


def extract_status_text(soup: bs) -> str:
    """
    Extract event status from either the structured table cell or a text fallback.

    The fallback exists because some PDGA pages flatten label/value rows into
    generic text blocks rather than preserving a clean status cell.
    """
    node = soup.select_one("td.status")
    if node:
        return node.get_text(" ", strip=True)

    text = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    for idx, line in enumerate(lines):
        if line.lower() == "status total players" and idx + 1 < len(lines):
            nxt = lines[idx + 1]
            match = re.match(r"^(.*?)(\d+)$", nxt)
            return match.group(1).strip() if match else nxt

    return ""


def find_division_rounds(soup: bs) -> Dict[str, int]:
    """
    Find the maximum round number shown for each division table.

    This uses heading text such as:
      MA1 · Advanced (50)

    and scans the next table header for columns like Rd1, Rd2, Rd3.
    """
    division_rounds: Dict[str, int] = {}
    round_pat = re.compile(r"\bRd(\d+)\b", re.IGNORECASE)

    for heading in soup.find_all(["h2", "h3", "h4"]):
        title = normalize_ws(heading.get_text(" ", strip=True))
        if "·" not in title and "Â·" not in title:
            continue

        separator = "·" if "·" in title else "Â·"
        div_code = title.split(separator, 1)[0].strip()

        table = heading.find_next("table")
        if not table:
            continue

        header = table.find("tr")
        header_text = header.get_text(" ", strip=True) if header else table.get_text(" ", strip=True)

        rounds = {int(match.group(1)) for match in round_pat.finditer(header_text)}
        if rounds:
            division_rounds[div_code] = max(rounds)

    return division_rounds


def looks_like_unscheduled_event_page(parsed: Dict[str, Any], soup: bs) -> bool:
    """
    Detect PDGA placeholder pages for event IDs that exist but are not scheduled yet.

    The strongest signal is explicit placeholder copy in the visible text.
    As a fallback, the page is treated as a placeholder when dates, status,
    and division rounds are all absent and the page text still suggests a
    not-yet-scheduled event.
    """
    visible_text = normalize_ws(soup.get_text(" ", strip=True))
    lower_text = visible_text.lower()

    phrase_hit = any(pattern.search(visible_text) for pattern in UNSCHEDULED_PLACEHOLDER_PATTERNS)
    if phrase_hit:
        return True

    missing_dates = not parsed.get("start_date") and not parsed.get("end_date")
    missing_rounds = not parsed.get("division_rounds")
    missing_status = not parsed.get("status_text")

    return missing_dates and missing_rounds and missing_status and "scheduled" in lower_text and "yet" in lower_text


def parse_event_page(event_id: int, html: str, source_url: Optional[str] = None) -> Dict[str, Any]:
    """
    Parse a PDGA event page into a normalized metadata payload.

    The parser is intentionally tolerant:
    - missing fields become warnings rather than hard failures
    - the raw HTML is still hashable and storable even when extraction is partial
    """
    soup = bs(html, "html.parser")

    warnings = []
    name = safe_text(soup.find("h1"))

    raw_date_str = ""
    start_date, end_date = "", ""
    try:
        raw_date_str = extract_date_str(soup)
        start_date, end_date = parse_date_range(raw_date_str)
    except Exception as exc:
        warnings.append(f"date_parse_failed:{exc}")

    status_text = extract_status_text(soup)
    if not status_text:
        warnings.append("status_not_found")

    division_rounds = find_division_rounds(soup)
    if not division_rounds:
        warnings.append("division_rounds_empty")

    out = {
        "event_id": int(event_id),
        "source_url": source_url or f"https://www.pdga.com/tour/event/{event_id}",
        "name": name,
        "raw_date_str": raw_date_str,
        "start_date": start_date,
        "end_date": end_date,
        "status_text": status_text,
        "division_rounds": division_rounds,
        "content_sha256": sha256_text(html),
        "parser_version": "event-page-v2",
        "parse_warnings": warnings,
        "raw_html_sha256": sha256_text(html),
    }
    # Placeholder classification is part of the parsed business meaning because
    # backfill mode uses it to decide when to stop scanning sequential IDs.
    out["is_unscheduled_placeholder"] = looks_like_unscheduled_event_page(out, soup)
    out["idempotency_sha256"] = idempotency_sha256(out)
    return out