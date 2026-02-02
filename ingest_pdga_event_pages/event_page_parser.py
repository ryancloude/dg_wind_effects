# event_page_parser.py
from __future__ import annotations
import json
import hashlib
import re
from datetime import datetime
from typing import Any, Dict, Tuple, Optional

from bs4 import BeautifulSoup as bs


# --- small utilities ---

def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def safe_text(node, default: str = "") -> str:
    return node.get_text(" ", strip=True) if node else default

def idempotency_sha256(parsed: Dict[str, Any]) -> str:
    payload = {
        "name": parsed.get("name", ""),
        "raw_date_str": parsed.get("raw_date_str", ""),
        "start_date": parsed.get("start_date", ""),
        "end_date": parsed.get("end_date", ""),
        "status_text": parsed.get("status_text", ""),
        "division_rounds": parsed.get("division_rounds", {}),
    }
    s = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sha256_text(s)


# --- date parsing ---

DATE_TOKEN_RE = re.compile(r"\b(\d{1,2}-[A-Za-z]{3}(?:-\d{4})?)\b")


def parse_date_range(raw: str) -> Tuple[str, str]:
    """
    Handles:
      - '12-Apr-2025'
      - '12-Apr-2025 to 13-Apr-2025'
      - '12-Apr to 13-Apr-2025' (start missing year)
      - '12-Apr-2025 – 13-Apr-2025' (en dash)
    Returns (start_date, end_date) as YYYY-MM-DD.
    """
    raw = raw.strip().replace("–", "to").replace("—", "to")
    raw = re.sub(r"\s+", " ", raw)

    if " to " not in raw:
        dt = datetime.strptime(raw, "%d-%b-%Y").strftime("%Y-%m-%d")
        return dt, dt

    left, right = [p.strip() for p in raw.split(" to ", 1)]

    # end date should include year
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
    Prefer the structured tournament date element first.
    Fall back to scanning page text.
    """
    node = soup.select_one("li.tournament-date")
    if node:
        raw = node.get_text(" ", strip=True)
        # often like "Dates: 12-Apr-2025" or "Dates: 12-Apr to 13-Apr-2025"
        if ":" in raw:
            return raw.split(":", 1)[1].strip()
        return raw.strip()

    # Fallback: scan for a line containing 'Dates:' or 'Date:'
    text = soup.get_text("\n", strip=True)
    for line in text.splitlines():
        lower = line.strip().lower()
        if lower.startswith("dates:") or lower.startswith("date:"):
            return line.split(":", 1)[1].strip()

    raise ValueError("Could not find date on page")


# --- status parsing ---

def extract_status_text(soup: bs) -> str:
    # 1) Structured selectors (fast + reliable when present)
    node = soup.select_one("td.status")
    if node:
        return node.get_text(" ", strip=True)

    # 2) Text-block fallback for pages that render status as a label row
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    for i, line in enumerate(lines):
        if line.lower() == "status total players" and i + 1 < len(lines):
            nxt = lines[i + 1]
            # Strip trailing player count if it’s jammed on the end
            m = re.match(r"^(.*?)(\d+)$", nxt)
            return m.group(1).strip() if m else nxt

    return ""


# --- division / rounds parsing ---

def find_division_rounds(soup: bs) -> Dict[str, int]:
    """
    For each division heading like:
      'MA1 · Advanced (50)'
    find the next results table and count Rd columns (Rd1, Rd2, ...).
    Return division -> max round number.
    """
    division_rounds: Dict[str, int] = {}
    round_pat = re.compile(r"\bRd(\d+)\b", re.IGNORECASE)

    for h in soup.find_all(["h2", "h3", "h4"]):
        title = h.get_text(" ", strip=True)
        if "·" not in title:
            continue

        div_code = title.split("·", 1)[0].strip()

        table = h.find_next("table")
        if not table:
            continue

        header = table.find("tr")
        header_text = header.get_text(" ", strip=True) if header else table.get_text(" ", strip=True)

        rounds = {int(m.group(1)) for m in round_pat.finditer(header_text)}
        if rounds:
            division_rounds[div_code] = max(rounds)

    return division_rounds


# --- main public function ---

def parse_event_page(event_id: int, html: str, source_url: Optional[str] = None) -> Dict[str, Any]:
    soup = bs(html, "html.parser")

    warnings = []
    name = safe_text(soup.find("h1"))

    raw_date_str = ""
    start_date, end_date = "", ""
    try:
        raw_date_str = extract_date_str(soup)
        start_date, end_date = parse_date_range(raw_date_str)
    except Exception as e:
        warnings.append(f"date_parse_failed:{e}")

    status_text = extract_status_text(soup)
    if not status_text:
        warnings.append("status_not_found")

    division_rounds = find_division_rounds(soup)
    if not division_rounds:
        warnings.append("division_rounds_empty")

    raw_html_sha256 = sha256_text(html)

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
        "parser_version": "event-page-v1",
        "parse_warnings": warnings,
        "raw_html_sha256": raw_html_sha256
    }
    out["idempotency_sha256"] = idempotency_sha256(out)
    return out