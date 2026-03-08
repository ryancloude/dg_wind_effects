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
    re.compile(r"\btournament\s+has\s+not\s+been\s+scheduled\b", re.IGNORECASE),
    re.compile(r"\bdetails\s+for\s+this\s+event\s+are\s+not\s+available\s+yet\b", re.IGNORECASE),
)

DIVISION_SEPARATOR_RE = re.compile(r"\s*(?:\u00b7|\u00c2\u00b7)\s*")
LOCATION_PREFIX_RE = re.compile(r"^location:\s*(.+)$", re.IGNORECASE)
US_STATE_CODE_RE = re.compile(r"^[A-Z]{2}$")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def safe_text(node, default: str = "") -> str:
    return node.get_text(" ", strip=True) if node else default


def normalize_ws(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def idempotency_sha256(parsed: Dict[str, Any]) -> str:
    payload = {
        "name": parsed.get("name", ""),
        "raw_date_str": parsed.get("raw_date_str", ""),
        "start_date": parsed.get("start_date", ""),
        "end_date": parsed.get("end_date", ""),
        "status_text": parsed.get("status_text", ""),
        "division_rounds": parsed.get("division_rounds", {}),
        "is_unscheduled_placeholder": parsed.get("is_unscheduled_placeholder", False),
        "location_raw": parsed.get("location_raw", ""),
        "city": parsed.get("city", ""),
        "state": parsed.get("state", ""),
        "country": parsed.get("country", ""),
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sha256_text(serialized)


def parse_date_range(raw: str) -> Tuple[str, str]:
    normalized = normalize_ws(raw.replace("Ã¢â‚¬â€œ", "to").replace("Ã¢â‚¬â€", "to"))
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
    division_rounds: Dict[str, int] = {}
    round_pat = re.compile(r"\bRd(\d+)\b", re.IGNORECASE)

    for heading in soup.find_all(["h2", "h3", "h4"]):
        title = normalize_ws(heading.get_text(" ", strip=True))
        if not DIVISION_SEPARATOR_RE.search(title):
            continue

        div_code = DIVISION_SEPARATOR_RE.split(title, maxsplit=1)[0].strip()

        table = heading.find_next("table")
        if not table:
            continue

        header = table.find("tr")
        header_text = header.get_text(" ", strip=True) if header else table.get_text(" ", strip=True)

        rounds = {int(match.group(1)) for match in round_pat.finditer(header_text)}
        if rounds:
            division_rounds[div_code] = max(rounds)

    return division_rounds


def extract_raw_location(soup: bs) -> str:
    node = soup.select_one("li.tournament-location")
    if node:
        raw = normalize_ws(node.get_text(" ", strip=True))
        if ":" in raw:
            return normalize_ws(raw.split(":", 1)[1])
        return raw

    text = soup.get_text("\n", strip=True)
    lines = [normalize_ws(line) for line in text.splitlines() if normalize_ws(line)]

    for idx, line in enumerate(lines):
        match = LOCATION_PREFIX_RE.match(line)
        if match:
            return normalize_ws(match.group(1))
        if line.lower() == "location" and idx + 1 < len(lines):
            return normalize_ws(lines[idx + 1])

    return ""


def parse_location_parts(raw_location: str) -> Tuple[str, str, str]:
    """
    Parse location string into city/state/country heuristically.

    Example:
      "Austin, TX, United States" -> ("Austin", "TX", "United States")
    """
    raw_location = normalize_ws(raw_location)
    if not raw_location:
        return "", "", ""

    parts = [normalize_ws(part) for part in raw_location.split(",") if normalize_ws(part)]
    if not parts:
        return "", "", ""

    if len(parts) >= 3:
        city = parts[0]
        state = parts[1]
        country = ", ".join(parts[2:])
        return city, state, country

    if len(parts) == 2:
        city = parts[0]
        second = parts[1]
        if US_STATE_CODE_RE.fullmatch(second):
            return city, second, ""
        return city, "", second

    return parts[0], "", ""


def looks_like_unscheduled_event_page(parsed: Dict[str, Any], soup: bs) -> bool:
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

    location_raw = extract_raw_location(soup)
    city, state, country = parse_location_parts(location_raw)

    out = {
        "event_id": int(event_id),
        "source_url": source_url or f"https://www.pdga.com/tour/event/{event_id}",
        "name": name,
        "raw_date_str": raw_date_str,
        "start_date": start_date,
        "end_date": end_date,
        "status_text": status_text,
        "division_rounds": division_rounds,
        "location_raw": location_raw,
        "city": city,
        "state": state,
        "country": country,
        "content_sha256": sha256_text(html),
        "parser_version": "event-page-v3",
        "parse_warnings": warnings,
        "raw_html_sha256": sha256_text(html),
    }
    out["is_unscheduled_placeholder"] = looks_like_unscheduled_event_page(out, soup)
    out["idempotency_sha256"] = idempotency_sha256(out)
    return out