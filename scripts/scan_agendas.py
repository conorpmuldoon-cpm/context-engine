"""
scan_agendas.py — Discover and parse council agenda PDFs from syr.gov.

Scrapes the Common Council Meetings & Agendas page, finds meeting pages
for the target year, downloads agenda PDFs, and calls parse_agenda.py
to produce structured JSON.

For Regular Meeting pages: prefers the short agenda PDF (~300-500KB).
For Study Session pages (which only have the full agenda book): downloads
the agenda book and calls parse_agenda.py with --max-pages 8.

Usage:
    python scripts/scan_agendas.py               # scan current year
    python scripts/scan_agendas.py --dry-run     # list what would be downloaded
    python scripts/scan_agendas.py --year 2025   # scan a specific year
"""

import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
AGENDAS_DIR = PROJECT_ROOT / "outputs" / "agendas"
STATE_PATH = PROJECT_ROOT / "outputs" / "agenda-scanner-state.json"

sys.path.insert(0, str(SCRIPT_DIR))
from collector_utils import load_state, save_state, setup_logger

LISTING_URL = "https://www.syr.gov/Departments/Common-Council/Common-Council-Agendas"
BASE_URL = "https://www.syr.gov"

# Max pages to OCR from agenda books (numbered item list is in first ~5-8 pages)
AGENDA_BOOK_MAX_PAGES = 8

# Date pattern in PDF filenames: MM.DD.YYYY or MM-DD-YYYY
_PDF_DATE_RE = re.compile(r"(\d{2})[.\-](\d{2})[.\-](\d{4})")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def extract_date_from_filename(filename: str) -> str | None:
    """Extract YYYY-MM-DD date from PDF filename like 03.09.2026-agenda.pdf."""
    m = _PDF_DATE_RE.search(filename)
    if m:
        month, day, year = m.group(1), m.group(2), m.group(3)
        return f"{year}-{month}-{day}"
    return None


def get_processed_dates() -> set[str]:
    """Get dates that already have parsed agendas."""
    dates = set()

    # From state file
    state = load_state(STATE_PATH)
    dates.update(state.get("processed_dates", []))

    # From existing agenda files
    AGENDAS_DIR.mkdir(parents=True, exist_ok=True)
    for f in AGENDAS_DIR.glob("*_agenda.json"):
        # Filename: 2026-03-09_agenda.json
        date_str = f.stem.replace("_agenda", "")
        if re.match(r"\d{4}-\d{2}-\d{2}$", date_str):
            dates.add(date_str)

    return dates


def discover_meeting_pages(year: int, logger=None) -> list[dict]:
    """Scrape the agenda listing page to find meeting page links for a year.

    Returns list of {"url": str, "title": str} dicts.
    """
    from playwright.sync_api import sync_playwright

    if logger:
        logger.info(f"Fetching agenda listing page for {year}...")

    meetings = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            page.goto(LISTING_URL, wait_until="networkidle", timeout=30000)
        except Exception as e:
            if logger:
                logger.error(f"Failed to load listing page: {e}")
            browser.close()
            return []

        # Find all links to meeting pages for the target year
        # Pattern: /Meetings-and-Agendas/YYYY/...
        links = page.query_selector_all(f'a[href*="/Meetings-and-Agendas/{year}/"]')

        for link in links:
            href = link.get_attribute("href")
            text = link.inner_text().strip()
            if href:
                if not href.startswith("http"):
                    href = BASE_URL + href
                meetings.append({"url": href, "title": text})

        browser.close()

    if logger:
        logger.info(f"  Found {len(meetings)} meeting page(s) for {year}")

    return meetings


def find_agenda_pdf(meeting_url: str, logger=None) -> dict | None:
    """Visit a meeting page and find the best agenda PDF link.

    Returns {"url": str, "is_book": bool, "filename": str} or None.
    """
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            page.goto(meeting_url, wait_until="networkidle", timeout=30000)
        except Exception as e:
            if logger:
                logger.warning(f"  Failed to load meeting page: {e}")
            browser.close()
            return None

        pdf_links = page.query_selector_all('a[href$=".pdf"]')

        agenda_pdf = None
        book_pdf = None

        for link in pdf_links:
            href = link.get_attribute("href")
            if not href:
                continue
            href_lower = href.lower()
            filename = href.split("/")[-1]

            # Skip non-agenda PDFs (minutes, etc.)
            if "agenda" not in href_lower:
                continue

            if "agenda-book" in href_lower or "book" in href_lower:
                book_pdf = {"url": href, "filename": filename}
            else:
                agenda_pdf = {"url": href, "filename": filename}

        browser.close()

    # Prefer short agenda; fall back to agenda book
    if agenda_pdf:
        if not agenda_pdf["url"].startswith("http"):
            agenda_pdf["url"] = BASE_URL + agenda_pdf["url"]
        return {**agenda_pdf, "is_book": False}
    elif book_pdf:
        if not book_pdf["url"].startswith("http"):
            book_pdf["url"] = BASE_URL + book_pdf["url"]
        return {**book_pdf, "is_book": True}

    return None


def run_parse_agenda(pdf_url: str, is_book: bool, logger=None) -> bool:
    """Call parse_agenda.py on a PDF URL. Returns True on success."""
    python = sys.executable
    cmd = [python, str(SCRIPT_DIR / "parse_agenda.py"), pdf_url]
    if is_book:
        cmd.extend(["--max-pages", str(AGENDA_BOOK_MAX_PAGES)])

    if logger:
        logger.info(f"  Running: parse_agenda.py {'(book, max-pages=' + str(AGENDA_BOOK_MAX_PAGES) + ')' if is_book else '(short agenda)'}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=str(PROJECT_ROOT),
        )

        if result.stdout:
            for line in result.stdout.strip().splitlines():
                if logger:
                    logger.debug(f"    {line}")

        if result.returncode != 0:
            if logger:
                logger.warning(f"  parse_agenda.py failed (exit {result.returncode})")
                if result.stderr:
                    logger.warning(f"    {result.stderr[:300]}")
            return False

        return True

    except subprocess.TimeoutExpired:
        if logger:
            logger.warning("  parse_agenda.py timed out")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = sys.argv[1:]
    dry_run = "--dry-run" in args
    if dry_run:
        args.remove("--dry-run")

    year = datetime.now().year
    if "--year" in args:
        idx = args.index("--year")
        if idx + 1 < len(args):
            try:
                year = int(args[idx + 1])
            except ValueError:
                print("Error: --year requires a number")
                sys.exit(1)

    logger = setup_logger("scan-agendas")
    logger.info(f"=== Agenda Scanner {'(DRY RUN)' if dry_run else ''} — {year} ===")

    # Get already-processed dates
    processed = get_processed_dates()
    logger.info(f"Already processed: {len(processed)} agendas")

    # Discover meeting pages
    meetings = discover_meeting_pages(year, logger)
    if not meetings:
        logger.info("No meeting pages found.")
        return

    # Process each meeting page
    new_agendas = 0
    skipped = 0
    failed = 0

    for meeting in meetings:
        title = meeting["title"][:60]

        # Find agenda PDF on this meeting page
        pdf_info = find_agenda_pdf(meeting["url"], logger)
        if not pdf_info:
            logger.debug(f"  {title}: no agenda PDF found")
            skipped += 1
            continue

        # Extract date from filename
        date_str = extract_date_from_filename(pdf_info["filename"])
        if not date_str:
            logger.debug(f"  {title}: couldn't parse date from {pdf_info['filename']}")
            skipped += 1
            continue

        # Skip already processed
        if date_str in processed:
            logger.debug(f"  {title} ({date_str}): already processed")
            skipped += 1
            continue

        pdf_type = "book" if pdf_info["is_book"] else "agenda"
        logger.info(f"  {title} ({date_str}): found {pdf_type} PDF")

        if dry_run:
            logger.info(f"    Would download: {pdf_info['filename']}")
            new_agendas += 1
            continue

        # Download and parse
        ok = run_parse_agenda(pdf_info["url"], pdf_info["is_book"], logger)
        if ok:
            new_agendas += 1
            processed.add(date_str)

            # Update state after each success
            state = load_state(STATE_PATH)
            state["processed_dates"] = sorted(processed)
            state["last_run"] = datetime.now(timezone.utc).isoformat()
            state["total_processed"] = len(processed)
            save_state(STATE_PATH, state)
        else:
            failed += 1

    # Summary
    logger.info(f"\n{'=' * 50}")
    logger.info(f"AGENDA SCANNER SUMMARY {'(DRY RUN)' if dry_run else ''}")
    logger.info(f"  Meeting pages checked: {len(meetings)}")
    logger.info(f"  New agendas {'found' if dry_run else 'parsed'}: {new_agendas}")
    logger.info(f"  Skipped (already processed or no PDF): {skipped}")
    if failed:
        logger.info(f"  Failed: {failed}")


if __name__ == "__main__":
    main()
