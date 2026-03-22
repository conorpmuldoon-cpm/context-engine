"""
scan_website.py — City Website Monitor for the Context Engine

Crawls syr.gov news, announcements, and public notices sections using
Playwright headless browser, enriches via Claude Haiku API, and saves
context records automatically.

Usage:
    python scripts/scan_website.py                # normal daily run
    python scripts/scan_website.py --no-email     # skip email summary
    python scripts/scan_website.py --dry-run      # show what would be processed
    python scripts/scan_website.py --force-all    # ignore state, process all found items
    python scripts/scan_website.py --backfill     # also crawl year-based archive pages

Designed to be called from Windows Task Scheduler or GitHub Actions on a daily basis.
Requires: pip install playwright && python -m playwright install chromium
"""

import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

from collector_utils import (
    PROJECT_ROOT,
    WEB_CONTENT_DIR,
    load_taxonomy,
    load_entity_registry,
    load_schema,
    load_api_config,
    load_email_config,
    get_valid_tags,
    get_freshness_class,
    extract_entities,
    extract_departments,
    extract_systems,
    extract_amounts,
    enrich_with_claude,
    get_next_sequence,
    load_existing_records,
    check_duplicate,
    validate_tags,
    validate_record,
    build_context_record,
    save_record,
    load_state,
    save_state,
    setup_logger,
    send_collector_email,
    archive_raw_content,
)


# ---------------------------------------------------------------------------
# Target pages configuration
# ---------------------------------------------------------------------------
TARGET_PAGES = [
    {
        "name": "News",
        "url": "https://www.syr.gov/News",
        "source_type": "press_release",
    },
    {
        "name": "Announcements",
        "url": "https://www.syr.gov/Announcements",
        "source_type": "press_release",
    },
    {
        "name": "Public Notices",
        "url": "https://www.syr.gov/Public-Notices",
        "source_type": "public_notice",
    },
]

BACKFILL_PAGES_2026 = [
    {
        "name": "City News 2026",
        "url": "https://www.syr.gov/News/City-News/City-2026",
        "source_type": "press_release",
    },
    {
        "name": "Infrastructure 2026",
        "url": "https://www.syr.gov/News/Traffic-Infrastructure-News/Traffic-Infrastructure-2026",
        "source_type": "press_release",
    },
    {
        "name": "Parks 2026",
        "url": "https://www.syr.gov/News/Parks-News/Parks-2026",
        "source_type": "press_release",
    },
    {
        "name": "Fire 2026",
        "url": "https://www.syr.gov/News/Fire-News/Fire-2026",
        "source_type": "press_release",
    },
    {
        "name": "Police 2026",
        "url": "https://www.syr.gov/News/2026/Police-News",
        "source_type": "press_release",
    },
]

STATE_PATH = PROJECT_ROOT / "outputs" / "website-monitor-state.json"
CRAWL_DELAY = 5  # seconds between page navigations

# Regex to detect a date slug in a URL path (e.g., /2026-03-13-Mayors-Office-News)
_URL_DATE_RE = re.compile(r"/(\d{4})-(\d{2})-(\d{2})-")


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def parse_syr_gov_date(text: str) -> str | None:
    """Parse date formats found on syr.gov. Returns YYYY-MM-DD or None."""
    text = text.strip()

    # "March 10, 2026" or "Mar 10, 2026"
    for fmt in ["%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%m-%d-%Y"]:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Try extracting date from within longer text
    match = re.search(
        r"(?:January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+\d{1,2},?\s+\d{4}", text
    )
    if match:
        try:
            return datetime.strptime(match.group().replace(",", ""), "%B %d %Y").strftime("%Y-%m-%d")
        except ValueError:
            pass

    return None


def parse_date_from_url(url: str) -> str | None:
    """Extract YYYY-MM-DD from syr.gov URL slugs like /2026-03-13-Mayors-Office-News."""
    match = _URL_DATE_RE.search(url)
    if match:
        y, m, d = match.group(1), match.group(2), match.group(3)
        try:
            datetime(int(y), int(m), int(d))  # validate
            return f"{y}-{m}-{d}"
        except ValueError:
            pass
    return None


def is_article_url(url: str) -> bool:
    """Return True if URL looks like an individual article (has a date slug).

    Filters out category/index pages like /City-2023, /Police-News, /Archives
    which don't contain a date pattern in their path.
    """
    return bool(_URL_DATE_RE.search(url))


# ---------------------------------------------------------------------------
# Page scraping
# ---------------------------------------------------------------------------

def extract_article_links(page, section_url: str, section_name: str, logger) -> list[dict]:
    """Navigate to section page, render JS, extract article links."""
    logger.info(f"  Navigating to {section_url}")

    try:
        page.goto(section_url, wait_until="networkidle", timeout=30000)
    except Exception as e:
        logger.warning(f"  Page load timeout/error for {section_url}: {e}")
        try:
            page.goto(section_url, wait_until="domcontentloaded", timeout=15000)
        except Exception as e2:
            logger.error(f"  Cannot load {section_url}: {e2}")
            return []

    # Wait a moment for any dynamic content
    time.sleep(2)

    links = []
    parsed_base = urlparse(section_url)

    # Strategy 1: Look for article-like links in the main content area
    # Granicus OpenCities typically uses content areas with article links
    selectors = [
        "main a[href]",
        ".content-area a[href]",
        "#content a[href]",
        "article a[href]",
        ".news-list a[href]",
        ".view-content a[href]",
        # Broader fallback
        "a[href*='/News/']",
        "a[href*='/Announcements/']",
        "a[href*='/Public-Notices/']",
    ]

    seen_urls = set()
    for selector in selectors:
        try:
            elements = page.query_selector_all(selector)
            for el in elements:
                href = el.get_attribute("href")
                if not href:
                    continue

                # Resolve relative URLs
                full_url = urljoin(section_url, href)

                # Filter: must be on syr.gov and look like a content page
                parsed = urlparse(full_url)
                if "syr.gov" not in parsed.netloc:
                    continue

                # Skip navigation, footer, external links
                path = parsed.path.lower()
                if path in ["/", "/news", "/announcements", "/public-notices"]:
                    continue
                if path.startswith("/search") or path.startswith("/user"):
                    continue

                # Must be a subpage of a content section
                content_paths = ["/news/", "/announcements/", "/public-notices/"]
                if not any(path.startswith(cp) for cp in content_paths):
                    continue

                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                # Skip category/index pages (no date slug in URL)
                if not is_article_url(full_url):
                    continue

                # Get title from link text
                link_text = el.inner_text().strip()
                if not link_text or len(link_text) < 5:
                    continue

                # Try to find a date near the link
                date_hint = None
                parent = el.evaluate("el => el.parentElement ? el.parentElement.innerText : ''")
                if parent:
                    date_hint = parse_syr_gov_date(parent)

                links.append({
                    "url": full_url,
                    "title": link_text,
                    "date_hint": date_hint,
                })
        except Exception:
            continue

    logger.info(f"  Found {len(links)} article links in {section_name}")
    return links


def extract_article_content(page, url: str, logger) -> dict | None:
    """Navigate to article page, render JS, extract content."""
    logger.debug(f"  Fetching: {url}")

    try:
        page.goto(url, wait_until="networkidle", timeout=30000)
    except Exception:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=15000)
        except Exception as e:
            logger.warning(f"  Cannot load {url}: {e}")
            return None

    time.sleep(1)

    # Extract title
    title = None
    for selector in ["h1", ".page-title", ".node-title", "article h1", "main h1"]:
        try:
            el = page.query_selector(selector)
            if el:
                title = el.inner_text().strip()
                if title:
                    break
        except Exception:
            continue

    if not title:
        title = page.title().strip()

    # Extract date
    pub_date = None
    # Look for date elements
    for selector in [
        "time", ".date", ".post-date", ".field-date",
        ".node-date", "span.date", ".created", ".published",
    ]:
        try:
            el = page.query_selector(selector)
            if el:
                date_text = el.inner_text().strip()
                pub_date = parse_syr_gov_date(date_text)
                if pub_date:
                    break
                # Check datetime attribute
                dt_attr = el.get_attribute("datetime")
                if dt_attr:
                    try:
                        pub_date = datetime.fromisoformat(dt_attr.replace("Z", "+00:00")).strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        pass
        except Exception:
            continue

    # Fallback 1: parse date from URL slug (most reliable for syr.gov)
    if not pub_date:
        pub_date = parse_date_from_url(url)

    # Fallback 2: try to find date in page text
    if not pub_date:
        body_text = page.inner_text("body")
        pub_date = parse_syr_gov_date(body_text[:500])

    # Last resort: today's date
    if not pub_date:
        pub_date = datetime.now().strftime("%Y-%m-%d")

    # Extract main content
    content = ""
    for selector in [
        "article",
        ".node-content",
        ".field-body",
        ".content-area",
        "main .content",
        "#content",
        "main",
    ]:
        try:
            el = page.query_selector(selector)
            if el:
                content = el.inner_text().strip()
                if len(content) > 50:
                    break
        except Exception:
            continue

    if not content or len(content) < 20:
        # Last resort: get all body text
        try:
            content = page.inner_text("body").strip()
        except Exception:
            logger.warning(f"  Could not extract content from {url}")
            return None

    return {
        "url": url,
        "title": title or "Untitled",
        "publication_date": pub_date,
        "content": content,
    }


# ---------------------------------------------------------------------------
# Article processing pipeline
# ---------------------------------------------------------------------------

def process_web_item(
    item: dict,
    source_type: str,
    registry: dict,
    taxonomy: dict,
    api_config: dict,
    schema: dict,
    valid_tags: set[str],
    existing_records: list[dict],
    next_seq: int,
    logger,
    dry_run: bool = False,
) -> tuple[dict | None, int]:
    """Process a single web page through the full pipeline."""
    title = item["title"]
    content = item["content"]
    combined_text = title + " " + content

    # Mechanical extraction
    mech_entities = extract_entities(combined_text, registry)
    mech_departments = extract_departments(combined_text, taxonomy)
    mech_systems = extract_systems(combined_text, taxonomy)
    mech_amounts = extract_amounts(combined_text)

    if dry_run:
        logger.info(f"  [DRY RUN] Would process: {title[:80]}")
        logger.info(f"    Entities: {[e['name'] for e in mech_entities]}")
        logger.info(f"    Departments: {mech_departments}")
        return None, next_seq

    # Claude enrichment
    enrichment = enrich_with_claude(
        title=title,
        publication_date=item["publication_date"],
        author=None,
        source_url=item["url"],
        content=content,
        source_type=source_type,
        taxonomy=taxonomy,
        registry=registry,
        api_config=api_config,
        mechanical_entities=mech_entities + mech_systems,
        mechanical_departments=mech_departments,
        mechanical_amounts=mech_amounts,
        logger=logger,
    )

    # Build record
    year = datetime.now().year
    record_id = f"CTX-WEB-{year}-{next_seq:05d}"
    freshness = get_freshness_class(source_type, taxonomy)

    record = build_context_record(
        record_id=record_id,
        source_agent="website_monitor",
        source_type=source_type,
        source_url=item["url"],
        publication_date=item["publication_date"],
        title=title,
        enrichment=enrichment,
        freshness_class=freshness,
    )

    # Validate
    tag_errors = validate_tags(record, valid_tags)
    if tag_errors:
        for err in tag_errors:
            logger.warning(f"    Tag issue: {err}")
            record["processing_notes"].append(f"Tag warning: {err}")

    schema_errors = validate_record(record, schema)
    if schema_errors:
        for err in schema_errors:
            logger.error(f"    Schema error: {err}")
        if not record.get("summary"):
            record["summary"] = f"[Auto-generated] {title}"
        if not record.get("topic_tags"):
            record["topic_tags"] = ["announcement"]
        schema_errors = validate_record(record, schema)
        if schema_errors:
            logger.error(f"    Cannot fix schema errors for {title[:60]} — skipping")
            return None, next_seq

    # Dedup
    dup_id = check_duplicate(record, existing_records)
    if dup_id:
        logger.info(f"    Dedup match: {dup_id} — linking via cluster")
        record["cluster_ids"].append(f"DEDUP-{dup_id}")
        record["processing_notes"].append(
            f"Potential duplicate of {dup_id} — linked for review"
        )

    # Archive
    safe_date = item["publication_date"].replace("-", "")
    archive_name = f"{safe_date}_{record_id}.txt"
    archive_raw_content(content, archive_name, WEB_CONTENT_DIR)

    # Save
    saved_path = save_record(record)
    logger.info(f"    Saved: {record_id} → {saved_path.name}")

    return record, next_seq + 1


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = set(sys.argv[1:])
    skip_email = "--no-email" in args
    dry_run = "--dry-run" in args
    force_all = "--force-all" in args
    backfill = "--backfill" in args

    logger = setup_logger("website-monitor")
    logger.info("=" * 60)
    logger.info(f"Website Monitor starting{' (backfill mode)' if backfill else ''}")

    # Load configs
    try:
        taxonomy = load_taxonomy()
        registry = load_entity_registry()
        schema = load_schema()
        api_config = load_api_config()
    except Exception as e:
        logger.error(f"Config load failed: {e}")
        print(f"Error: {e}")
        sys.exit(1)

    valid_tags = get_valid_tags(taxonomy)

    # Check Playwright availability
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        msg = "Playwright not installed. Run: python -m pip install playwright && python -m playwright install chromium"
        logger.error(msg)
        print(f"Error: {msg}")
        sys.exit(1)

    # Load state
    state = load_state(STATE_PATH)
    processed_urls = set(state.get("processed_urls", [])) if not force_all else set()

    # Load existing records for dedup
    existing_records = load_existing_records()
    year = datetime.now().year
    next_seq = get_next_sequence("WEB", year)

    saved_count = 0
    skipped_count = 0
    error_count = 0
    new_urls = []
    saved_titles = []

    pages_to_scan = list(TARGET_PAGES)
    if backfill:
        pages_to_scan.extend(BACKFILL_PAGES_2026)
        logger.info(f"Backfill mode: scanning {len(pages_to_scan)} sections total")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="SyracuseContextEngine/1.0 (City of Syracuse Office of Analytics)"
        )
        page = context.new_page()

        for section in pages_to_scan:
            logger.info(f"Scanning: {section['name']} ({section['url']})")

            try:
                links = extract_article_links(page, section["url"], section["name"], logger)
            except Exception as e:
                logger.error(f"  Failed to extract links from {section['url']}: {e}")
                error_count += 1
                continue

            time.sleep(CRAWL_DELAY)

            for link_info in links:
                url = link_info["url"]
                if url in processed_urls:
                    skipped_count += 1
                    continue

                try:
                    item = extract_article_content(page, url, logger)
                    if not item:
                        new_urls.append(url)
                        continue

                    time.sleep(CRAWL_DELAY)

                    record, next_seq = process_web_item(
                        item, section["source_type"],
                        registry, taxonomy, api_config, schema,
                        valid_tags, existing_records, next_seq,
                        logger, dry_run,
                    )

                    new_urls.append(url)
                    if record:
                        saved_count += 1
                        saved_titles.append(record["title"][:80])
                        existing_records.append(record)

                except Exception as e:
                    logger.error(f"  Error processing {url}: {e}")
                    error_count += 1
                    new_urls.append(url)

        browser.close()

    # Update state (cap URLs at 1000) — skip in dry-run mode
    if not dry_run:
        all_urls = list(processed_urls) + new_urls
        if len(all_urls) > 1000:
            all_urls = all_urls[-1000:]

        save_state(STATE_PATH, {
            "last_run": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "processed_urls": all_urls,
            "total_processed": state.get("total_processed", 0) + len(new_urls),
            "total_saved": state.get("total_saved", 0) + saved_count,
        })

    # Summary
    summary = (
        f"Website Monitor Run — {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
        f"{'=' * 50}\n"
        f"Sections scanned: {len(pages_to_scan)}\n"
        f"New pages found: {len(new_urls)}\n"
        f"Saved as records: {saved_count}\n"
        f"Already processed: {skipped_count}\n"
        f"Errors: {error_count}\n"
    )
    if saved_titles:
        summary += "\nNew records:\n"
        for t in saved_titles:
            summary += f"  • {t}\n"

    logger.info(summary)
    print(summary)

    # Email
    if not skip_email and saved_count > 0:
        email_config = load_email_config()
        if email_config:
            today = datetime.now().strftime("%Y-%m-%d")
            send_collector_email(
                email_config, "Website Monitor",
                f"Daily Report — {today} — {saved_count} new records",
                summary,
            )
            logger.info("Summary email sent.")


if __name__ == "__main__":
    main()
