"""
import_links.py — Batch URL Importer for the Context Engine

Reads a CSV of news article URLs (from email archive), fetches each article
via Playwright, and processes through the standard Context Engine pipeline.

Usage:
    python scripts/import_links.py --login            # open browser to log into Syracuse.com
    python scripts/import_links.py --retry-blocked    # re-attempt previously blocked URLs
    python scripts/import_links.py --dry-run           # preview what would be captured
    python scripts/import_links.py --no-email          # run without email summary
    python scripts/import_links.py --limit 20          # process only first N new URLs
    python scripts/import_links.py --skip-blocked      # skip syracuse.com URLs entirely

Requires: pip install playwright && playwright install chromium
"""

import csv
import json as _json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from html.parser import HTMLParser
from urllib.request import urlopen, Request

from playwright.sync_api import sync_playwright

from collector_utils import (
    PROJECT_ROOT,
    NEWS_ARTICLES_DIR,
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
    is_relevant,
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
# Configuration
# ---------------------------------------------------------------------------

CSV_PATH = PROJECT_ROOT / "cmuldoon emails input" / "filtered_weblinks.csv"
STATE_PATH = PROJECT_ROOT / "outputs" / "link-importer-state.json"
SESSION_PATH = PROJECT_ROOT / "outputs" / "syracuse-session.json"  # legacy
BROWSER_PROFILE_DIR = PROJECT_ROOT / "outputs" / "browser-profile"

CRAWL_DELAY = 3  # seconds between same-domain requests
CONTENT_MIN_LENGTH = 100  # minimum chars to consider content valid

# Domains to skip entirely (not city government news)
SKIP_DOMAINS = {
    "obits.syracuse.com",
    "www.tasteofsyracuse.com",
    "www.outsyracuse.com",
}

# Date pattern in URLs: /YYYY/MM/ or /YYYY-MM-DD
URL_DATE_PATTERNS = [
    re.compile(r"/(\d{4})/(\d{2})/"),  # /2025/03/
    re.compile(r"/(\d{4})-(\d{2})-(\d{2})"),  # /2025-03-15
]


# ---------------------------------------------------------------------------
# URL processing
# ---------------------------------------------------------------------------

def load_urls(csv_path: Path) -> list[str]:
    """Load and deduplicate URLs from CSV file."""
    urls = []
    seen = set()
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("url", "").strip().rstrip("#")
            if url and url not in seen:
                seen.add(url)
                urls.append(url)
    return urls


def get_domain(url: str) -> str:
    """Extract domain from URL."""
    parsed = urlparse(url)
    return parsed.netloc.lower()


def should_skip_domain(url: str) -> bool:
    """Check if URL domain should be skipped."""
    domain = get_domain(url)
    return domain in SKIP_DOMAINS


def is_syracuse_com(url: str) -> bool:
    """Check if URL is from syracuse.com."""
    domain = get_domain(url)
    return "syracuse.com" in domain and "obits" not in domain


def get_wayback_url(url: str, logger=None) -> str | None:
    """Check the Wayback Machine for an archived copy of a URL.

    Returns the archived URL or None.
    """
    api = f"https://archive.org/wayback/available?url={url}"
    try:
        req = Request(api, headers={"User-Agent": "ContextEngine/1.0"})
        resp = urlopen(req, timeout=10)
        data = _json.loads(resp.read())
        snap = data.get("archived_snapshots", {}).get("closest", {})
        if snap.get("available") and snap.get("status") == "200":
            wb_url = snap["url"]
            if logger:
                logger.debug(f"  Wayback hit: {wb_url[:80]}")
            return wb_url
    except Exception as e:
        if logger:
            logger.debug(f"  Wayback lookup failed: {e}")
    return None


class _TextExtractor(HTMLParser):
    """Minimal HTML→text extractor using only the standard library."""

    SKIP_TAGS = {"script", "style", "noscript", "nav", "header", "footer", "aside"}

    def __init__(self):
        super().__init__()
        self._pieces: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag in self.SKIP_TAGS:
            self._skip_depth += 1

    def handle_endtag(self, tag):
        if tag in self.SKIP_TAGS and self._skip_depth > 0:
            self._skip_depth -= 1

    def handle_data(self, data):
        if self._skip_depth == 0:
            text = data.strip()
            if text:
                self._pieces.append(text)

    def get_text(self) -> str:
        return "\n".join(self._pieces)


def fetch_wayback_content(wb_url: str, original_url: str, logger=None) -> dict | None:
    """Fetch article content from a Wayback Machine URL using urllib.

    Playwright gets blocked by archive.org, so we use plain HTTP instead.
    Returns the same dict format as extract_article(), or None.
    """
    try:
        req = Request(wb_url, headers={
            "User-Agent": "Mozilla/5.0 (Context Engine research bot)",
        })
        resp = urlopen(req, timeout=20)
        html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        if logger:
            logger.debug(f"  Wayback fetch failed: {e}")
        return None

    if len(html) < 500:
        return None

    # --- Title ---
    title = None
    m = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)', html, re.I)
    if m:
        title = m.group(1).strip()
    if not title:
        m = re.search(r"<title[^>]*>([^<]+)</title>", html, re.I)
        if m:
            title = m.group(1).strip()
    if not title or len(title) < 5:
        return None

    # Clean title (remove site name suffixes)
    for sep in [" | ", " - ", " — ", " – "]:
        if sep in title:
            parts = title.split(sep)
            title = max(parts, key=len).strip()

    # --- Author ---
    author = None
    m = re.search(r'<meta[^>]+name=["\']author["\'][^>]+content=["\']([^"\']+)', html, re.I)
    if m:
        author = m.group(1).strip()

    # --- Publication date ---
    pub_date = None
    for attr in ["article:published_time", "date", "publish-date"]:
        m = re.search(rf'<meta[^>]+(?:property|name)=["\']{ re.escape(attr) }["\'][^>]+content=["\']([^"\']+)', html, re.I)
        if m:
            try:
                pub_date = m.group(1)[:10]
                datetime.strptime(pub_date, "%Y-%m-%d")
                break
            except ValueError:
                pub_date = None
    if not pub_date:
        m = re.search(r'"datePublished"\s*:\s*"([^"]+)"', html)
        if m:
            try:
                pub_date = m.group(1)[:10]
                datetime.strptime(pub_date, "%Y-%m-%d")
            except ValueError:
                pub_date = None
    if not pub_date:
        pub_date = extract_date_from_url(original_url)
    if not pub_date:
        pub_date = datetime.now().strftime("%Y-%m-%d")

    # --- Content (article body) ---
    # Try to isolate article body HTML first
    body_html = html
    for pattern in [
        r'<article[^>]*class="[^"]*article-body[^"]*"[^>]*>(.*?)</article>',
        r'<div[^>]*class="[^"]*article-body[^"]*"[^>]*>(.*?)</div>',
        r'<article[^>]*>(.*?)</article>',
    ]:
        m = re.search(pattern, html, re.I | re.S)
        if m and len(m.group(1)) > 200:
            body_html = m.group(1)
            break

    parser = _TextExtractor()
    parser.feed(body_html)
    content = parser.get_text()

    if len(content) < CONTENT_MIN_LENGTH:
        if logger:
            logger.debug(f"  Wayback content too short ({len(content)} chars)")
        return None

    if len(content) > 10000:
        content = content[:10000]

    return {
        "url": original_url,
        "title": title,
        "author": author,
        "publication_date": pub_date,
        "content": content,
    }


def extract_date_from_url(url: str) -> str | None:
    """Try to extract publication date from URL path."""
    for pattern in URL_DATE_PATTERNS:
        match = pattern.search(url)
        if match:
            groups = match.groups()
            if len(groups) == 3:
                y, m, d = groups
                try:
                    datetime(int(y), int(m), int(d))
                    return f"{y}-{m}-{d}"
                except ValueError:
                    pass
            elif len(groups) == 2:
                y, m = groups
                try:
                    datetime(int(y), int(m), 1)
                    return f"{y}-{m}-15"  # approximate mid-month
                except ValueError:
                    pass
    return None


# ---------------------------------------------------------------------------
# Content extraction
# ---------------------------------------------------------------------------

def extract_article(page, url: str, logger) -> dict | None:
    """Fetch a URL and extract article content.

    Returns dict with keys: url, title, author, publication_date, content
    or None if extraction fails.
    """
    try:
        response = page.goto(url, wait_until="domcontentloaded", timeout=20000)
        if not response or response.status >= 400:
            logger.debug(f"  HTTP {response.status if response else 'no response'}: {url[:80]}")
            return None

        # Wait a moment for JS to render
        page.wait_for_timeout(2000)

    except Exception as e:
        logger.debug(f"  Navigation error: {url[:80]} — {e}")
        return None

    try:
        # --- Title ---
        title = None

        # Try og:title meta tag (most reliable across sites)
        og_title = page.query_selector('meta[property="og:title"]')
        if og_title:
            title = og_title.get_attribute("content")

        # Fall back to h1
        if not title:
            h1 = page.query_selector("h1")
            if h1:
                title = h1.inner_text().strip()

        # Fall back to <title> tag
        if not title:
            title = page.title()

        if not title or len(title) < 5:
            logger.debug(f"  No title found: {url[:80]}")
            return None

        # Clean up title
        title = title.strip()
        # Remove site name suffixes like " | Syracuse.com" or " - CNYCentral"
        for sep in [" | ", " - ", " — ", " – "]:
            if sep in title:
                parts = title.split(sep)
                # Keep longest part (usually the article title)
                title = max(parts, key=len).strip()

        # --- Author ---
        author = None
        author_meta = page.query_selector('meta[name="author"]')
        if not author_meta:
            author_meta = page.query_selector('meta[property="article:author"]')
        if author_meta:
            author = author_meta.get_attribute("content")
        if not author:
            byline = page.query_selector(".byline, .author, .article-author, [rel='author']")
            if byline:
                author = byline.inner_text().strip()

        # --- Publication Date ---
        pub_date = None

        # Try meta tags
        for selector in [
            'meta[property="article:published_time"]',
            'meta[name="date"]',
            'meta[name="publish-date"]',
            'meta[property="og:article:published_time"]',
        ]:
            meta = page.query_selector(selector)
            if meta:
                date_str = meta.get_attribute("content")
                if date_str:
                    try:
                        pub_date = date_str[:10]  # Take YYYY-MM-DD from ISO format
                        datetime.strptime(pub_date, "%Y-%m-%d")
                        break
                    except ValueError:
                        pub_date = None

        # Try JSON-LD
        if not pub_date:
            try:
                ld_scripts = page.query_selector_all('script[type="application/ld+json"]')
                for script in ld_scripts:
                    text = script.inner_text()
                    if "datePublished" in text:
                        match = re.search(r'"datePublished"\s*:\s*"([^"]+)"', text)
                        if match:
                            pub_date = match.group(1)[:10]
                            datetime.strptime(pub_date, "%Y-%m-%d")
                            break
            except Exception:
                pub_date = None

        # Fall back to URL date
        if not pub_date:
            pub_date = extract_date_from_url(url)

        # Final fallback
        if not pub_date:
            pub_date = datetime.now().strftime("%Y-%m-%d")

        # --- Content ---
        content = None

        # Try common article body selectors (in order of specificity)
        content_selectors = [
            "article .article-body",
            "article .story-body",
            ".article-content",
            ".story-content",
            ".entry-content",
            ".post-content",
            "article .content",
            ".field-body",
            "article",
            "main .content",
            "main",
        ]

        for selector in content_selectors:
            el = page.query_selector(selector)
            if el:
                text = el.inner_text().strip()
                if len(text) >= CONTENT_MIN_LENGTH:
                    content = text
                    break

        # Fall back to body text
        if not content:
            body = page.query_selector("body")
            if body:
                text = body.inner_text().strip()
                if len(text) >= CONTENT_MIN_LENGTH:
                    content = text

        if not content or len(content) < CONTENT_MIN_LENGTH:
            logger.debug(f"  Content too short ({len(content) if content else 0} chars): {url[:80]}")
            return None

        # Truncate very long content (keep first 10K chars for processing)
        if len(content) > 10000:
            content = content[:10000]

        return {
            "url": url,
            "title": title,
            "author": author,
            "publication_date": pub_date,
            "content": content,
        }

    except Exception as e:
        logger.debug(f"  Extraction error: {url[:80]} — {e}")
        return None


# ---------------------------------------------------------------------------
# Login flow
# ---------------------------------------------------------------------------

def do_login(logger):
    """Open a persistent browser profile for the user to log into Syracuse.com.

    Uses launch_persistent_context so cookies, localStorage, and service
    workers are saved to disk automatically — no export/import needed.
    """
    logger.info("Opening browser for Syracuse.com login...")
    print("\n" + "=" * 60)
    print("SYRACUSE.COM LOGIN")
    print("=" * 60)
    print("A browser window will open to Syracuse.com.")
    print("Please log in with your subscriber account.")
    print("When you're done, come back here and press Enter.")
    print("=" * 60 + "\n")

    BROWSER_PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(BROWSER_PROFILE_DIR),
            headless=False,
            accept_downloads=False,
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.goto("https://www.syracuse.com/")

        input("Press Enter after you've logged in to Syracuse.com...")

        context.close()

    logger.info(f"Browser profile saved to {BROWSER_PROFILE_DIR}")
    print(f"\nLogin saved. You can now run the importer without --login.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = set(sys.argv[1:])
    dry_run = "--dry-run" in args
    skip_email = "--no-email" in args
    skip_blocked = "--skip-blocked" in args
    do_login_flag = "--login" in args
    retry_blocked = "--retry-blocked" in args

    # Parse --limit N
    limit = None
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--limit" and i < len(sys.argv) - 1:
            try:
                limit = int(sys.argv[i + 1])
            except ValueError:
                pass

    logger = setup_logger("link-importer")
    logger.info("=" * 60)
    logger.info("Link Importer starting")

    # Handle login flow
    if do_login_flag:
        do_login(logger)
        return

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

    # Load URLs
    if not CSV_PATH.exists():
        logger.error(f"CSV not found: {CSV_PATH}")
        print(f"Error: CSV not found at {CSV_PATH}")
        sys.exit(1)

    all_urls = load_urls(CSV_PATH)
    logger.info(f"Loaded {len(all_urls)} unique URLs from CSV")

    # Filter out skip domains
    urls = [u for u in all_urls if not should_skip_domain(u)]
    skipped_domains = len(all_urls) - len(urls)
    if skipped_domains:
        logger.info(f"Skipped {skipped_domains} URLs from irrelevant domains")

    # Skip syracuse.com if requested
    if skip_blocked:
        before = len(urls)
        urls = [u for u in urls if not is_syracuse_com(u)]
        logger.info(f"Skipped {before - len(urls)} Syracuse.com URLs (--skip-blocked)")

    # Load state
    state = load_state(STATE_PATH)
    processed_urls = set(state.get("processed_urls", []))

    # --retry-blocked: re-attempt Syracuse.com URLs that were blocked before
    if retry_blocked:
        # Find Syracuse.com URLs that were processed but NOT saved as records
        import glob as _glob
        saved_urls = set()
        for fp in _glob.glob(str(PROJECT_ROOT / "context-store" / "CTX-NEWS-*.json")):
            try:
                rec = _json.load(open(fp, encoding="utf-8"))
                src = rec.get("source_url", "")
                if src:
                    saved_urls.add(src)
            except Exception:
                pass
        retry_count = 0
        for u in list(processed_urls):
            if is_syracuse_com(u) and u not in saved_urls:
                processed_urls.discard(u)
                retry_count += 1
        logger.info(f"Retrying {retry_count} previously-blocked Syracuse.com URLs")

    # Filter to unprocessed URLs
    new_urls = [u for u in urls if u not in processed_urls]
    logger.info(f"New URLs to process: {len(new_urls)} (of {len(urls)} total)")

    if limit:
        new_urls = new_urls[:limit]
        logger.info(f"Limited to {limit} URLs")

    if not new_urls:
        logger.info("No new URLs to process.")
        print("No new URLs to process.")
        return

    # Load existing records for dedup
    existing_records = load_existing_records()

    # Counters
    saved_count = 0
    filtered_count = 0
    blocked_count = 0
    wayback_count = 0
    error_count = 0
    processed_in_run = []
    saved_titles = []

    # Check for Syracuse.com login profile
    has_profile = BROWSER_PROFILE_DIR.exists() and any(BROWSER_PROFILE_DIR.iterdir())
    has_session = has_profile or SESSION_PATH.exists()  # support legacy too
    syr_com_count = sum(1 for u in new_urls if is_syracuse_com(u))
    if syr_com_count > 0 and not has_session:
        logger.warning(
            f"{syr_com_count} Syracuse.com URLs but no saved login. "
            f"Run with --login first for subscriber access."
        )
        print(
            f"\nWarning: {syr_com_count} Syracuse.com URLs but no login session.\n"
            f"Run: python scripts/import_links.py --login\n"
            f"to save your subscriber session first.\n"
            f"Proceeding anyway (articles may be blocked).\n"
        )

    # Process URLs
    with sync_playwright() as p:
        # Persistent profile keeps cookies/localStorage/service workers
        if has_profile:
            context = p.chromium.launch_persistent_context(
                user_data_dir=str(BROWSER_PROFILE_DIR),
                headless=False,
                accept_downloads=False,
                args=["--disable-blink-features=AutomationControlled"],
            )
            logger.info("Using persistent browser profile (logged in, visible)")
            page = context.pages[0] if context.pages else context.new_page()
        else:
            browser = p.chromium.launch(headless=True)
            if SESSION_PATH.exists():
                context = browser.new_context(storage_state=str(SESSION_PATH))
                logger.info("Loaded Syracuse.com session (legacy)")
            else:
                context = browser.new_context()
            page = context.new_page()

        last_domain = None

        for i, url in enumerate(new_urls):
            domain = get_domain(url)
            progress = f"[{i+1}/{len(new_urls)}]"

            # Crawl delay
            if last_domain == domain:
                time.sleep(CRAWL_DELAY)
            else:
                time.sleep(1)
            last_domain = domain

            logger.info(f"{progress} Fetching: {url[:100]}")

            # Fetch and extract
            article = extract_article(page, url, logger)

            # If blocked and it's a paywalled domain, try Wayback Machine
            if not article and is_syracuse_com(url):
                wb_url = get_wayback_url(url, logger)
                if wb_url:
                    logger.info(f"{progress} Trying Wayback Machine...")
                    article = fetch_wayback_content(wb_url, url, logger)
                    if article:
                        wayback_count += 1

            if not article:
                logger.info(f"{progress} Blocked/empty: {url[:80]}")
                blocked_count += 1
                processed_in_run.append(url)
                continue

            title = article["title"]
            content = article["content"]
            pub_date = article["publication_date"]

            # Relevance filter
            relevant, reasons = is_relevant(title, content, registry, taxonomy)
            if not relevant:
                logger.debug(f"{progress} Filtered (not relevant): {title[:80]}")
                filtered_count += 1
                processed_in_run.append(url)
                continue

            logger.info(f"{progress} Relevant: {title[:80]}")
            for r in reasons:
                logger.debug(f"  → {r}")

            combined_text = title + " " + content
            mech_entities = extract_entities(combined_text, registry)
            mech_departments = extract_departments(combined_text, taxonomy)
            mech_systems = extract_systems(combined_text, taxonomy)
            mech_amounts = extract_amounts(combined_text)

            if dry_run:
                logger.info(f"{progress} [DRY RUN] Would process: {title[:80]}")
                logger.info(f"  Date: {pub_date} | Entities: {[e['name'] for e in mech_entities]}")
                processed_in_run.append(url)
                continue

            # Claude enrichment
            try:
                enrichment = enrich_with_claude(
                    title=title,
                    publication_date=pub_date,
                    author=article.get("author"),
                    source_url=url,
                    content=content,
                    source_type="news_article",
                    taxonomy=taxonomy,
                    registry=registry,
                    api_config=api_config,
                    mechanical_entities=mech_entities + mech_systems,
                    mechanical_departments=mech_departments,
                    mechanical_amounts=mech_amounts,
                    logger=logger,
                )
            except Exception as e:
                logger.error(f"{progress} Claude enrichment failed: {e}")
                error_count += 1
                processed_in_run.append(url)
                continue

            # Determine year from publication date
            try:
                year = int(pub_date[:4])
            except (ValueError, IndexError):
                year = datetime.now().year

            next_seq = get_next_sequence("NEWS", year)
            record_id = f"CTX-NEWS-{year}-{next_seq:05d}"
            freshness = get_freshness_class("news_article", taxonomy)

            record = build_context_record(
                record_id=record_id,
                source_agent="link_importer",
                source_type="news_article",
                source_url=url,
                publication_date=pub_date,
                title=title,
                enrichment=enrichment,
                freshness_class=freshness,
            )

            # Add provenance note
            record["processing_notes"].append(
                f"Imported from email link archive ({domain})"
            )
            if is_syracuse_com(url) and not has_session:
                record["processing_notes"].append(
                    "Content retrieved via Wayback Machine (archive.org)"
                )

            # Validate
            tag_errors = validate_tags(record, valid_tags)
            if tag_errors:
                for err in tag_errors:
                    logger.warning(f"  Tag issue: {err}")
                    record["processing_notes"].append(f"Tag warning: {err}")

            schema_errors = validate_record(record, schema)
            if schema_errors:
                for err in schema_errors:
                    logger.error(f"  Schema error: {err}")
                if not record.get("summary"):
                    record["summary"] = f"[Auto-generated] {title}"
                if not record.get("topic_tags"):
                    record["topic_tags"] = ["announcement"]
                schema_errors = validate_record(record, schema)
                if schema_errors:
                    logger.error(f"  Cannot fix schema errors — skipping")
                    error_count += 1
                    processed_in_run.append(url)
                    continue

            # Dedup
            dup_id = check_duplicate(record, existing_records)
            if dup_id:
                logger.info(f"  Dedup match: {dup_id}")
                record["cluster_ids"].append(f"DEDUP-{dup_id}")
                record["processing_notes"].append(f"Potential duplicate of {dup_id}")

            # Archive raw content
            safe_date = pub_date.replace("-", "")
            archive_name = f"{safe_date}_{record_id}.txt"
            archive_raw_content(content, archive_name, NEWS_ARTICLES_DIR)

            # Save
            saved_path = save_record(record)
            logger.info(f"  Saved: {record_id} → {saved_path.name}")

            saved_count += 1
            saved_titles.append(f"{title[:70]} ({pub_date})")
            existing_records.append(record)
            processed_in_run.append(url)

        # Close browser/context
        if has_profile:
            context.close()
        else:
            browser.close()

    # Update state
    if not dry_run:
        all_processed = list(processed_urls) + processed_in_run
        save_state(STATE_PATH, {
            "last_run": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "processed_urls": all_processed,
            "total_processed": state.get("total_processed", 0) + len(processed_in_run),
            "total_saved": state.get("total_saved", 0) + saved_count,
            "total_blocked": state.get("total_blocked", 0) + blocked_count,
            "total_filtered": state.get("total_filtered", 0) + filtered_count,
        })

    # Summary
    summary = (
        f"Link Importer Run — {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
        f"{'=' * 50}\n"
        f"URLs in CSV: {len(all_urls)}\n"
        f"Skipped (domain): {skipped_domains}\n"
        f"Already processed: {len(urls) - len(new_urls)}\n"
        f"Attempted this run: {len(processed_in_run)}\n"
        f"Blocked/empty: {blocked_count}\n"
        f"Recovered via Wayback Machine: {wayback_count}\n"
        f"Filtered (not relevant): {filtered_count}\n"
        f"Saved as records: {saved_count}\n"
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
                email_config, "Link Importer",
                f"Batch Import — {today} — {saved_count} new records",
                summary,
            )
            logger.info("Summary email sent.")


if __name__ == "__main__":
    main()
