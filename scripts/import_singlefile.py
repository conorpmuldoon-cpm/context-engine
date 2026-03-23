"""
import_singlefile.py — Import SingleFile-saved HTML pages into Context Engine

Reads .html files saved by the SingleFile browser extension, extracts article
content, and processes through the standard Claude enrichment pipeline.

Usage:
    python scripts/import_singlefile.py                    # process all .html in singlefile-input/
    python scripts/import_singlefile.py --dir ~/Downloads   # custom input directory
    python scripts/import_singlefile.py --dry-run           # preview without saving
    python scripts/import_singlefile.py --limit 5           # process first N files
    python scripts/import_singlefile.py --no-email          # skip email summary
"""

import json
import re
import sys
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

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

DEFAULT_INPUT_DIR = PROJECT_ROOT / "singlefile-input"
STATE_PATH = PROJECT_ROOT / "outputs" / "singlefile-importer-state.json"
CONTENT_MIN_LENGTH = 100


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------

class _TextExtractor(HTMLParser):
    """Minimal HTML->text extractor using only the standard library."""

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


def extract_from_html(html: str, filename: str) -> dict | None:
    """Extract article metadata and content from a SingleFile HTML page.

    Returns dict with: url, title, author, publication_date, content
    or None if extraction fails.
    """

    # --- Source URL ---
    # SingleFile embeds the original URL in a comment near the top:
    #   url: https://www.syracuse.com/...
    # Also check for <link rel="canonical">
    url = None
    m = re.search(r'^\s*url:\s*(https?://[^\s]+)', html, re.M)
    if m:
        url = m.group(1).strip()
    if not url:
        m = re.search(r'saved from url=\(\d+\)(https?://[^\s]+)', html)
        if m:
            url = m.group(1).strip()
    if not url:
        m = re.search(r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)', html, re.I)
        if m:
            url = m.group(1).strip()
    if not url:
        m = re.search(r'<meta[^>]+property=["\']og:url["\'][^>]+content=["\']([^"\']+)', html, re.I)
        if m:
            url = m.group(1).strip()

    # --- Title ---
    title = None
    m = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)', html, re.I)
    if m:
        title = m.group(1).strip()
    if not title:
        m = re.search(r"<title[^>]*>([^<]+)</title>", html, re.I)
        if m:
            title = m.group(1).strip()
    if not title:
        # Fall back to filename
        title = Path(filename).stem.replace("_", " ")

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
    if not author:
        m = re.search(r'<meta[^>]+property=["\']article:author["\'][^>]+content=["\']([^"\']+)', html, re.I)
        if m:
            author = m.group(1).strip()

    # --- Publication date ---
    pub_date = None
    for attr in ["article:published_time", "date", "publish-date"]:
        m = re.search(
            rf'<meta[^>]+(?:property|name)=["\']{ re.escape(attr) }["\'][^>]+content=["\']([^"\']+)',
            html, re.I,
        )
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
        # Try URL date pattern
        if url:
            m = re.search(r"/(\d{4})/(\d{2})/", url)
            if m:
                y, mo = m.groups()
                pub_date = f"{y}-{mo}-15"
    if not pub_date:
        pub_date = datetime.now().strftime("%Y-%m-%d")

    # --- Content ---
    # Try to isolate article body
    body_html = html
    for pattern in [
        r'<article[^>]*class="[^"]*article-body[^"]*"[^>]*>(.*?)</article>',
        r'<div[^>]*class="[^"]*article-body[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*class="[^"]*entry-content[^"]*"[^>]*>(.*?)</div>',
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
        return None

    if len(content) > 10000:
        content = content[:10000]

    return {
        "url": url or "",
        "title": title,
        "author": author,
        "publication_date": pub_date,
        "content": content,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = sys.argv[1:]
    dry_run = "--dry-run" in args
    skip_email = "--no-email" in args

    # Parse --dir
    input_dir = DEFAULT_INPUT_DIR
    if "--dir" in args:
        idx = args.index("--dir")
        if idx + 1 < len(args):
            input_dir = Path(args[idx + 1])

    # Parse --limit
    limit = None
    if "--limit" in args:
        idx = args.index("--limit")
        if idx + 1 < len(args):
            try:
                limit = int(args[idx + 1])
            except ValueError:
                pass

    logger = setup_logger("singlefile-importer")
    logger.info("=" * 60)
    logger.info("SingleFile Importer starting")

    # Check input directory
    if not input_dir.exists():
        input_dir.mkdir(parents=True, exist_ok=True)
        print(f"Created input directory: {input_dir}")
        print(f"Save your SingleFile .html pages there, then re-run.")
        return

    html_files = sorted(input_dir.glob("*.html"))
    if not html_files:
        print(f"No .html files found in {input_dir}")
        print(f"Save your SingleFile pages there, then re-run.")
        return

    logger.info(f"Found {len(html_files)} HTML files in {input_dir}")

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

    # Load state
    state = load_state(STATE_PATH)
    processed_files = set(state.get("processed_files", []))

    # Filter to unprocessed files
    new_files = [f for f in html_files if f.name not in processed_files]
    logger.info(f"New files to process: {len(new_files)} (of {len(html_files)} total)")

    if limit:
        new_files = new_files[:limit]
        logger.info(f"Limited to {limit} files")

    if not new_files:
        print("No new files to process.")
        return

    # Load existing records for dedup
    existing_records = load_existing_records()

    # Counters
    saved_count = 0
    skipped_count = 0
    filtered_count = 0
    error_count = 0
    processed_in_run = []
    saved_titles = []

    for i, filepath in enumerate(new_files):
        progress = f"[{i+1}/{len(new_files)}]"
        logger.info(f"{progress} Processing: {filepath.name}")

        # Read HTML
        try:
            html = filepath.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            logger.error(f"{progress} Read error: {e}")
            error_count += 1
            processed_in_run.append(filepath.name)
            continue

        # Extract article
        article = extract_from_html(html, filepath.name)
        if not article:
            logger.info(f"{progress} Could not extract content — skipping")
            skipped_count += 1
            processed_in_run.append(filepath.name)
            continue

        title = article["title"]
        content = article["content"]
        pub_date = article["publication_date"]
        url = article["url"]

        logger.info(f"{progress} Extracted: {title[:80]}")
        logger.info(f"  URL: {url[:80] if url else '(none)'}")
        logger.info(f"  Date: {pub_date} | Content: {len(content)} chars")

        # Skip relevance filter — user manually curated these files
        reasons = []

        for r in reasons:
            logger.debug(f"  -> {r}")

        combined_text = title + " " + content
        mech_entities = extract_entities(combined_text, registry)
        mech_departments = extract_departments(combined_text, taxonomy)
        mech_systems = extract_systems(combined_text, taxonomy)
        mech_amounts = extract_amounts(combined_text)

        if dry_run:
            logger.info(f"{progress} [DRY RUN] Would save: {title[:80]}")
            logger.info(f"  Entities: {[e['name'] for e in mech_entities]}")
            processed_in_run.append(filepath.name)
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
            processed_in_run.append(filepath.name)
            continue

        # Build record
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

        record["processing_notes"].append(
            "Imported from SingleFile HTML (subscriber content)"
        )

        # Validate tags
        tag_errors = validate_tags(record, valid_tags)
        if tag_errors:
            for err in tag_errors:
                logger.warning(f"  Tag issue: {err}")
                record["processing_notes"].append(f"Tag warning: {err}")

        # Validate schema
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
                logger.error(f"  Cannot fix schema errors - skipping")
                error_count += 1
                processed_in_run.append(filepath.name)
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
        logger.info(f"  Saved: {record_id} -> {saved_path.name}")

        saved_count += 1
        saved_titles.append(f"{title[:70]} ({pub_date})")
        existing_records.append(record)
        processed_in_run.append(filepath.name)

    # Update state
    if not dry_run:
        all_processed = list(processed_files) + processed_in_run
        save_state(STATE_PATH, {
            "last_run": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "processed_files": all_processed,
            "total_processed": state.get("total_processed", 0) + len(processed_in_run),
            "total_saved": state.get("total_saved", 0) + saved_count,
        })

    # Summary
    summary = (
        f"SingleFile Importer — {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
        f"{'=' * 50}\n"
        f"HTML files found: {len(html_files)}\n"
        f"Already processed: {len(html_files) - len(new_files)}\n"
        f"Attempted this run: {len(processed_in_run)}\n"
        f"Could not extract: {skipped_count}\n"
        f"Filtered (not relevant): {filtered_count}\n"
        f"Saved as records: {saved_count}\n"
        f"Errors: {error_count}\n"
    )
    if saved_titles:
        summary += "\nNew records:\n"
        for t in saved_titles:
            summary += f"  * {t}\n"

    logger.info(summary)
    print(summary)

    # Email
    if not skip_email and saved_count > 0:
        email_config = load_email_config()
        if email_config:
            today = datetime.now().strftime("%Y-%m-%d")
            send_collector_email(
                email_config, "SingleFile Importer",
                f"SingleFile Import — {today} — {saved_count} new records",
                summary,
            )
            logger.info("Summary email sent.")


if __name__ == "__main__":
    main()
