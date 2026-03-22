"""
scan_news.py — Local News Scanner for the Context Engine

Pulls Syracuse.com RSS feeds, filters for city government relevance,
enriches via Claude Haiku API, and saves context records automatically.

Usage:
    python scripts/scan_news.py                # normal daily run
    python scripts/scan_news.py --no-email     # skip email summary
    python scripts/scan_news.py --dry-run      # show what would be processed
    python scripts/scan_news.py --force-all    # ignore state, process all items in feed

Designed to be called from Windows Task Scheduler on a daily basis.
"""

import sys
import time
from datetime import datetime
from pathlib import Path

import feedparser
from bs4 import BeautifulSoup

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
    is_relevant,
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
# RSS feed configuration
# ---------------------------------------------------------------------------
RSS_FEEDS = [
    {
        "name": "Syracuse.com Politics",
        "url": "https://www.syracuse.com/arc/outboundfeeds/rss/?section=politics",
    },
    {
        "name": "Syracuse.com Government",
        "url": "https://www.syracuse.com/arc/outboundfeeds/rss/?section=government",
    },
    {
        "name": "Syracuse.com Local",
        "url": "https://www.syracuse.com/arc/outboundfeeds/rss/?section=local",
    },
    {
        "name": "Syracuse.com Main",
        "url": "https://www.syracuse.com/arc/outboundfeeds/rss/",
    },
]

STATE_PATH = PROJECT_ROOT / "outputs" / "news-scanner-state.json"


# ---------------------------------------------------------------------------
# RSS parsing
# ---------------------------------------------------------------------------

def extract_text_from_html(html: str) -> str:
    """Strip HTML tags, return clean text."""
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(separator=" ", strip=True)


def parse_rss_date(entry) -> str | None:
    """Parse publication date from RSS entry. Returns YYYY-MM-DD or None."""
    published = entry.get("published_parsed") or entry.get("updated_parsed")
    if published:
        try:
            return time.strftime("%Y-%m-%d", published)
        except (TypeError, ValueError):
            pass
    # Fallback: try parsing published string
    pub_str = entry.get("published", "")
    for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%Y-%m-%dT%H:%M:%S%z"]:
        try:
            return datetime.strptime(pub_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def fetch_all_feeds(logger) -> list[dict]:
    """Fetch all RSS feeds, deduplicate by link, return article list."""
    seen_links = set()
    articles = []

    for feed_config in RSS_FEEDS:
        name = feed_config["name"]
        url = feed_config["url"]
        logger.info(f"Fetching: {name}")

        try:
            feed = feedparser.parse(url)
        except Exception as e:
            logger.error(f"  Failed to fetch {name}: {e}")
            continue

        if feed.bozo and not feed.entries:
            logger.warning(f"  Feed parse error for {name}: {feed.bozo_exception}")
            continue

        count = 0
        for entry in feed.entries:
            link = entry.get("link", "")
            if not link or link in seen_links:
                continue
            seen_links.add(link)

            # Extract full content (prefer content:encoded over description)
            content_html = ""
            if entry.get("content"):
                content_html = entry.content[0].get("value", "")
            if not content_html:
                content_html = entry.get("description", "")

            content_text = extract_text_from_html(content_html) if content_html else ""

            pub_date = parse_rss_date(entry)
            if not pub_date:
                pub_date = datetime.now().strftime("%Y-%m-%d")

            articles.append({
                "guid": entry.get("id", link),
                "title": entry.get("title", "Untitled"),
                "link": link,
                "author": entry.get("author") or entry.get("dc_creator"),
                "publication_date": pub_date,
                "content": content_text,
                "content_html": content_html,
            })
            count += 1

        logger.info(f"  {count} new articles from {name}")

    logger.info(f"Total unique articles across all feeds: {len(articles)}")
    return articles


# ---------------------------------------------------------------------------
# Article processing pipeline
# ---------------------------------------------------------------------------

def process_article(
    article: dict,
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
    """Process a single article through the full pipeline.

    Returns (record_or_None, updated_next_seq).
    """
    title = article["title"]
    content = article["content"]
    combined_text = title + " " + content

    # 1. Relevance filter
    relevant, reasons = is_relevant(title, content, registry, taxonomy)
    if not relevant:
        logger.debug(f"  Filtered (not relevant): {title[:80]}")
        return None, next_seq

    logger.info(f"  Relevant: {title[:80]}")
    for r in reasons:
        logger.debug(f"    → {r}")

    # 2. Mechanical extraction
    mech_entities = extract_entities(combined_text, registry)
    mech_departments = extract_departments(combined_text, taxonomy)
    mech_systems = extract_systems(combined_text, taxonomy)
    mech_amounts = extract_amounts(combined_text)

    if dry_run:
        logger.info(f"  [DRY RUN] Would process: {title[:80]}")
        logger.info(f"    Entities: {[e['name'] for e in mech_entities]}")
        logger.info(f"    Departments: {mech_departments}")
        logger.info(f"    Amounts: {mech_amounts}")
        return None, next_seq

    # 3. Claude enrichment
    enrichment = enrich_with_claude(
        title=title,
        publication_date=article["publication_date"],
        author=article.get("author"),
        source_url=article["link"],
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

    # 4. Build record
    year = datetime.now().year
    record_id = f"CTX-NEWS-{year}-{next_seq:05d}"
    freshness = get_freshness_class("news_article", taxonomy)

    record = build_context_record(
        record_id=record_id,
        source_agent="news_scanner",
        source_type="news_article",
        source_url=article["link"],
        publication_date=article["publication_date"],
        title=title,
        enrichment=enrichment,
        freshness_class=freshness,
    )

    # 5. Validate
    tag_errors = validate_tags(record, valid_tags)
    if tag_errors:
        for err in tag_errors:
            logger.warning(f"    Tag issue: {err}")
            record["processing_notes"].append(f"Tag warning: {err}")

    schema_errors = validate_record(record, schema)
    if schema_errors:
        for err in schema_errors:
            logger.error(f"    Schema error: {err}")
        # Try to fix common issues
        if not record.get("summary"):
            record["summary"] = f"[Auto-generated] {title}"
        if not record.get("topic_tags"):
            record["topic_tags"] = ["announcement"]
        # Re-validate
        schema_errors = validate_record(record, schema)
        if schema_errors:
            logger.error(f"    Cannot fix schema errors for {title[:60]} — skipping")
            return None, next_seq

    # 6. Dedup check
    dup_id = check_duplicate(record, existing_records)
    if dup_id:
        logger.info(f"    Dedup match: {dup_id} — linking via cluster")
        record["cluster_ids"].append(f"DEDUP-{dup_id}")
        record["processing_notes"].append(
            f"Potential duplicate of {dup_id} — linked for review"
        )

    # 7. Archive raw content
    safe_date = article["publication_date"].replace("-", "")
    archive_name = f"{safe_date}_{record_id}.txt"
    archive_raw_content(content, archive_name, NEWS_ARTICLES_DIR)

    # 8. Save
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

    logger = setup_logger("news-scanner")
    logger.info("=" * 60)
    logger.info("News Scanner starting")

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
    processed_guids = set(state.get("processed_guids", [])) if not force_all else set()

    # Fetch feeds
    articles = fetch_all_feeds(logger)

    if not articles:
        logger.info("No articles fetched. Exiting.")
        return

    # Filter to new articles
    new_articles = [a for a in articles if a["guid"] not in processed_guids]
    logger.info(f"New articles (not previously processed): {len(new_articles)}")

    if not new_articles:
        logger.info("No new articles. Updating state and exiting.")
        save_state(STATE_PATH, {
            "last_run": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "processed_guids": list(processed_guids),
            "total_processed": state.get("total_processed", 0),
            "total_saved": state.get("total_saved", 0),
        })
        return

    # Load existing records for dedup
    existing_records = load_existing_records()
    year = datetime.now().year
    next_seq = get_next_sequence("NEWS", year)

    # Process articles
    saved_count = 0
    filtered_count = 0
    error_count = 0
    new_guids = []
    saved_titles = []

    for article in new_articles:
        try:
            record, next_seq = process_article(
                article, registry, taxonomy, api_config, schema,
                valid_tags, existing_records, next_seq, logger, dry_run,
            )
            new_guids.append(article["guid"])
            if record:
                saved_count += 1
                saved_titles.append(record["title"][:80])
                existing_records.append(record)
            else:
                filtered_count += 1
        except Exception as e:
            logger.error(f"Error processing '{article.get('title', '?')[:60]}': {e}")
            error_count += 1
            new_guids.append(article["guid"])

    # Update state (cap GUIDs at 2000) — skip in dry-run mode
    if not dry_run:
        all_guids = list(processed_guids) + new_guids
        if len(all_guids) > 2000:
            all_guids = all_guids[-2000:]

        save_state(STATE_PATH, {
            "last_run": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "processed_guids": all_guids,
            "total_processed": state.get("total_processed", 0) + len(new_articles),
            "total_saved": state.get("total_saved", 0) + saved_count,
        })

    # Summary
    summary = (
        f"News Scanner Run — {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
        f"{'=' * 50}\n"
        f"Articles in feeds: {len(articles)}\n"
        f"New (not seen before): {len(new_articles)}\n"
        f"Saved as records: {saved_count}\n"
        f"Filtered (not relevant): {filtered_count}\n"
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
                email_config, "News Scanner",
                f"Daily Report — {today} — {saved_count} new records",
                summary,
            )
            logger.info("Summary email sent.")


if __name__ == "__main__":
    main()
