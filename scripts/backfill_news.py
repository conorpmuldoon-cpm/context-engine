"""
backfill_news.py — Syracuse.com News Catch-Up for the Context Engine

Fetches up to 100 recent articles per RSS feed section (vs ~50 for the
daily scanner) and processes any that the daily scanner missed. Useful
when the daily scanner didn't run for a day or two.

NOTE: Syracuse.com RSS feeds only serve recent articles (~1-2 days).
Historical backfill for older articles is not possible via RSS or web
scraping (Syracuse.com blocks headless browsers). For historical city
government context, use the syr.gov website monitor backfill instead.

Usage:
    python scripts/backfill_news.py --dry-run      # preview what would be captured
    python scripts/backfill_news.py --no-email      # run without email summary
    python scripts/backfill_news.py --force-all     # ignore state, reprocess everything

Requires: pip install feedparser beautifulsoup4
"""

import re
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import feedparser
except ImportError:
    feedparser = None

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

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

# RSS feeds with size=100 for deeper catch-up
RSS_FEEDS = [
    {
        "name": "Syracuse.com Politics",
        "url": "https://www.syracuse.com/arc/outboundfeeds/rss/?section=politics&size=100",
    },
    {
        "name": "Syracuse.com Government",
        "url": "https://www.syracuse.com/arc/outboundfeeds/rss/?section=government&size=100",
    },
    {
        "name": "Syracuse.com Local",
        "url": "https://www.syracuse.com/arc/outboundfeeds/rss/?section=local&size=100",
    },
]

STATE_PATH = PROJECT_ROOT / "outputs" / "news-backfill-state.json"


# ---------------------------------------------------------------------------
# RSS parsing (same approach as scan_news.py)
# ---------------------------------------------------------------------------

def parse_rss_articles(feed_url: str, feed_name: str, logger) -> list[dict]:
    """Parse RSS feed and extract article data."""
    logger.info(f"Fetching: {feed_name}")

    feed = feedparser.parse(feed_url)
    if feed.bozo and not feed.entries:
        logger.warning(f"  Feed error: {feed.bozo_exception}")
        return []

    articles = []
    for entry in feed.entries:
        # Extract GUID
        guid = entry.get("id") or entry.get("link", "")
        if not guid:
            continue

        # Extract title
        title = entry.get("title", "").strip()
        if not title:
            continue

        # Extract URL
        url = entry.get("link", "")

        # Extract author
        author = entry.get("author", None)

        # Extract publication date
        pub_date = None
        published = entry.get("published_parsed") or entry.get("updated_parsed")
        if published:
            try:
                pub_date = time.strftime("%Y-%m-%d", published)
            except Exception:
                pass
        if not pub_date:
            pub_date = datetime.now().strftime("%Y-%m-%d")

        # Extract content (prefer content:encoded, fall back to description)
        content = ""
        if "content" in entry:
            for c in entry["content"]:
                if c.get("value"):
                    content = c["value"]
                    break

        if not content:
            content = entry.get("summary", "") or entry.get("description", "")

        # Strip HTML tags if present
        if content and BeautifulSoup:
            soup = BeautifulSoup(content, "html.parser")
            content = soup.get_text(separator="\n").strip()

        if not content or len(content) < 50:
            continue

        articles.append({
            "guid": guid,
            "url": url,
            "title": title,
            "author": author,
            "publication_date": pub_date,
            "content": content,
        })

    logger.info(f"  {len(articles)} articles from {feed_name}")
    return articles


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if feedparser is None:
        print("Error: feedparser not installed. Run: pip install feedparser")
        sys.exit(1)

    args = set(sys.argv[1:])
    skip_email = "--no-email" in args
    dry_run = "--dry-run" in args
    force_all = "--force-all" in args

    logger = setup_logger("news-backfill")
    logger.info("=" * 60)
    logger.info("News Backfill (RSS catch-up) starting")

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

    # Also load daily scanner state to skip articles it already processed
    daily_state_path = PROJECT_ROOT / "outputs" / "news-scanner-state.json"
    daily_state = load_state(daily_state_path)
    daily_guids = set(daily_state.get("processed_guids", []))
    all_processed = processed_guids | daily_guids

    # Load existing records for dedup
    existing_records = load_existing_records()
    year = datetime.now().year
    next_seq = get_next_sequence("NEWS", year)

    # Collect all articles from all feeds
    all_articles = []
    seen_guids = set()
    for feed_info in RSS_FEEDS:
        articles = parse_rss_articles(feed_info["url"], feed_info["name"], logger)
        for article in articles:
            if article["guid"] not in seen_guids:
                seen_guids.add(article["guid"])
                all_articles.append(article)

    logger.info(f"Total unique articles across all feeds: {len(all_articles)}")

    # Filter to new articles only
    new_articles = [a for a in all_articles if a["guid"] not in all_processed]
    logger.info(f"New articles (not previously processed by daily or backfill): {len(new_articles)}")

    saved_count = 0
    filtered_count = 0
    error_count = 0
    new_guids = []
    saved_titles = []

    for article in new_articles:
        title = article["title"]
        content = article["content"]

        try:
            # Relevance filter
            relevant, reasons = is_relevant(title, content, registry, taxonomy)
            if not relevant:
                logger.debug(f"  Filtered (not relevant): {title[:80]}")
                filtered_count += 1
                new_guids.append(article["guid"])
                continue

            logger.info(f"  Relevant: {title[:80]}")
            for r in reasons:
                logger.debug(f"    → {r}")

            combined_text = title + " " + content
            mech_entities = extract_entities(combined_text, registry)
            mech_departments = extract_departments(combined_text, taxonomy)
            mech_systems = extract_systems(combined_text, taxonomy)
            mech_amounts = extract_amounts(combined_text)

            if dry_run:
                logger.info(f"  [DRY RUN] Would process: {title[:80]}")
                logger.info(f"    Entities: {[e['name'] for e in mech_entities]}")
                logger.info(f"    Departments: {mech_departments}")
                new_guids.append(article["guid"])
                continue

            # Claude enrichment
            enrichment = enrich_with_claude(
                title=title,
                publication_date=article["publication_date"],
                author=article.get("author"),
                source_url=article["url"],
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

            record_id = f"CTX-NEWS-{year}-{next_seq:05d}"
            freshness = get_freshness_class("news_article", taxonomy)

            record = build_context_record(
                record_id=record_id,
                source_agent="news_scanner",
                source_type="news_article",
                source_url=article["url"],
                publication_date=article["publication_date"],
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
                    logger.error(f"    Cannot fix schema errors — skipping")
                    new_guids.append(article["guid"])
                    continue

            # Dedup
            dup_id = check_duplicate(record, existing_records)
            if dup_id:
                logger.info(f"    Dedup match: {dup_id}")
                record["cluster_ids"].append(f"DEDUP-{dup_id}")
                record["processing_notes"].append(f"Potential duplicate of {dup_id}")

            # Archive raw content
            safe_date = article["publication_date"].replace("-", "")
            archive_name = f"{safe_date}_{record_id}.txt"
            archive_raw_content(content, archive_name, NEWS_ARTICLES_DIR)

            # Save
            saved_path = save_record(record)
            logger.info(f"    Saved: {record_id} → {saved_path.name}")

            saved_count += 1
            next_seq += 1
            saved_titles.append(title[:80])
            existing_records.append(record)
            new_guids.append(article["guid"])

        except Exception as e:
            logger.error(f"  Error processing '{title[:60]}': {e}")
            error_count += 1
            new_guids.append(article["guid"])

    # Update state — skip in dry-run mode
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
        f"News Backfill Run — {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
        f"{'=' * 50}\n"
        f"Feeds checked: {len(RSS_FEEDS)}\n"
        f"Total unique articles: {len(all_articles)}\n"
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
                email_config, "News Backfill",
                f"Catch-Up Report — {today} — {saved_count} new records",
                summary,
            )
            logger.info("Summary email sent.")


if __name__ == "__main__":
    main()
