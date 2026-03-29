"""
gen_briefing.py — Automated context briefing generator

Queries context-store, scores relevance using the 6-factor model from
CLAUDE.md, sends top records to Claude Haiku, and writes a markdown briefing.

Usage:
    python scripts/gen_briefing.py --target "Department of Public Works" --days 180
    python scripts/gen_briefing.py --target "lead remediation" --days 90
    python scripts/gen_briefing.py --target "CLUSTER-SHA-GOVERNANCE-CRISIS-2025"
    python scripts/gen_briefing.py --target "police" --dry-run

Designed to be called from GitHub Actions (via briefing-request issues) or locally.
"""

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import anthropic
except ImportError:
    print("Error: anthropic not installed. Run: python -m pip install anthropic")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
CONTEXT_STORE = PROJECT_ROOT / "context-store"
BRIEFINGS_DIR = PROJECT_ROOT / "outputs" / "briefings"

# Reuse collector_utils for config loading
sys.path.insert(0, str(SCRIPT_DIR))
from collector_utils import load_api_config


# ---------------------------------------------------------------------------
# Load records
# ---------------------------------------------------------------------------

def load_all_records() -> list[dict]:
    """Load all context records from context-store/."""
    records = []
    CONTEXT_STORE.mkdir(parents=True, exist_ok=True)
    for path in sorted(CONTEXT_STORE.glob("CTX-*.json")):
        try:
            with open(path, encoding="utf-8") as f:
                records.append(json.load(f))
        except (json.JSONDecodeError, OSError):
            pass
    return records


# ---------------------------------------------------------------------------
# Relevance scoring (6-factor model from CLAUDE.md)
# ---------------------------------------------------------------------------

# Source authority weights
_SOURCE_AUTHORITY = {
    "council_segment": 1.0,
    "budget_document": 1.0,
    "press_release": 0.8,
    "board_minutes": 0.7,
    "public_notice": 0.7,
    "meeting_notes": 0.6,
    "news_article": 0.5,
    "other": 0.3,
}

# Freshness decay classes (days of full relevance)
_FRESHNESS_WINDOWS = {
    "long": 730,    # 24 months
    "medium": 180,  # 6 months
    "short": 42,    # 6 weeks
}


def score_record(record: dict, target: str, cutoff_date: str | None,
                 cluster_sizes: dict[str, int]) -> float:
    """Score a record's relevance to the target using the 6-factor model.

    Factors (from CLAUDE.md):
        Tag overlap         30%
        Department overlap   20%
        Entity mention       20%
        Recency             15%
        Source authority     10%
        Pattern frequency    5%
    """
    target_lower = target.lower()
    target_words = set(re.split(r"[\s\-_/]+", target_lower))

    # --- Factor 1: Tag overlap (30%) ---
    tags = [t.lower() for t in record.get("topic_tags", [])]
    tag_hits = sum(1 for t in tags if target_lower in t or t in target_lower
                   or any(w in t for w in target_words if len(w) > 3))
    tag_score = min(tag_hits / max(len(target_words), 1), 1.0)

    # --- Factor 2: Department overlap (20%) ---
    depts = [d.lower() for d in record.get("department_refs", [])]
    dept_hits = sum(1 for d in depts if target_lower in d or d in target_lower
                    or any(w in d for w in target_words if len(w) > 3))
    dept_score = min(dept_hits, 1.0)

    # --- Factor 3: Entity mention (20%) ---
    entities = record.get("entity_refs", [])
    entity_names = [e["name"].lower() if isinstance(e, dict) else str(e).lower()
                    for e in entities]
    entity_hits = sum(1 for e in entity_names
                      if target_lower in e or e in target_lower
                      or any(w in e for w in target_words if len(w) > 3))
    entity_score = min(entity_hits, 1.0)

    # --- Factor 4: Recency (15%) ---
    try:
        pub_date = datetime.strptime(record.get("publication_date", ""), "%Y-%m-%d")
        now = datetime.now()
        age_days = (now - pub_date).days
        freshness_class = record.get("freshness_class", "medium")
        window = _FRESHNESS_WINDOWS.get(freshness_class, 180)
        if age_days <= window:
            recency_score = 1.0
        else:
            # Linear decay over 2x the window
            recency_score = max(0, 1.0 - (age_days - window) / window)
    except (ValueError, TypeError):
        recency_score = 0.3  # Unknown date gets partial credit

    # --- Factor 5: Source authority (10%) ---
    source_type = record.get("source_type", "other")
    authority_score = _SOURCE_AUTHORITY.get(source_type, 0.3)

    # --- Factor 6: Pattern frequency (5%) ---
    cluster_ids = record.get("cluster_ids", [])
    if cluster_ids:
        max_cluster_size = max(cluster_sizes.get(c, 1) for c in cluster_ids)
        # Normalize: 10+ records in a cluster = max score
        frequency_score = min(max_cluster_size / 10.0, 1.0)
    else:
        frequency_score = 0.0

    # --- Cluster ID direct match bonus ---
    # If target is a cluster ID, records in that cluster get a large boost
    cluster_bonus = 0.0
    for cid in cluster_ids:
        if target_lower in cid.lower() or cid.lower() in target_lower:
            cluster_bonus = 1.0
            break

    # --- Title/summary text match bonus ---
    title = record.get("title", "").lower()
    summary = record.get("summary", "").lower()
    text_hits = sum(1 for w in target_words if len(w) > 3
                    and (w in title or w in summary))
    text_bonus = min(text_hits / max(len(target_words), 1), 1.0) * 0.15

    # Weighted combination
    weighted = (
        tag_score * 0.30
        + dept_score * 0.20
        + entity_score * 0.20
        + recency_score * 0.15
        + authority_score * 0.10
        + frequency_score * 0.05
        + cluster_bonus * 0.30  # Strong boost for cluster-targeted queries
        + text_bonus            # Bonus for title/summary text matches
    )

    # Apply date cutoff filter
    if cutoff_date:
        pub = record.get("publication_date", "")
        if pub and pub < cutoff_date:
            weighted *= 0.1  # Heavy penalty for records outside time range

    return round(weighted, 4)


# ---------------------------------------------------------------------------
# Build Claude prompt
# ---------------------------------------------------------------------------

def build_briefing_prompt(target: str, scored_records: list[tuple[float, dict]],
                          total_records: int) -> str:
    """Build the prompt for Claude to generate a briefing."""

    # Take top 50 records for the prompt
    top_records = scored_records[:50]

    record_lines = []
    for score, rec in top_records:
        rid = rec.get("record_id", "?")
        date = rec.get("publication_date", "?")
        title = rec.get("title", "Untitled")
        summary = (rec.get("summary", "") or "")[:300]
        depts = ", ".join(rec.get("department_refs", []))
        tags = ", ".join(rec.get("topic_tags", []))
        clusters = ", ".join(rec.get("cluster_ids", []))

        signal = rec.get("political_signal")
        signal_str = ""
        if signal and isinstance(signal, dict):
            signal_str = f" | Signal: {signal.get('signal_type', '')} — {signal.get('description', '')[:100]}"

        sentiment = rec.get("sentiment", "")

        record_lines.append(
            f"- {rid} | {date} | Score: {score:.2f} | {title}\n"
            f"  Summary: {summary}\n"
            f"  Dept: {depts} | Tags: {tags} | Sentiment: {sentiment}\n"
            f"  Clusters: {clusters or 'none'}{signal_str}"
        )

    core_count = len(top_records)

    return f"""You are the Context Engine Librarian for the City of Syracuse Chief Innovation Officer.

Generate a Context Briefing on: {target}

RECORDS ({core_count} most relevant of {total_records} total, sorted by relevance score):
{chr(10).join(record_lines)}

Write the briefing in this EXACT markdown format:

# Context Briefing: {target}

**Generated:** {datetime.now().strftime("%Y-%m-%d")}
**Records Reviewed:** {total_records}
**Core Records:** {core_count}
**Relevance Score Range:** {scored_records[-1][0] if scored_records else 0:.2f}–{scored_records[0][0] if scored_records else 0:.2f}

---

## Executive Summary
(2-3 paragraphs synthesizing the political landscape, key players, budget context, and current status. Be specific — cite dollar amounts, dates, and names. Write for a senior government executive, not a programmer.)

---

## Recent Council Activity
(Relevant council segments, votes, committee discussions from the records. Cite record IDs.)

---

## Recent News Coverage
(Key articles, editorial trends, public sentiment from news records. Cite record IDs.)

---

## Budget Context
(Fiscal commitments, grant awards, capital planning from the records. Cite record IDs.)

---

## Active Initiatives
(Press releases, contracts, launches, current programs. Cite record IDs.)

---

## Political Signals
(Table of signal types with counts and key instances from the records.)

| Signal Type | Count | Key Instances |
|-------------|-------|---------------|
| ... | ... | ... |

---

## Open Questions
(Intelligence gaps, items needing follow-up, things the Context Engine couldn't determine.)

IMPORTANT INSTRUCTIONS:
- Cite specific record IDs (e.g., CTX-NEWS-2026-00042) so the CIO can drill into details
- Be direct and practical — no jargon, no filler
- If a section has no relevant data, write "No relevant records in this category." rather than omitting it
- Focus on what the CIO needs to know for decision-making
- Include dollar amounts, dates, and names when available in the records"""


# ---------------------------------------------------------------------------
# Call Claude
# ---------------------------------------------------------------------------

def generate_briefing(prompt: str, api_config: dict) -> str:
    """Send prompt to Claude and return the briefing text."""
    client = anthropic.Anthropic(api_key=api_config["anthropic_api_key"])
    model = api_config.get("model", "claude-haiku-4-5-20251001")

    response = client.messages.create(
        model=model,
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}],
    )

    return response.content[0].text


# ---------------------------------------------------------------------------
# Slug generation
# ---------------------------------------------------------------------------

def make_slug(target: str) -> str:
    """Convert target string to a filename-safe slug."""
    slug = target.lower()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s]+", "-", slug.strip())
    slug = re.sub(r"-+", "-", slug)
    # Remove common prefixes for cleaner filenames
    for prefix in ["department-of-", "city-of-", "office-of-", "syracuse-"]:
        if slug.startswith(prefix):
            slug = slug[len(prefix):]
    return slug[:60]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate a context briefing")
    parser.add_argument("--target", required=True, help="Department, topic, or cluster ID")
    parser.add_argument("--days", type=int, default=365, help="How far back to look (days)")
    parser.add_argument("--dry-run", action="store_true", help="Show prompt without calling Claude")
    parser.add_argument("--min-score", type=float, default=0.10,
                        help="Minimum relevance score to include (default 0.10)")
    args = parser.parse_args()

    today = datetime.now().strftime("%Y-%m-%d")
    cutoff_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    # Load records
    all_records = load_all_records()
    print(f"Loaded {len(all_records)} records from context-store/")

    if not all_records:
        print("No records found. Nothing to brief on.")
        return

    # Build cluster size map for pattern frequency scoring
    cluster_sizes: dict[str, int] = {}
    for rec in all_records:
        for cid in rec.get("cluster_ids", []):
            cluster_sizes[cid] = cluster_sizes.get(cid, 0) + 1

    # Score all records
    scored = []
    for rec in all_records:
        score = score_record(rec, args.target, cutoff_date, cluster_sizes)
        if score >= args.min_score:
            scored.append((score, rec))

    # Sort by relevance (highest first)
    scored.sort(key=lambda x: x[0], reverse=True)

    print(f"Found {len(scored)} relevant records (score >= {args.min_score})")
    if scored:
        print(f"Top score: {scored[0][0]:.2f} ({scored[0][1].get('record_id', '?')})")
        print(f"Score range: {scored[-1][0]:.2f} – {scored[0][0]:.2f}")

    if not scored:
        print(f"No records match target '{args.target}' with score >= {args.min_score}")
        return

    # Build prompt
    prompt = build_briefing_prompt(args.target, scored, len(all_records))

    if args.dry_run:
        print(f"\n--- DRY RUN: Prompt ({len(prompt)} chars) ---\n")
        print(prompt[:3000])
        if len(prompt) > 3000:
            print(f"\n[...truncated, {len(prompt)} total chars]")
        print(f"\n--- Top 10 records ---")
        for score, rec in scored[:10]:
            print(f"  {score:.2f}  {rec.get('record_id', '?')}  {rec.get('title', '')[:60]}")
        return

    # Load API config and generate
    api_config = load_api_config()
    print(f"Sending {min(len(scored), 50)} records to Claude...")

    try:
        briefing_text = generate_briefing(prompt, api_config)
        print("Briefing generated successfully.")
    except Exception as e:
        print(f"Claude API error: {e}", file=sys.stderr)
        sys.exit(1)

    # Save briefing
    BRIEFINGS_DIR.mkdir(parents=True, exist_ok=True)
    slug = make_slug(args.target)
    output_path = BRIEFINGS_DIR / f"{today}_{slug}.md"

    # Avoid overwriting — add sequence if needed
    if output_path.exists():
        seq = 2
        while True:
            output_path = BRIEFINGS_DIR / f"{today}_{slug}-{seq}.md"
            if not output_path.exists():
                break
            seq += 1

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(briefing_text)

    print(f"Briefing saved: {output_path}")
    # Print path for GitHub Actions to capture
    print(f"::set-output name=briefing_path::{output_path.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
