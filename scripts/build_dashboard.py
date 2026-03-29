"""
build_dashboard.py — Generate static dashboard data from the context store.

Reads all context records and produces:
  dashboard/data/records-index.json  — compact index of all records
  dashboard/data/clusters.json       — cluster membership map
  dashboard/data/taxonomy.json       — copy of controlled vocabulary
  dashboard/data/stats.json          — summary statistics
  dashboard/briefings/*.html         — briefings converted from markdown

Usage:
    python scripts/build_dashboard.py
"""

import json
import re
import shutil
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STORE = PROJECT_ROOT / "context-store"
CONFIG = PROJECT_ROOT / "config"
BRIEFINGS_SRC = PROJECT_ROOT / "outputs" / "briefings"
DASHBOARD = PROJECT_ROOT / "dashboard"
DATA_OUT = DASHBOARD / "data"
BRIEFINGS_OUT = DASHBOARD / "briefings"


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))


def build_records_index():
    """Scan context-store and build a compact index of all records."""
    records = []
    for fpath in sorted(STORE.glob("CTX-*.json")):
        try:
            rec = load_json(fpath)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

        signal = rec.get("political_signal") or {}
        entities = [
            e.get("name", "") for e in (rec.get("entity_refs") or [])[:5]
        ]

        records.append({
            "id": rec.get("record_id", fpath.stem),
            "title": rec.get("title", ""),
            "summary": rec.get("summary", ""),
            "date": rec.get("publication_date", ""),
            "source_type": rec.get("source_type", ""),
            "source_url": rec.get("source_url", ""),
            "tags": rec.get("topic_tags", []),
            "depts": rec.get("department_refs", []),
            "entities": entities,
            "sentiment": rec.get("sentiment", ""),
            "signal": signal.get("signal_type", ""),
            "clusters": rec.get("cluster_ids", []),
        })

    return records


def build_clusters(records):
    """Build cluster membership map from record data."""
    clusters = defaultdict(list)
    for rec in records:
        for cid in rec["clusters"]:
            clusters[cid].append(rec["id"])

    return {
        cid: {"records": rids, "count": len(rids)}
        for cid, rids in sorted(clusters.items())
    }


def build_stats(records):
    """Generate summary statistics."""
    by_source = Counter(r["source_type"] for r in records)
    by_dept = Counter()
    by_month = Counter()
    by_sentiment = Counter(r["sentiment"] for r in records if r["sentiment"])
    by_signal = Counter(r["signal"] for r in records if r["signal"])
    tag_counts = Counter()
    clustered = 0

    for r in records:
        for d in r["depts"]:
            by_dept[d] += 1
        for t in r["tags"]:
            tag_counts[t] += 1
        if r["date"]:
            by_month[r["date"][:7]] += 1
        if r["clusters"]:
            clustered += 1

    return {
        "total_records": len(records),
        "clustered_records": clustered,
        "unclustered_records": len(records) - clustered,
        "cluster_pct": round(100 * clustered / len(records)) if records else 0,
        "by_source": dict(by_source.most_common()),
        "by_department": dict(by_dept.most_common(20)),
        "by_month": dict(sorted(by_month.items())),
        "by_sentiment": dict(by_sentiment.most_common()),
        "by_signal": dict(by_signal.most_common()),
        "top_tags": dict(tag_counts.most_common(30)),
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }


def md_to_html(md_text):
    """Convert markdown to HTML using basic regex (no external deps)."""
    html = md_text

    # Metadata block → styled header
    html = re.sub(
        r"^\*\*([^*]+)\*\*\s*(.+)$",
        r'<p class="meta"><strong>\1</strong> \2</p>',
        html,
        flags=re.MULTILINE,
    )

    # Headings
    html = re.sub(r"^#### (.+)$", r"<h4>\1</h4>", html, flags=re.MULTILINE)
    html = re.sub(r"^### (.+)$", r"<h3>\1</h3>", html, flags=re.MULTILINE)
    html = re.sub(r"^## (.+)$", r"<h2>\1</h2>", html, flags=re.MULTILINE)
    html = re.sub(r"^# (.+)$", r"<h1>\1</h1>", html, flags=re.MULTILINE)

    # Horizontal rules
    html = re.sub(r"^---+$", "<hr>", html, flags=re.MULTILINE)

    # Bold and italic
    html = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", html)
    html = re.sub(r"\*([^*]+)\*", r"<em>\1</em>", html)

    # Inline code
    html = re.sub(r"`([^`]+)`", r"<code>\1</code>", html)

    # Links
    html = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', html)

    # Unordered lists (simple)
    lines = html.split("\n")
    result = []
    in_list = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("- "):
            if not in_list:
                result.append("<ul>")
                in_list = True
            result.append(f"  <li>{stripped[2:]}</li>")
        else:
            if in_list:
                result.append("</ul>")
                in_list = False
            result.append(line)
    if in_list:
        result.append("</ul>")
    html = "\n".join(result)

    # Paragraphs — wrap orphan text lines
    lines = html.split("\n")
    result = []
    for line in lines:
        stripped = line.strip()
        if (
            stripped
            and not stripped.startswith("<")
            and not stripped.startswith("|")
        ):
            result.append(f"<p>{stripped}</p>")
        else:
            result.append(line)
    html = "\n".join(result)

    # Simple table support
    html = convert_tables(html)

    return html


def convert_tables(html):
    """Convert markdown tables to HTML tables."""
    lines = html.split("\n")
    result = []
    table_lines = []
    in_table = False

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("|") and stripped.endswith("|"):
            if not in_table:
                in_table = True
                table_lines = []
            table_lines.append(stripped)
        else:
            if in_table:
                result.append(render_table(table_lines))
                in_table = False
                table_lines = []
            result.append(line)

    if in_table:
        result.append(render_table(table_lines))

    return "\n".join(result)


def render_table(lines):
    """Render markdown table lines as HTML."""
    if len(lines) < 2:
        return "\n".join(lines)

    rows = []
    for i, line in enumerate(lines):
        cells = [c.strip() for c in line.strip("|").split("|")]
        # Skip separator row
        if all(re.match(r"^[-:]+$", c) for c in cells):
            continue
        tag = "th" if i == 0 else "td"
        row = "".join(f"<{tag}>{c}</{tag}>" for c in cells)
        rows.append(f"  <tr>{row}</tr>")

    return '<table class="briefing-table">\n' + "\n".join(rows) + "\n</table>"


def convert_briefings():
    """Convert markdown briefings to HTML files."""
    BRIEFINGS_OUT.mkdir(parents=True, exist_ok=True)
    briefings = []

    if not BRIEFINGS_SRC.exists():
        return briefings

    for md_path in sorted(BRIEFINGS_SRC.glob("*.md")):
        md_text = md_path.read_text(encoding="utf-8")
        body_html = md_to_html(md_text)

        # Extract title from first heading
        title_match = re.search(r"^#\s+(.+)", md_text, re.MULTILINE)
        title = title_match.group(1) if title_match else md_path.stem

        # Extract metadata
        records_match = re.search(r"\*\*Records Reviewed:\*\*\s*(\d+)", md_text)
        date_match = re.search(r"\*\*Generated:\*\*\s*([\d-]+)", md_text)

        slug = md_path.stem
        html_content = BRIEFING_TEMPLATE.format(
            title=title,
            body=body_html,
        )

        out_path = BRIEFINGS_OUT / f"{slug}.html"
        out_path.write_text(html_content, encoding="utf-8")

        briefings.append({
            "slug": slug,
            "title": title,
            "date": date_match.group(1) if date_match else "",
            "records": int(records_match.group(1)) if records_match else 0,
            "file": f"briefings/{slug}.html",
        })

    return briefings


BRIEFING_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title} — Context Engine</title>
<link rel="stylesheet" href="../styles.css">
</head>
<body>
<nav class="briefing-nav">
  <a href="../index.html">&larr; Back to Dashboard</a>
</nav>
<main class="briefing-content">
{body}
</main>
</body>
</html>
"""


def main():
    print("Building dashboard data...")

    # 1. Records index
    records = build_records_index()
    save_json(DATA_OUT / "records-index.json", records)
    print(f"  records-index.json: {len(records)} records")

    # 2. Clusters
    clusters = build_clusters(records)
    save_json(DATA_OUT / "clusters.json", clusters)
    print(f"  clusters.json: {len(clusters)} clusters")

    # 3. Taxonomy
    tax_src = CONFIG / "taxonomy.json"
    if tax_src.exists():
        shutil.copy2(tax_src, DATA_OUT / "taxonomy.json")
        print("  taxonomy.json: copied")

    # 4. Stats
    stats = build_stats(records)
    save_json(DATA_OUT / "stats.json", stats)
    print(f"  stats.json: {stats['total_records']} records, "
          f"{stats['cluster_pct']}% clustered")

    # 5. Briefings
    briefings = convert_briefings()
    save_json(DATA_OUT / "briefings-index.json", briefings)
    print(f"  briefings: {len(briefings)} converted")

    print("\nDashboard build complete.")


if __name__ == "__main__":
    main()
