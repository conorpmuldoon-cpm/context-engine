"""
weekly_digest.py — AI-powered weekly content digest for the Context Engine

Scans context-store for records added or modified since the last digest,
collects mechanical stats, sends summaries to Claude for synthesis, and
emails the CIO a digest with key takeaways.

Usage:
    python scripts/weekly_digest.py              # run digest + email
    python scripts/weekly_digest.py --no-email   # run digest, skip email
    python scripts/weekly_digest.py --all        # include ALL records (not just new)
    python scripts/weekly_digest.py --dry-run    # show what would be sent to Claude

Designed to be called from Windows Task Scheduler on a weekly basis.
"""

import json
import os
import smtplib
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
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
TRANSCRIPTS_DIR = PROJECT_ROOT / "outputs" / "transcripts"
AGENDAS_DIR = PROJECT_ROOT / "outputs" / "agendas"
EMAIL_CONFIG_PATH = PROJECT_ROOT / "config" / "email-config.json"
API_CONFIG_PATH = PROJECT_ROOT / "config" / "api-config.json"
STATE_PATH = PROJECT_ROOT / "outputs" / "digest-state.json"
DIGESTS_DIR = PROJECT_ROOT / "outputs" / "digests"


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_email_config() -> dict | None:
    """Load email configuration. Returns None if not configured."""
    if not EMAIL_CONFIG_PATH.exists():
        return None
    with open(EMAIL_CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)
    if not config.get("enabled"):
        return None
    for key in ["smtp_server", "smtp_port", "sender_email", "sender_password", "recipient_email"]:
        if not config.get(key) or "YOUR_" in str(config.get(key, "")):
            return None
    return config


def load_api_config() -> dict:
    """Load Anthropic API configuration."""
    if not API_CONFIG_PATH.exists():
        print("Error: config/api-config.json not found")
        sys.exit(1)
    with open(API_CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)
    if "YOUR_" in config.get("anthropic_api_key", "YOUR_"):
        print("Error: API key not set in config/api-config.json")
        sys.exit(1)
    return config


def load_state() -> dict:
    """Load last digest run state."""
    if STATE_PATH.exists():
        with open(STATE_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"last_run": None, "last_record_count": 0}


def save_state(record_count: int):
    """Save current run state."""
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    state = {
        "last_run": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "last_record_count": record_count,
    }
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


# ---------------------------------------------------------------------------
# Load records
# ---------------------------------------------------------------------------

def load_all_records() -> list[tuple[Path, dict]]:
    """Load all context records."""
    records = []
    CONTEXT_STORE.mkdir(parents=True, exist_ok=True)
    for path in sorted(CONTEXT_STORE.glob("CTX-*.json")):
        try:
            with open(path, encoding="utf-8") as f:
                records.append((path, json.load(f)))
        except (json.JSONDecodeError, OSError):
            pass
    return records


def get_new_records(records: list[tuple[Path, dict]], since: str | None) -> list[tuple[Path, dict]]:
    """Filter to records modified since the last digest run."""
    if not since:
        return records  # First run — include everything

    since_dt = datetime.strptime(since, "%Y-%m-%dT%H:%M:%S")

    new_records = []
    for path, rec in records:
        # Check file modification time
        mod_time = datetime.fromtimestamp(path.stat().st_mtime)
        if mod_time > since_dt:
            new_records.append((path, rec))

    return new_records


# ---------------------------------------------------------------------------
# Mechanical stats
# ---------------------------------------------------------------------------

def collect_stats(all_records, new_records) -> dict:
    """Collect mechanical statistics for the digest."""
    stats = {
        "total_records": len(all_records),
        "new_records": len(new_records),
        "new_by_source": Counter(),
        "new_by_dept": Counter(),
        "new_tags": Counter(),
        "clusters": defaultdict(list),
        "all_clusters": defaultdict(list),
        "transcripts_referenced": set(),
        "meetings_covered": set(),
        "political_signals": [],
    }

    # Analyze new records
    for path, rec in new_records:
        rid = rec.get("record_id", path.stem)
        source = rec.get("source_type", "unknown")
        stats["new_by_source"][source] += 1

        for dept in rec.get("department_refs", []):
            stats["new_by_dept"][dept] += 1

        for tag in rec.get("topic_tags", []):
            stats["new_tags"][tag] += 1

        for cid in rec.get("cluster_ids", []):
            stats["clusters"][cid].append(rid)

        # Track transcripts/meetings
        tref = rec.get("transcript_ref", "")
        if tref:
            stats["transcripts_referenced"].add(tref)

        url = rec.get("source_url", "") or ""
        date = rec.get("publication_date", "")
        if "youtube.com" in url and date:
            vid = url.split("v=")[1].split("&")[0] if "v=" in url else ""
            if vid:
                stats["meetings_covered"].add((date, vid))

        # Collect political signals
        signal = rec.get("political_signal")
        if signal and isinstance(signal, dict):
            stats["political_signals"].append({
                "record_id": rid,
                "title": rec.get("title", ""),
                "signal_type": signal.get("signal_type", ""),
                "description": signal.get("description", ""),
            })

    # All clusters for context
    for path, rec in all_records:
        rid = rec.get("record_id", path.stem)
        for cid in rec.get("cluster_ids", []):
            stats["all_clusters"][cid].append(rid)

    return stats


# ---------------------------------------------------------------------------
# Build Claude prompt
# ---------------------------------------------------------------------------

def build_digest_prompt(new_records, stats) -> str:
    """Build the prompt for Claude to synthesize takeaways."""

    # Build record summaries for Claude
    record_summaries = []
    for path, rec in new_records:
        rid = rec.get("record_id", path.stem)
        entry = (
            f"- {rid} | {rec.get('publication_date', '?')} | "
            f"{rec.get('title', 'Untitled')}\n"
            f"  Summary: {rec.get('summary', 'No summary')[:300]}\n"
            f"  Dept: {', '.join(rec.get('department_refs', []))}\n"
            f"  Tags: {', '.join(rec.get('topic_tags', []))}\n"
            f"  Cluster: {', '.join(rec.get('cluster_ids', [])) or 'none'}"
        )
        signal = rec.get("political_signal")
        if signal and isinstance(signal, dict):
            entry += f"\n  Political signal: {signal.get('signal_type', '')} — {signal.get('description', '')}"
        record_summaries.append(entry)

    # Cluster context
    cluster_lines = []
    for cid, members in sorted(stats["all_clusters"].items(), key=lambda x: -len(x[1])):
        cluster_lines.append(f"- {cid}: {len(members)} records")

    prompt = f"""You are the Context Engine Librarian for the City of Syracuse Chief Innovation Officer.

Below are {len(new_records)} context records from the past week's council meetings and public sources. Synthesize them into a concise weekly digest email.

RECORDS:
{chr(10).join(record_summaries)}

ACTIVE CLUSTERS (all-time):
{chr(10).join(cluster_lines)}

STATS:
- Total records in store: {stats['total_records']}
- New this period: {stats['new_records']}
- Meetings covered: {len(stats['meetings_covered'])}
- Departments active: {', '.join(stats['new_by_dept'].keys())}

Write the digest in this format:

AT A GLANCE
(2-3 sentence overview of what happened this week)

KEY TAKEAWAYS
(3-5 bullet points of the most important political, operational, or budget items the CIO should know about — be specific, cite record IDs)

WHAT'S HEATING UP
(1-3 emerging issues or trends gaining momentum across meetings)

CLUSTER UPDATES
(For each active cluster with new activity: 1 sentence on what changed)

WATCH THIS WEEK
(1-3 items to keep an eye on based on signals in the data)

Keep it practical and direct. The CIO is not a programmer — write for a senior government executive. No jargon. Cite specific record IDs so they can dig deeper if needed."""

    return prompt


# ---------------------------------------------------------------------------
# Call Claude
# ---------------------------------------------------------------------------

def generate_synthesis(prompt: str, api_config: dict) -> str:
    """Send prompt to Claude and return the synthesis."""
    client = anthropic.Anthropic(api_key=api_config["anthropic_api_key"])
    model = api_config.get("model", "claude-haiku-4-5-20251001")

    response = client.messages.create(
        model=model,
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )

    return response.content[0].text


# ---------------------------------------------------------------------------
# Format digest
# ---------------------------------------------------------------------------

def format_digest(synthesis: str, stats: dict) -> str:
    """Combine mechanical stats header with Claude's synthesis."""
    today = datetime.now().strftime("%Y-%m-%d")

    header = f"""CONTEXT ENGINE WEEKLY DIGEST — {today}
{'=' * 50}

Records: {stats['total_records']} total | {stats['new_records']} new this period
Meetings covered: {len(stats['meetings_covered'])}
Active clusters: {len(stats['all_clusters'])}
"""

    # Meeting list
    if stats["meetings_covered"]:
        header += "\nMeetings scanned:\n"
        for date, vid in sorted(stats["meetings_covered"]):
            header += f"  - {date} (youtube.com/watch?v={vid})\n"

    # Transcript list
    if stats["transcripts_referenced"]:
        header += f"\nTranscripts referenced: {len(stats['transcripts_referenced'])}\n"

    header += f"\n{'=' * 50}\n\n"

    footer = f"""
{'=' * 50}
Generated by Context Engine weekly_digest.py
Model: Claude Haiku | Records analyzed: {stats['new_records']}
Full records available in context-store/
"""

    return header + synthesis + footer


# ---------------------------------------------------------------------------
# Save digest
# ---------------------------------------------------------------------------

def save_digest(content: str) -> Path:
    """Save digest to a dated file."""
    DIGESTS_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    digest_path = DIGESTS_DIR / f"digest-{today}.txt"

    if digest_path.exists():
        seq = 2
        while True:
            digest_path = DIGESTS_DIR / f"digest-{today}-{seq}.txt"
            if not digest_path.exists():
                break
            seq += 1

    with open(digest_path, "w", encoding="utf-8") as f:
        f.write(content)

    return digest_path


# ---------------------------------------------------------------------------
# Send email
# ---------------------------------------------------------------------------

def send_digest_email(config: dict, subject: str, body: str) -> bool:
    """Send the digest email."""
    msg = MIMEMultipart()
    msg["From"] = config["sender_email"]
    msg["To"] = config["recipient_email"]
    msg["Subject"] = f"[Context Engine Digest] {subject}"
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(config["smtp_server"], config["smtp_port"]) as server:
            server.starttls()
            server.login(config["sender_email"], config["sender_password"])
            server.send_message(msg)
        return True
    except Exception as e:
        print(f"Email send failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = set(sys.argv[1:])
    skip_email = "--no-email" in args
    include_all = "--all" in args
    dry_run = "--dry-run" in args

    today = datetime.now().strftime("%Y-%m-%d")

    # Load configs
    api_config = load_api_config()
    email_config = None
    if not skip_email:
        email_config = load_email_config()
        if not email_config:
            print("Email not configured — will save digest only.")

    # Load state and records
    state = load_state()
    all_records = load_all_records()
    print(f"Loaded {len(all_records)} records from context-store/")

    if include_all:
        new_records = all_records
        print(f"Including ALL {len(all_records)} records (--all flag)")
    else:
        since = state.get("last_run")
        new_records = get_new_records(all_records, since)
        if since:
            print(f"Found {len(new_records)} records modified since {since}")
        else:
            print(f"First run — including all {len(new_records)} records")

    if not new_records:
        print("No new records since last digest. Nothing to report.")
        save_state(len(all_records))
        return

    # Collect stats
    stats = collect_stats(all_records, new_records)

    # Build prompt
    prompt = build_digest_prompt(new_records, stats)

    if dry_run:
        print("\n--- DRY RUN: Prompt that would be sent to Claude ---\n")
        print(prompt)
        print(f"\n--- Prompt length: {len(prompt)} chars ---")
        return

    # Generate synthesis via Claude
    print(f"Sending {len(new_records)} record summaries to Claude...")
    try:
        synthesis = generate_synthesis(prompt, api_config)
        print("Synthesis generated successfully.")
    except Exception as e:
        print(f"Claude API error: {e}")
        # Fall back to mechanical-only digest
        synthesis = "(Claude synthesis unavailable — API error)\n\nSee record list above for details."

    # Format full digest
    digest = format_digest(synthesis, stats)

    # Save
    digest_path = save_digest(digest)
    print(f"Digest saved: {digest_path}")

    # Email
    if email_config:
        subject = f"Weekly Digest — {today}"
        if send_digest_email(email_config, subject, digest):
            print("Digest email sent successfully!")
        else:
            print("Email failed — digest still saved to file.")

    # Update state
    save_state(len(all_records))

    # Print to console
    print(f"\n{digest}")


if __name__ == "__main__":
    main()
