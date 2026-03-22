"""
pull_transcript.py — Fetch a YouTube auto-generated transcript and apply
name corrections from the Context Engine entity registry.

Usage:
    python scripts/pull_transcript.py QEfsEBcZQoE
    python scripts/pull_transcript.py "https://www.youtube.com/watch?v=7fCCamsh6U0"
    python scripts/pull_transcript.py 7fCCamsh6U0 --agenda outputs/agendas/2026-02-23_names.json

Outputs (in outputs/transcripts/):
    {video_id}_raw.json       — raw transcript snippets from YouTube
    {video_id}_corrected.txt  — human-readable timestamped transcript with names fixed
"""

import json
import re
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from youtube_transcript_api import YouTubeTranscriptApi


# ---------------------------------------------------------------------------
# Paths — resolved relative to this script so it works from the project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
ENTITY_REGISTRY = PROJECT_ROOT / "config" / "entity-registry.json"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "transcripts"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def extract_video_id(arg: str) -> str:
    """Accept a bare video ID or a full YouTube URL and return just the ID."""
    # If it looks like a URL, parse out the v= parameter
    if "youtube.com" in arg or "youtu.be" in arg:
        parsed = urlparse(arg)
        if "youtu.be" in parsed.hostname:
            return parsed.path.lstrip("/")
        qs = parse_qs(parsed.query)
        if "v" in qs:
            return qs["v"][0]
    # Otherwise treat the whole argument as a video ID
    return arg.strip()


def format_timestamp(seconds: float) -> str:
    """Convert seconds to [HH:MM:SS] format."""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"[{h:02d}:{m:02d}:{s:02d}]"


def load_entity_registry() -> dict:
    """Load the entity registry; return empty structure if missing."""
    if not ENTITY_REGISTRY.exists():
        print(f"Warning: Entity registry not found at {ENTITY_REGISTRY}")
        return {"persons": [], "organizations": []}
    with open(ENTITY_REGISTRY, encoding="utf-8") as f:
        return json.load(f)


def build_replacement_map(registry: dict) -> list[tuple[str, str]]:
    """Build a list of (variant, canonical_name) pairs sorted longest-first
    so that longer variants are matched before shorter substrings."""
    pairs = []
    for person in registry.get("persons", []):
        canonical = person["canonical_name"]
        for variant in person.get("transcript_variants", []):
            pairs.append((variant, canonical))
    for org in registry.get("organizations", []):
        canonical = org["canonical_name"]
        for variant in org.get("transcript_variants", []):
            pairs.append((variant, canonical))
    # Sort longest variant first to avoid partial replacements
    pairs.sort(key=lambda p: len(p[0]), reverse=True)
    return pairs


def apply_corrections(text: str, replacement_map: list[tuple[str, str]]) -> tuple[str, list[str]]:
    """Apply name corrections to text. Returns (corrected_text, list_of_corrections_made).
    Short variants (<=3 chars) use word-boundary matching to avoid mangling words."""
    corrections = []
    for variant, canonical in replacement_map:
        escaped = re.escape(variant)
        # Short variants like "SU" or "CNS" need word boundaries
        if len(variant) <= 3:
            pattern = re.compile(r"\b" + escaped + r"\b", re.IGNORECASE)
        else:
            pattern = re.compile(escaped, re.IGNORECASE)
        if pattern.search(text):
            corrections.append(f"{variant} -> {canonical}")
            text = pattern.sub(canonical, text)
    return text, corrections


def load_agenda_data(agenda_path: str) -> dict:
    """Load parsed agenda names JSON for detection and reporting."""
    path = Path(agenda_path)
    if not path.exists():
        print(f"Warning: Agenda names file not found: {path}")
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def detect_agenda_names(agenda_data: dict, corrected_snippets: list[dict]) -> dict:
    """Scan corrected transcript for agenda names. Returns found/missing report.
    Uses last-name matching for people and keyword matching for organizations."""
    full_text = " ".join(s["text"] for s in corrected_snippets).lower()

    found = []
    missing = []

    for entry in agenda_data.get("names", []):
        name = entry.get("name", "")
        if not name:
            continue
        parts = name.split()
        last_name = parts[-1].lower() if parts else ""
        if last_name and len(last_name) > 2 and re.search(r"\b" + re.escape(last_name) + r"\b", full_text):
            found.append({"name": name, "context": entry.get("context", ""), "matched_by": last_name})
        else:
            missing.append({"name": name, "context": entry.get("context", "")})

    # Common words to skip when matching organizations
    skip_words = {
        "county", "city", "state", "york", "new", "west", "east", "north", "south",
        "foundation", "association", "department", "corporation", "services",
        "design", "architecture", "engineering", "environmental", "landscape",
    }

    for entry in agenda_data.get("organizations", []):
        name = entry.get("name", "")
        if not name:
            continue
        # Try matching a distinctive keyword (5+ chars, not a common word)
        words = [w for w in name.split() if len(w) >= 5 and w.lower() not in skip_words]
        matched = False
        for word in words:
            if re.search(r"\b" + re.escape(word.lower()) + r"\b", full_text):
                found.append({"name": name, "context": entry.get("context", ""), "matched_by": word})
                matched = True
                break
        if not matched:
            missing.append({"name": name, "context": entry.get("context", "")})

    return {"found": found, "missing": missing}


def fetch_video_title(video_id: str) -> str:
    """Try to get the video title from YouTube's oembed endpoint."""
    try:
        import urllib.request
        url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data.get("title", "(title unavailable)")
    except Exception:
        return "(title unavailable)"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/pull_transcript.py <video_id_or_url> [--agenda <names.json>]")
        print('  Example: python scripts/pull_transcript.py QEfsEBcZQoE')
        print('  Example: python scripts/pull_transcript.py 7fCCamsh6U0 --agenda outputs/agendas/2026-02-23_names.json')
        sys.exit(1)

    # Parse arguments
    agenda_path = None
    args = sys.argv[1:]
    if "--agenda" in args:
        idx = args.index("--agenda")
        if idx + 1 < len(args):
            agenda_path = args[idx + 1]
            args = args[:idx] + args[idx + 2:]
        else:
            print("Error: --agenda requires a path to a names JSON file.")
            sys.exit(1)

    video_id = extract_video_id(args[0])
    print(f"Video ID: {video_id}")

    # --- Fetch transcript ---------------------------------------------------
    print("Fetching transcript...")
    try:
        transcript = YouTubeTranscriptApi().fetch(video_id)
    except Exception as e:
        print(f"Error fetching transcript: {e}")
        sys.exit(1)

    snippets = transcript.to_raw_data()
    if not snippets:
        print("No transcript snippets returned.")
        sys.exit(1)

    duration_sec = snippets[-1]["start"] + snippets[-1].get("duration", 0)
    dur_m, dur_s = divmod(int(duration_sec), 60)
    dur_h, dur_m = divmod(dur_m, 60)
    duration_str = f"{dur_h}h {dur_m:02d}m {dur_s:02d}s" if dur_h else f"{dur_m}m {dur_s:02d}s"

    is_generated = getattr(transcript, "is_generated", None)
    gen_label = " (auto-generated)" if is_generated else ""

    print(f"  Snippets: {len(snippets)} | Duration: ~{duration_str}{gen_label}")

    # --- Fetch video title --------------------------------------------------
    title = fetch_video_title(video_id)
    print(f"  Title: {title}")

    # --- Load entity registry and build replacements ------------------------
    registry = load_entity_registry()
    replacement_map = build_replacement_map(registry)
    print(f"  Entity registry: {len(replacement_map)} replacement variants loaded")

    # --- Load agenda data if provided ----------------------------------------
    agenda_data = {}
    if agenda_path:
        agenda_data = load_agenda_data(agenda_path)
        if agenda_data:
            n_names = len(agenda_data.get("names", []))
            n_orgs = len(agenda_data.get("organizations", []))
            print(f"  Agenda loaded: {n_names} people, {n_orgs} organizations")

    # --- Apply corrections --------------------------------------------------
    all_corrections = []
    corrected_snippets = []
    for snip in snippets:
        corrected_text, corrections = apply_corrections(snip["text"], replacement_map)
        corrected_snippets.append({**snip, "text": corrected_text})
        all_corrections.extend(corrections)

    # Deduplicate correction list for the summary
    unique_corrections = sorted(set(all_corrections))

    # --- Write raw JSON -----------------------------------------------------
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    raw_path = OUTPUT_DIR / f"{video_id}_raw.json"
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(snippets, f, indent=2, ensure_ascii=False)
    print(f"  Saved raw transcript:  {raw_path.relative_to(PROJECT_ROOT)}")

    # --- Write corrected text -----------------------------------------------
    corrected_path = OUTPUT_DIR / f"{video_id}_corrected.txt"
    with open(corrected_path, "w", encoding="utf-8") as f:
        f.write(f"=== Video: {title} ===\n")
        f.write(f"=== Duration: ~{duration_str} | Snippets: {len(snippets)}{gen_label} ===\n")
        f.write(f"=== Name corrections applied: {len(all_corrections)} ({len(unique_corrections)} unique) ===\n")
        if unique_corrections:
            f.write(f"=== Corrections: {'; '.join(unique_corrections)} ===\n")
        if agenda_data:
            f.write(f"=== Agenda reference: {agenda_data.get('date', '?')} ({agenda_path}) ===\n")
        f.write("\n")
        for snip in corrected_snippets:
            ts = format_timestamp(snip["start"])
            f.write(f"{ts} {snip['text']}\n")
    print(f"  Saved corrected text:  {corrected_path.relative_to(PROJECT_ROOT)}")

    # --- Console summary ----------------------------------------------------
    print()
    print("--- Summary ---")
    print(f"  Video:       {title}")
    print(f"  Duration:    ~{duration_str}")
    print(f"  Snippets:    {len(snippets)}")
    print(f"  Corrections: {len(all_corrections)} total ({len(unique_corrections)} unique)")
    if unique_corrections:
        for c in unique_corrections:
            print(f"    - {c}")

    # Collect all unique names found (both corrected and uncorrected)
    # by scanning the corrected text for canonical names
    found_canonical = set()
    for person in registry.get("persons", []):
        name = person["canonical_name"]
        for snip in corrected_snippets:
            if name.lower() in snip["text"].lower():
                found_canonical.add(name)
                break
    for org in registry.get("organizations", []):
        name = org["canonical_name"]
        for snip in corrected_snippets:
            if name.lower() in snip["text"].lower():
                found_canonical.add(name)
                break

    if found_canonical:
        print(f"\n  Known entities detected ({len(found_canonical)}):")
        for name in sorted(found_canonical):
            print(f"    - {name}")

    # --- Agenda name detection report ---------------------------------------
    if agenda_data:
        report = detect_agenda_names(agenda_data, corrected_snippets)
        if report["found"]:
            print(f"\n  Agenda names found in transcript ({len(report['found'])}):")
            for item in report["found"]:
                print(f"    + {item['name']} (matched '{item['matched_by']}', {item['context']})")
        if report["missing"]:
            print(f"\n  Agenda names NOT found ({len(report['missing'])}) — may be garbled or absent:")
            for item in report["missing"]:
                print(f"    ? {item['name']} ({item['context']})")

    print()


if __name__ == "__main__":
    main()
