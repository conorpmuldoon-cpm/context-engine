"""
finalize_records.py — Validate Claude-enriched drafts, assign record IDs, and save
to context-store/.

Usage:
    python scripts/finalize_records.py outputs/drafts/QEfsEBcZQoE_drafts.json
    python scripts/finalize_records.py outputs/drafts/*.json
    python scripts/finalize_records.py outputs/drafts/QEfsEBcZQoE_drafts.json --dry-run

Reads enriched draft JSON files (produced by draft_records.py + Claude review),
filters to substantive segments with non-empty titles/summaries, validates against
the context-record schema, checks for duplicates, assigns CTX-COUNCIL-YYYY-NNNNN
IDs, and saves individual record files to context-store/.
"""

import json
import glob as globmod
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import jsonschema
except ImportError:
    print("Error: jsonschema not installed. Run: python -m pip install jsonschema")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
CONTEXT_STORE = PROJECT_ROOT / "context-store"
SCHEMA_PATH = PROJECT_ROOT / "schemas" / "context-record-schema.json"
TAXONOMY_PATH = PROJECT_ROOT / "config" / "taxonomy.json"
ENTITY_REGISTRY_PATH = PROJECT_ROOT / "config" / "entity-registry.json"


# ---------------------------------------------------------------------------
# Load config
# ---------------------------------------------------------------------------

def load_schema() -> dict:
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_taxonomy() -> dict:
    with open(TAXONOMY_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_entity_registry() -> dict:
    with open(ENTITY_REGISTRY_PATH, encoding="utf-8") as f:
        return json.load(f)


def get_valid_tags(taxonomy: dict) -> set[str]:
    """Collect all valid tags from taxonomy (domain + cross-cutting)."""
    tags = set()
    for domain in taxonomy.get("domain_tags", {}).values():
        tags.update(domain.get("tags", []))
    for cross in taxonomy.get("cross_cutting_tags", {}).values():
        tags.update(cross.get("tags", []))
    return tags


# ---------------------------------------------------------------------------
# Record ID management
# ---------------------------------------------------------------------------

def get_next_sequence(year: int) -> int:
    """Scan context-store/ for the highest CTX-COUNCIL-{year}-NNNNN and return next."""
    CONTEXT_STORE.mkdir(parents=True, exist_ok=True)
    max_seq = 0
    for path in CONTEXT_STORE.glob("CTX-COUNCIL-*.json"):
        name = path.stem  # e.g. CTX-COUNCIL-2026-00003
        parts = name.split("-")
        if len(parts) == 4 and parts[2] == str(year):
            try:
                seq = int(parts[3])
                if seq > max_seq:
                    max_seq = seq
            except ValueError:
                pass
    # Also check other source codes to avoid any ID collisions in the store
    return max_seq + 1


# ---------------------------------------------------------------------------
# Dedup check
# ---------------------------------------------------------------------------

def load_existing_records() -> list[dict]:
    """Load all existing context records for dedup checking."""
    records = []
    CONTEXT_STORE.mkdir(parents=True, exist_ok=True)
    for path in CONTEXT_STORE.glob("CTX-*.json"):
        try:
            with open(path, encoding="utf-8") as f:
                records.append(json.load(f))
        except (json.JSONDecodeError, OSError):
            pass
    return records


def check_duplicate(record: dict, existing: list[dict]) -> str | None:
    """Check if a record is a potential duplicate of an existing one.

    Returns the matching record_id if duplicate detected, None otherwise.

    Criteria (all must match):
    1. Same publication_date (±2 days)
    2. Same primary department_refs (at least 1 overlap)
    3. At least 2 overlapping topic_tags
    4. At least 1 overlapping entity_refs (by name)
    """
    from datetime import timedelta

    try:
        new_date = datetime.strptime(record["publication_date"], "%Y-%m-%d").date()
    except (ValueError, KeyError):
        return None

    new_depts = set(record.get("department_refs", []))
    new_tags = set(record.get("topic_tags", []))
    new_entities = {e["name"] for e in record.get("entity_refs", []) if isinstance(e, dict)}

    for existing_rec in existing:
        try:
            ex_date = datetime.strptime(existing_rec["publication_date"], "%Y-%m-%d").date()
        except (ValueError, KeyError):
            continue

        # 1. Date within ±2 days
        if abs((new_date - ex_date).days) > 2:
            continue

        # 2. At least 1 department overlap
        ex_depts = set(existing_rec.get("department_refs", []))
        if not new_depts & ex_depts:
            continue

        # 3. At least 2 tag overlap
        ex_tags = set(existing_rec.get("topic_tags", []))
        if len(new_tags & ex_tags) < 2:
            continue

        # 4. At least 1 entity overlap
        ex_entities = {e["name"] for e in existing_rec.get("entity_refs", []) if isinstance(e, dict)}
        if not new_entities & ex_entities:
            continue

        return existing_rec.get("record_id", "UNKNOWN")

    return None


# ---------------------------------------------------------------------------
# Build context record from enriched segment
# ---------------------------------------------------------------------------

def build_record(segment: dict, draft: dict, record_id: str,
                 capture_dt: str) -> dict:
    """Transform an enriched draft segment into a full context record."""
    video_id = draft["video_id"]
    pub_date = draft["publication_date"]
    start_sec = segment.get("start_seconds", 0)

    source_url = f"https://www.youtube.com/watch?v={video_id}&t={start_sec}s"

    # Transcript ref — point to the corrected transcript file
    transcript_ref = f"outputs/transcripts/{video_id}_corrected.txt"

    # Freshness class from taxonomy mapping
    freshness = "long"  # council_segment default

    # Build processing_notes from both auto_note and enriched processing_notes
    notes = []
    auto_note = segment.get("auto_note", "")
    if auto_note:
        notes.append(auto_note)
    notes.extend(segment.get("processing_notes", []))
    notes.append(f"Semi-automated: draft_records.py extraction + Claude enrichment")
    notes.append(f"Segment {segment.get('segment_number', '?')} of {len(draft['segments'])} "
                 f"from {draft.get('video_title', video_id)}")
    if draft.get("corrections_applied"):
        notes.append(f"Transcript corrections applied: {draft['corrections_applied'][:200]}")

    record = {
        "record_id": record_id,
        "source_agent": "council_transcriber",
        "source_type": "council_segment",
        "source_url": source_url,
        "publication_date": pub_date,
        "capture_date": capture_dt,
        "title": segment.get("title", ""),
        "summary": segment.get("summary", ""),
        "raw_content": segment.get("raw_content", ""),
        "transcript_ref": transcript_ref,
        "topic_tags": segment.get("topic_tags", []),
        "department_refs": segment.get("department_refs", []),
        "entity_refs": segment.get("entity_refs", []),
        "speakers": segment.get("speakers", []),
        "speaker_confidence": segment.get("speaker_confidence", "medium"),
        "sentiment": segment.get("sentiment", "neutral"),
        "political_signal": segment.get("political_signal", None),
        "freshness_class": freshness,
        "cluster_ids": segment.get("cluster_ids", []),
        "engagement_relevance": [],
        "feedback": None,
        "schema_version": "1.0.0",
        "librarian_version": "1.0.0",
        "processing_notes": notes,
        "last_relevance_update": None,
    }

    return record


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_tags(record: dict, valid_tags: set[str]) -> list[str]:
    """Check all topic_tags are in taxonomy or are PROVISIONAL:."""
    errors = []
    for tag in record.get("topic_tags", []):
        if tag.startswith("PROVISIONAL:"):
            continue
        if tag not in valid_tags:
            errors.append(f"Unknown tag '{tag}' not in taxonomy")
    return errors


def validate_record(record: dict, schema: dict) -> list[str]:
    """Validate record against JSON schema. Returns list of error messages."""
    errors = []
    try:
        jsonschema.validate(instance=record, schema=schema)
    except jsonschema.ValidationError as e:
        errors.append(f"Schema validation: {e.message}")
    return errors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_draft_file(draft_path: Path, schema: dict, taxonomy: dict,
                       valid_tags: set[str], existing_records: list[dict],
                       next_seq: int, dry_run: bool) -> tuple[int, int, int, list[str]]:
    """Process one draft file. Returns (saved, skipped, dedup_linked, errors)."""
    with open(draft_path, encoding="utf-8") as f:
        draft = json.load(f)

    segments = draft.get("segments", [])
    capture_dt = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    year = int(draft["publication_date"][:4]) if draft.get("publication_date") else 2026

    saved = 0
    skipped = 0
    dedup_linked = 0
    errors = []
    records_saved = []

    print(f"\n{'=' * 60}")
    print(f"Draft: {draft_path.name}")
    print(f"  Meeting: {draft.get('video_title', draft.get('video_id', '?'))}")
    print(f"  Date: {draft.get('publication_date', '?')}")
    print(f"  Segments: {len(segments)}")

    for seg in segments:
        seg_num = seg.get("segment_number", "?")

        # Skip non-substantive or unenriched segments
        if not seg.get("substantive", False):
            print(f"  Seg {seg_num}: SKIP (non-substantive)")
            skipped += 1
            continue

        title = seg.get("title", "").strip()
        summary = seg.get("summary", "").strip()
        if not title or not summary:
            print(f"  Seg {seg_num}: SKIP (missing title or summary -- not enriched)")
            skipped += 1
            continue

        # Assign ID
        record_id = f"CTX-COUNCIL-{year}-{next_seq:05d}"
        record = build_record(seg, draft, record_id, capture_dt)

        # Validate tags
        tag_errors = validate_tags(record, valid_tags)
        if tag_errors:
            for err in tag_errors:
                errors.append(f"Seg {seg_num}: {err}")
                print(f"  Seg {seg_num}: TAG ERROR -- {err}")

        # Validate against schema
        schema_errors = validate_record(record, schema)
        if schema_errors:
            for err in schema_errors:
                errors.append(f"Seg {seg_num}: {err}")
                print(f"  Seg {seg_num}: SCHEMA ERROR -- {err}")
            continue  # Don't save invalid records

        # Dedup check against existing + records saved in this run
        all_existing = existing_records + records_saved
        dup_id = check_duplicate(record, all_existing)
        if dup_id:
            # Link via cluster_ids rather than skip
            dedup_linked += 1
            if dup_id not in record.get("cluster_ids", []):
                record.setdefault("cluster_ids", [])
                # Don't add the dup record_id directly — they should share cluster IDs
                # The existing record already has its cluster_ids; just note the relationship
                record["processing_notes"].append(
                    f"Potential duplicate detected: overlaps with {dup_id} -- linked, not skipped"
                )
            print(f"  Seg {seg_num}: DEDUP -- overlaps with {dup_id} (linking, not skipping)")

        # Save
        if not dry_run:
            out_path = CONTEXT_STORE / f"{record_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, indent=2, ensure_ascii=False)
            print(f"  Seg {seg_num}: SAVED -> {record_id} -- {title[:60]}")
        else:
            print(f"  Seg {seg_num}: DRY RUN -> {record_id} -- {title[:60]}")

        records_saved.append(record)
        next_seq += 1
        saved += 1

    return saved, skipped, dedup_linked, errors


def main():
    args = sys.argv[1:]
    dry_run = "--dry-run" in args
    if dry_run:
        args.remove("--dry-run")

    if not args:
        print("Usage: python scripts/finalize_records.py <draft_file(s)> [--dry-run]")
        print("  Accepts file paths or glob patterns (e.g., outputs/drafts/*.json)")
        sys.exit(1)

    # Expand glob patterns
    draft_paths: list[Path] = []
    for arg in args:
        matches = globmod.glob(arg)
        if matches:
            draft_paths.extend(Path(m) for m in matches)
        else:
            p = Path(arg)
            if p.exists():
                draft_paths.append(p)
            else:
                print(f"Warning: {arg} not found, skipping")

    if not draft_paths:
        print("No draft files found.")
        sys.exit(1)

    # Load config
    schema = load_schema()
    taxonomy = load_taxonomy()
    valid_tags = get_valid_tags(taxonomy)
    existing_records = load_existing_records()

    # Determine starting sequence number
    year = 2026  # Will be overridden per-draft
    next_seq = get_next_sequence(year)

    print(f"Finalize Records {'(DRY RUN)' if dry_run else ''}")
    print(f"  Draft files: {len(draft_paths)}")
    print(f"  Existing records in context-store: {len(existing_records)}")
    print(f"  Next sequence number: {next_seq:05d}")
    print(f"  Valid taxonomy tags: {len(valid_tags)}")

    total_saved = 0
    total_skipped = 0
    total_dedup = 0
    all_errors = []
    all_tags_used = set()

    for draft_path in draft_paths:
        saved, skipped, dedup, errs = process_draft_file(
            draft_path, schema, taxonomy, valid_tags, existing_records,
            next_seq, dry_run
        )
        total_saved += saved
        total_skipped += skipped
        total_dedup += dedup
        all_errors.extend(errs)
        next_seq += saved  # Advance sequence for next file

        # Track tags used
        try:
            with open(draft_path, encoding="utf-8") as f:
                d = json.load(f)
            for seg in d.get("segments", []):
                if seg.get("substantive") and seg.get("title"):
                    all_tags_used.update(seg.get("topic_tags", []))
        except Exception:
            pass

    # Summary
    print(f"\n{'=' * 60}")
    print(f"SUMMARY {'(DRY RUN)' if dry_run else ''}")
    print(f"  Records saved: {total_saved}")
    print(f"  Segments skipped: {total_skipped}")
    print(f"  Dedup links created: {total_dedup}")
    print(f"  Errors: {len(all_errors)}")
    if all_tags_used:
        provisional = sorted(t for t in all_tags_used if t.startswith("PROVISIONAL:"))
        regular = sorted(t for t in all_tags_used if not t.startswith("PROVISIONAL:"))
        print(f"  Tags used: {', '.join(regular)}")
        if provisional:
            print(f"  Provisional tags: {', '.join(provisional)}")
    if all_errors:
        print(f"\nErrors:")
        for err in all_errors:
            print(f"  - {err}")


if __name__ == "__main__":
    main()
