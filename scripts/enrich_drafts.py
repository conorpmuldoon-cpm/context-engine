"""
enrich_drafts.py — Enrich draft council meeting segments via Claude Haiku API.

Takes draft JSON files produced by draft_records.py and fills in the empty
enrichment fields (title, summary, topic_tags, speakers, entity_refs, etc.)
by sending each substantive segment to Claude Haiku.

Usage:
    python scripts/enrich_drafts.py outputs/drafts/VIDEO_ID_drafts.json
    python scripts/enrich_drafts.py --all
    python scripts/enrich_drafts.py outputs/drafts/VIDEO_ID_drafts.json --dry-run
"""

import json
import sys
from pathlib import Path

try:
    import anthropic
except ImportError:
    anthropic = None

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DRAFTS_DIR = PROJECT_ROOT / "outputs" / "drafts"

# Reuse collector_utils for config loading and tag utilities
sys.path.insert(0, str(SCRIPT_DIR))
from collector_utils import (
    load_taxonomy,
    load_entity_registry,
    load_api_config,
    get_flat_tag_list,
    get_valid_tags,
    setup_logger,
)


# ---------------------------------------------------------------------------
# Council-specific enrichment prompt
# ---------------------------------------------------------------------------

def _build_council_prompt(
    segment: dict,
    draft: dict,
    taxonomy: dict,
    registry: dict,
) -> str:
    """Build a council-segment-specific enrichment prompt."""
    tag_list = get_flat_tag_list(taxonomy)

    # Segment info
    raw_content = segment.get("raw_content", "")
    if len(raw_content) > 6000:
        raw_content = raw_content[:6000] + "\n[...truncated]"

    seg_num = segment.get("segment_number", "?")
    total_segs = len(draft.get("segments", []))

    # Agenda context
    agenda_items = segment.get("agenda_items", [])
    agenda_descs = segment.get("agenda_descriptions", [])
    agenda_str = ""
    if agenda_items:
        agenda_str = f"Items: {', '.join(str(i) for i in agenda_items)}"
        if agenda_descs:
            # Include first few agenda descriptions for context
            descs = agenda_descs[:3]
            for i, desc in enumerate(descs):
                if len(desc) > 300:
                    desc = desc[:300] + "..."
                agenda_str += f"\n  - {desc}"
            if len(agenda_descs) > 3:
                agenda_str += f"\n  ... and {len(agenda_descs) - 3} more items"

    # Mechanical extractions
    entities_str = ", ".join(
        e["name"] if isinstance(e, dict) else str(e)
        for e in segment.get("detected_entities", [])
    ) or "none detected"
    depts_str = ", ".join(segment.get("detected_departments", [])) or "none detected"
    speakers_str = ", ".join(segment.get("detected_speakers", [])) or "none detected"
    amounts_str = ", ".join(segment.get("detected_amounts", [])) or "none detected"

    # Known persons list for speaker attribution
    known_persons = []
    for person in registry.get("persons", []):
        known_persons.append(f"{person['canonical_name']} ({person.get('title', '')})")
    persons_hint = "; ".join(known_persons[:20])

    # Political signal types for reference
    signal_types = "championship, opposition, scrutiny, constituent_pressure, priority_alignment, budget_commitment"

    return f"""You are the Context Engine Librarian analyzing a Syracuse Common Council meeting segment.
Your audience is the Chief Innovation Officer — focus on operational and political intelligence.

MEETING: {draft.get('video_title', 'Unknown')}
DATE: {draft.get('publication_date', 'Unknown')}
TYPE: {draft.get('meeting_type', 'Unknown')}
SEGMENT: {seg_num} of {total_segs}
AGENDA CONTEXT: {agenda_str or 'No agenda items referenced'}

TRANSCRIPT:
{raw_content}

MECHANICAL EXTRACTIONS (validate and augment — these may have false positives):
Entities: {entities_str}
Departments: {depts_str}
Speakers: {speakers_str}
Dollar amounts: {amounts_str}

KNOWN COUNCIL MEMBERS & OFFICIALS (use for speaker attribution):
{persons_hint}

AVAILABLE TAGS (use ONLY these, or prefix new ones with PROVISIONAL:):
{tag_list}

VALID POLITICAL SIGNAL TYPES: {signal_types}

Return ONLY a JSON object (no markdown fences, no explanation) with these exact keys:
{{
  "title": "Brief descriptive title for this segment (5-15 words)",
  "summary": "2-4 sentence summary focused on what the CIO needs to know (20-2000 chars)",
  "topic_tags": ["tag1", "tag2"],
  "department_refs": ["Full Canonical Department Name"],
  "entity_refs": [{{"name": "Person or Org Name", "type": "person|organization|system", "canonical_id": "PERSON-FIRSTNAME-LASTNAME|ORG-SHORT-NAME|SYS-SHORT-NAME|null"}}],
  "speakers": [{{"name": "Full Name", "confidence": "high|medium|low", "role": "Council President|Councilor|City Clerk|Department Head|Public Commenter|Presenter"}}],
  "sentiment": "positive|neutral|critical|mixed|advocacy|procedural",
  "political_signal": {{"signal_type": "one of the valid types above", "description": "...", "confidence": "high|medium|low"}} or null,
  "processing_notes": ["any notes about data quality, uncertainty, or connections"]
}}"""


def _mechanical_fallback(segment: dict, draft: dict, taxonomy: dict, error_msg: str) -> dict:
    """Produce a minimal enrichment from mechanical extraction when Claude fails."""
    from collector_utils import _infer_tags_from_departments

    depts = segment.get("detected_departments", [])
    entities = []
    for e in segment.get("detected_entities", []):
        if isinstance(e, dict):
            entities.append(e)

    speakers = []
    for name in segment.get("detected_speakers", []):
        speakers.append({"name": name, "confidence": "medium", "role": "Unknown"})

    # Build a title from agenda descriptions or auto_note
    title = segment.get("auto_note", "")
    if not title and segment.get("agenda_descriptions"):
        title = segment["agenda_descriptions"][0][:80]
    if not title:
        title = f"Segment {segment.get('segment_number', '?')} — {draft.get('meeting_type', 'meeting')}"

    return {
        "title": title,
        "summary": f"[Auto-generated] Council meeting segment from {draft.get('publication_date', 'unknown date')}.",
        "topic_tags": _infer_tags_from_departments(depts, taxonomy),
        "department_refs": depts,
        "entity_refs": entities,
        "speakers": speakers,
        "sentiment": "procedural",
        "political_signal": None,
        "processing_notes": [f"Mechanical extraction only — {error_msg}"],
    }


def enrich_segment(
    segment: dict,
    draft: dict,
    taxonomy: dict,
    registry: dict,
    api_config: dict,
    logger=None,
) -> dict:
    """Enrich a single segment via Claude API. Returns enrichment dict."""
    if anthropic is None:
        return _mechanical_fallback(segment, draft, taxonomy, "anthropic library not installed")

    prompt = _build_council_prompt(segment, draft, taxonomy, registry)

    try:
        client = anthropic.Anthropic(api_key=api_config["anthropic_api_key"])
        model = api_config.get("model", "claude-haiku-4-5-20251001")

        response = client.messages.create(
            model=model,
            max_tokens=1200,
            messages=[{"role": "user", "content": prompt}],
        )

        raw_text = response.content[0].text.strip()

        # Handle markdown-wrapped JSON
        if raw_text.startswith("```"):
            lines = raw_text.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            raw_text = "\n".join(lines)

        result = json.loads(raw_text)

        # Validate expected keys
        for key in ["title", "summary", "topic_tags", "department_refs",
                     "entity_refs", "speakers", "sentiment"]:
            if key not in result:
                result[key] = [] if isinstance(result.get(key), list) or key.endswith("s") or key.endswith("refs") else ""

        if "processing_notes" not in result:
            result["processing_notes"] = []
        if "political_signal" not in result:
            result["political_signal"] = None

        result["processing_notes"].append(
            f"Enriched by Claude ({api_config.get('model', 'haiku')}) — auto_council pipeline"
        )

        return result

    except json.JSONDecodeError as e:
        msg = f"Failed to parse Claude response as JSON: {e}"
        if logger:
            logger.warning(msg)
        return _mechanical_fallback(segment, draft, taxonomy, msg)

    except Exception as e:
        msg = f"Claude API error: {e}"
        if logger:
            logger.warning(msg)
        return _mechanical_fallback(segment, draft, taxonomy, msg)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def enrich_draft_file(draft_path: Path, taxonomy: dict, registry: dict,
                      api_config: dict, dry_run: bool = False,
                      logger=None) -> tuple[int, int, int]:
    """Enrich all substantive segments in a draft file.

    Returns (enriched_count, skipped_count, error_count).
    """
    with open(draft_path, encoding="utf-8") as f:
        draft = json.load(f)

    segments = draft.get("segments", [])
    enriched = 0
    skipped = 0
    errors = 0

    print(f"\nEnriching: {draft_path.name}")
    print(f"  Meeting: {draft.get('video_title', '?')}")
    print(f"  Segments: {len(segments)}")

    for seg in segments:
        seg_num = seg.get("segment_number", "?")

        # Skip non-substantive
        if not seg.get("substantive", False):
            skipped += 1
            continue

        # Skip already-enriched segments
        if seg.get("title", "").strip() and seg.get("summary", "").strip():
            print(f"  Seg {seg_num}: already enriched, skipping")
            skipped += 1
            continue

        if dry_run:
            print(f"  Seg {seg_num}: would enrich ({seg.get('word_count', 0)} words)")
            skipped += 1
            continue

        # Enrich via Claude
        print(f"  Seg {seg_num}: enriching ({seg.get('word_count', 0)} words)...", end=" ")
        result = enrich_segment(seg, draft, taxonomy, registry, api_config, logger)

        # Apply enrichment to segment
        is_fallback = any("Mechanical extraction only" in n for n in result.get("processing_notes", []))
        if is_fallback:
            errors += 1
            print("FALLBACK")
        else:
            print("OK")

        seg["title"] = result.get("title", "")
        seg["summary"] = result.get("summary", "")
        seg["topic_tags"] = result.get("topic_tags", [])
        seg["department_refs"] = result.get("department_refs", [])
        seg["entity_refs"] = result.get("entity_refs", [])
        seg["speakers"] = result.get("speakers", [])
        seg["sentiment"] = result.get("sentiment", "neutral")
        seg["political_signal"] = result.get("political_signal")
        seg["processing_notes"] = result.get("processing_notes", [])

        enriched += 1

    # Write enriched draft back
    if not dry_run and enriched > 0:
        with open(draft_path, "w", encoding="utf-8") as f:
            json.dump(draft, f, indent=2, ensure_ascii=False)
        print(f"  Saved enriched draft: {draft_path.name}")

    return enriched, skipped, errors


def find_unenriched_drafts() -> list[Path]:
    """Find draft files that have substantive segments without enrichment."""
    unenriched = []
    if not DRAFTS_DIR.exists():
        return unenriched

    for path in sorted(DRAFTS_DIR.glob("*_drafts.json")):
        try:
            with open(path, encoding="utf-8") as f:
                draft = json.load(f)
            for seg in draft.get("segments", []):
                if seg.get("substantive") and not seg.get("title", "").strip():
                    unenriched.append(path)
                    break
        except (json.JSONDecodeError, OSError):
            pass

    return unenriched


def main():
    args = sys.argv[1:]
    dry_run = "--dry-run" in args
    if dry_run:
        args.remove("--dry-run")

    enrich_all = "--all" in args
    if enrich_all:
        args.remove("--all")

    # Load config
    taxonomy = load_taxonomy()
    registry = load_entity_registry()
    api_config = load_api_config()
    logger = setup_logger("enrich-drafts")

    # Determine which files to process
    if enrich_all:
        draft_paths = find_unenriched_drafts()
        if not draft_paths:
            print("No unenriched drafts found.")
            return
        print(f"Found {len(draft_paths)} unenriched draft(s)")
    elif args:
        draft_paths = [Path(a) for a in args if Path(a).exists()]
        if not draft_paths:
            print("No valid draft files specified.")
            sys.exit(1)
    else:
        print("Usage: python scripts/enrich_drafts.py <draft_file> [--dry-run]")
        print("       python scripts/enrich_drafts.py --all [--dry-run]")
        sys.exit(1)

    total_enriched = 0
    total_skipped = 0
    total_errors = 0

    for path in draft_paths:
        e, s, err = enrich_draft_file(path, taxonomy, registry, api_config, dry_run, logger)
        total_enriched += e
        total_skipped += s
        total_errors += err

    print(f"\n{'=' * 50}")
    print(f"ENRICHMENT SUMMARY {'(DRY RUN)' if dry_run else ''}")
    print(f"  Files processed: {len(draft_paths)}")
    print(f"  Segments enriched: {total_enriched}")
    print(f"  Segments skipped: {total_skipped}")
    print(f"  Fallback errors: {total_errors}")


if __name__ == "__main__":
    main()
