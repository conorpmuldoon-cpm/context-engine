"""
draft_records.py — Segment YouTube transcripts and extract structured data
for semi-automated context record creation.

Reads corrected transcripts and cross-references with parsed agendas to produce
draft segment files. These drafts are then reviewed by Claude (in conversation)
to add titles, summaries, tags, and political signals before being finalized
by finalize_records.py.

Usage:
    python scripts/draft_records.py <video_id>                    # process one video
    python scripts/draft_records.py <video_id> --agenda <path>    # with explicit agenda
    python scripts/draft_records.py --all                         # all unprocessed

Outputs:
    outputs/drafts/{video_id}_drafts.json — structured segments for Claude review
"""

import json
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
TRANSCRIPTS_DIR = PROJECT_ROOT / "outputs" / "transcripts"
AGENDAS_DIR = PROJECT_ROOT / "outputs" / "agendas"
DRAFTS_DIR = PROJECT_ROOT / "outputs" / "drafts"
ENTITY_REGISTRY_PATH = PROJECT_ROOT / "config" / "entity-registry.json"
TAXONOMY_PATH = PROJECT_ROOT / "config" / "taxonomy.json"

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


# ---------------------------------------------------------------------------
# Config Loading
# ---------------------------------------------------------------------------

def load_entity_registry() -> dict:
    if not ENTITY_REGISTRY_PATH.exists():
        print(f"Warning: Entity registry not found at {ENTITY_REGISTRY_PATH}")
        return {"persons": [], "organizations": []}
    with open(ENTITY_REGISTRY_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_taxonomy() -> dict:
    if not TAXONOMY_PATH.exists():
        print(f"Warning: Taxonomy not found at {TAXONOMY_PATH}")
        return {}
    with open(TAXONOMY_PATH, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Header Parsing
# ---------------------------------------------------------------------------

def parse_header(corrected_path: Path) -> dict:
    """Extract metadata from the === header lines === of a corrected transcript."""
    header = {}
    with open(corrected_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("==="):
                break
            content = line.strip("= ").strip()

            if content.startswith("Video:"):
                header["title"] = content[len("Video:"):].strip()
            elif content.startswith("Duration:"):
                m = re.match(r"Duration:\s*~(.+?)\s*\|\s*Snippets:\s*(\d+)", content)
                if m:
                    header["duration_str"] = m.group(1).strip()
                    header["snippets"] = int(m.group(2))
            elif content.startswith("Name corrections"):
                m = re.match(r"Name corrections applied:\s*(\d+)\s*\((\d+) unique\)", content)
                if m:
                    header["corrections_count"] = int(m.group(1))
                    header["corrections_unique"] = int(m.group(2))
            elif content.startswith("Corrections:"):
                header["corrections_list"] = content[len("Corrections:"):].strip()
    return header


def parse_duration_seconds(duration_str: str) -> int:
    """'47m 33s' or '1h 06m 21s' → seconds."""
    total = 0
    for m in re.finditer(r"(\d+)h", duration_str):
        total += int(m.group(1)) * 3600
    for m in re.finditer(r"(\d+)m", duration_str):
        total += int(m.group(1)) * 60
    for m in re.finditer(r"(\d+)s", duration_str):
        total += int(m.group(1))
    return total


# ---------------------------------------------------------------------------
# Meeting Classification & Date Parsing
# ---------------------------------------------------------------------------

def classify_meeting_type(title: str) -> str:
    """Classify from video title → 'study_session' | 'regular_meeting' | 'committee_meeting'."""
    t = title.lower()
    if "study session" in t:
        return "study_session"
    if "regular meeting" in t:
        return "regular_meeting"
    if "public meeting" in t:
        return "regular_meeting"
    if "committee" in t:
        return "committee_meeting"
    return "committee_meeting"  # fallback for unknown types


def parse_date_from_title(title: str) -> date | None:
    """Parse 'Wednesday March 4th 2026' from a video title."""
    m = re.search(
        r"(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s*(\d{4})",
        title, re.IGNORECASE,
    )
    if not m:
        return None
    try:
        return date(int(m.group(3)), MONTHS[m.group(1).lower()], int(m.group(2)))
    except (ValueError, KeyError):
        return None


# ---------------------------------------------------------------------------
# Agenda Matching
# ---------------------------------------------------------------------------

def find_matching_agenda(pub_date: date | None, meeting_type: str,
                         agenda_override: str | None = None) -> tuple[Path | None, str | None]:
    """Find the agenda JSON matching a meeting date.

    Study sessions on Wednesday → next Monday's agenda.
    Study sessions/regular meetings on Monday → same date.
    Committee meetings → no agenda.

    Returns (path, date_string) or (None, None).
    """
    if agenda_override:
        p = Path(agenda_override)
        if not p.is_absolute():
            p = PROJECT_ROOT / p
        if p.exists():
            m = re.search(r"(\d{4}-\d{2}-\d{2})", p.name)
            return p, (m.group(1) if m else None)
        print(f"  Warning: Specified agenda not found: {p}")
        return None, None

    if meeting_type == "committee_meeting" or pub_date is None:
        return None, None

    # Determine the target agenda date
    weekday = pub_date.weekday()  # 0=Mon
    if weekday == 0:
        target = pub_date
    else:
        days_ahead = (7 - weekday) % 7
        if days_ahead == 0:
            days_ahead = 7
        target = pub_date + timedelta(days=days_ahead)

    # Search for the agenda file, allowing ±2 days of slack
    for delta in [0, 1, -1, 2, -2]:
        candidate = target + timedelta(days=delta)
        path = AGENDAS_DIR / f"{candidate.isoformat()}_agenda.json"
        if path.exists():
            return path, candidate.isoformat()

    return None, None


def load_agenda(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Transcript Loading
# ---------------------------------------------------------------------------

def load_transcript_lines(corrected_path: Path) -> list[dict]:
    """Parse [HH:MM:SS] lines → [{seconds, text}, ...]."""
    lines = []
    with open(corrected_path, encoding="utf-8") as f:
        for raw in f:
            raw = raw.rstrip("\n")
            if raw.startswith("===") or not raw.strip():
                continue
            m = re.match(r"\[(\d{2}):(\d{2}):(\d{2})\]\s*(.*)", raw)
            if m:
                seconds = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
                lines.append({"seconds": seconds, "text": m.group(4)})
    return lines


# ---------------------------------------------------------------------------
# Item Reference Detection
# ---------------------------------------------------------------------------

def detect_item_references(lines: list[dict]) -> list[tuple[int, list[int]]]:
    """Find agenda item mentions in transcript text.

    Joins all lines into a single text to catch cross-line patterns
    (e.g., 'item' on one line and '12 to 22' on the next).

    Returns [(seconds, [item_numbers]), ...] sorted by timestamp,
    with nearby mentions (within 10 s) merged.
    """
    # Build full text, tracking character offsets → timestamps
    full_text = ""
    offset_to_ts: list[tuple[int, int]] = []  # (char_offset, seconds)
    for line in lines:
        offset_to_ts.append((len(full_text), line["seconds"]))
        full_text += line["text"] + " "

    def ts_at(char_pos: int) -> int:
        """Find the timestamp for a character position in full_text."""
        best_ts = offset_to_ts[0][1] if offset_to_ts else 0
        for offset, ts in offset_to_ts:
            if offset > char_pos:
                break
            best_ts = ts
        return best_ts

    raw_refs: list[tuple[int, set[int]]] = []

    # Ranges: "item(s) 12 through/to/and 17"
    for m in re.finditer(
        r"\bitems?\s+(?:number\s+)?(\d{1,3})\s+(?:through|thru|to|and)\s+(\d{1,3})\b",
        full_text, re.IGNORECASE,
    ):
        lo, hi = int(m.group(1)), int(m.group(2))
        raw_refs.append((ts_at(m.start()), set(range(lo, hi + 1))))

    # Singles: "item 12" or "item number 12"
    for m in re.finditer(r"\bitem\s+(?:number\s+)?(\d{1,3})\b", full_text, re.IGNORECASE):
        raw_refs.append((ts_at(m.start()), {int(m.group(1))}))

    # "number N is/was/has..." (standalone reference to item by number)
    for m in re.finditer(
        r"\bnumber\s+(\d{1,3})\s+(?:is|was|has|relates|deals|involves)",
        full_text, re.IGNORECASE,
    ):
        raw_refs.append((ts_at(m.start()), {int(m.group(1))}))

    # Sort by timestamp
    raw_refs.sort(key=lambda r: r[0])

    # Merge refs within 10 seconds of each other
    merged: list[tuple[int, list[int]]] = []
    for seconds, items in raw_refs:
        if merged and seconds - merged[-1][0] < 10:
            combined = set(merged[-1][1])
            combined.update(items)
            merged[-1] = (merged[-1][0], sorted(combined))
        else:
            merged.append((seconds, sorted(items)))
    return merged


# ---------------------------------------------------------------------------
# Segmentation
# ---------------------------------------------------------------------------

def _format_ts(seconds: int) -> str:
    """Seconds → [HH:MM:SS]."""
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"[{h:02d}:{m:02d}:{s:02d}]"


def build_segment_text(lines: list[dict], start: int, end: int) -> str:
    """Return timestamped text for lines in [start, end)."""
    return "\n".join(
        f"{_format_ts(l['seconds'])} {l['text']}"
        for l in lines
        if start <= l["seconds"] < end
    )


def _word_count(text: str) -> int:
    cleaned = re.sub(r"\[\d{2}:\d{2}:\d{2}\]", "", text)
    return len(cleaned.split())


def segment_by_items(lines: list[dict], item_refs: list[tuple[int, list[int]]],
                     agenda: dict | None) -> list[dict]:
    """Segment a study-session or regular-meeting transcript by item references."""
    if not lines:
        return []

    first_ts = lines[0]["seconds"]
    last_ts = lines[-1]["seconds"]
    segments: list[dict] = []

    # Opening segment (before first item mention)
    if item_refs and item_refs[0][0] > first_ts + 15:
        text = build_segment_text(lines, first_ts, item_refs[0][0])
        segments.append({
            "start_seconds": first_ts,
            "end_seconds": item_refs[0][0],
            "agenda_items": [],
            "agenda_descriptions": [],
            "raw_content": text,
            "substantive": False,
            "auto_note": "Opening/procedural segment",
        })

    # One segment per item-reference group
    for i, (ts, items) in enumerate(item_refs):
        end_ts = item_refs[i + 1][0] if i + 1 < len(item_refs) else last_ts + 5
        text = build_segment_text(lines, ts, end_ts)
        wc = _word_count(text)
        duration = end_ts - ts

        # Look up agenda descriptions
        descs: list[str] = []
        if agenda:
            for item_num in items:
                for ai in agenda.get("items", []):
                    if ai.get("number") == item_num and ai.get("description"):
                        descs.append(ai["description"][:500])
                        break

        is_substantive = duration >= 30 and wc >= 50

        segments.append({
            "start_seconds": ts,
            "end_seconds": end_ts,
            "agenda_items": items,
            "agenda_descriptions": descs,
            "raw_content": text,
            "substantive": is_substantive,
            "auto_note": "" if is_substantive else "Brief — likely 'ready' confirmation",
        })

    # Fallback: no item refs → whole transcript as one segment
    if not item_refs:
        text = build_segment_text(lines, first_ts, last_ts + 5)
        segments.append({
            "start_seconds": first_ts,
            "end_seconds": last_ts + 5,
            "agenda_items": [],
            "agenda_descriptions": [],
            "raw_content": text,
            "substantive": True,
            "auto_note": "No item references detected — full transcript as single segment",
        })

    return segments


def segment_regular_meeting(lines: list[dict], item_refs: list[tuple[int, list[int]]],
                            agenda: dict | None) -> list[dict]:
    """Segment a regular meeting into opening + bulk procedural + substantive discussions.

    Most regular meeting items are rapid voice votes (<60s each).  Group those into
    one bulk segment listing all item numbers.  Only break out items with >=60s of
    actual discussion as separate substantive segments.
    """
    if not lines:
        return []

    first_ts = lines[0]["seconds"]
    last_ts = lines[-1]["seconds"]
    segments: list[dict] = []
    DISCUSS_THRESHOLD = 60  # seconds — items with more discussion get their own segment

    # Opening segment (before first item mention)
    if item_refs and item_refs[0][0] > first_ts + 15:
        text = build_segment_text(lines, first_ts, item_refs[0][0])
        segments.append({
            "start_seconds": first_ts,
            "end_seconds": item_refs[0][0],
            "agenda_items": [],
            "agenda_descriptions": [],
            "raw_content": text,
            "substantive": False,
            "auto_note": "Opening/procedural segment",
        })

    # Classify each item ref as procedural or substantive by duration
    procedural_items: list[int] = []  # item numbers grouped into bulk
    procedural_start: int | None = None
    procedural_end: int = 0

    def _flush_procedural():
        """Emit accumulated procedural items as one bulk segment."""
        nonlocal procedural_items, procedural_start, procedural_end
        if not procedural_items:
            return
        text = build_segment_text(lines, procedural_start, procedural_end)
        unique_items = list(dict.fromkeys(procedural_items))  # preserve order, dedup
        descs = _lookup_agenda_descs(unique_items, agenda)
        segments.append({
            "start_seconds": procedural_start,
            "end_seconds": procedural_end,
            "agenda_items": unique_items,
            "agenda_descriptions": descs,
            "raw_content": text,
            "substantive": False,
            "auto_note": f"Bulk procedural — {len(unique_items)} items voted with minimal discussion",
        })
        procedural_items.clear()
        procedural_start = None
        procedural_end = 0

    for i, (ts, items) in enumerate(item_refs):
        end_ts = item_refs[i + 1][0] if i + 1 < len(item_refs) else last_ts + 5
        duration = end_ts - ts

        if duration < DISCUSS_THRESHOLD:
            # Accumulate into procedural bulk
            if procedural_start is None:
                procedural_start = ts
            procedural_end = end_ts
            procedural_items.extend(items)
        else:
            # Flush any accumulated procedural items first
            _flush_procedural()
            # Emit substantive segment
            text = build_segment_text(lines, ts, end_ts)
            descs = _lookup_agenda_descs(items, agenda)
            segments.append({
                "start_seconds": ts,
                "end_seconds": end_ts,
                "agenda_items": items,
                "agenda_descriptions": descs,
                "raw_content": text,
                "substantive": True,
                "auto_note": "",
            })

    _flush_procedural()  # flush any remaining

    # Fallback: no item refs → whole transcript as one segment
    if not item_refs:
        text = build_segment_text(lines, first_ts, last_ts + 5)
        segments.append({
            "start_seconds": first_ts,
            "end_seconds": last_ts + 5,
            "agenda_items": [],
            "agenda_descriptions": [],
            "raw_content": text,
            "substantive": True,
            "auto_note": "No item references detected — full transcript as single segment",
        })

    return segments


def _lookup_agenda_descs(items: list[int], agenda: dict | None) -> list[str]:
    """Look up agenda descriptions for a list of item numbers."""
    descs: list[str] = []
    if not agenda:
        return descs
    for item_num in items:
        for ai in agenda.get("items", []):
            if ai.get("number") == item_num and ai.get("description"):
                descs.append(ai["description"][:500])
                break
    return descs


def segment_committee_meeting(lines: list[dict]) -> list[dict]:
    """Time-based segmentation for committee meetings (no agenda item numbers)."""
    if not lines:
        return []

    first_ts = lines[0]["seconds"]
    last_ts = lines[-1]["seconds"]
    total = last_ts - first_ts

    TARGET = 600  # 10 minutes

    if total <= TARGET * 1.5:
        text = build_segment_text(lines, first_ts, last_ts + 5)
        return [{
            "start_seconds": first_ts,
            "end_seconds": last_ts + 5,
            "agenda_items": [],
            "agenda_descriptions": [],
            "raw_content": text,
            "substantive": True,
            "auto_note": "Short meeting — single segment",
        }]

    # Collect potential natural boundaries
    natural_bounds: list[int] = []
    for i in range(1, len(lines)):
        gap = lines[i]["seconds"] - lines[i - 1]["seconds"]
        if gap > 8:
            natural_bounds.append(lines[i]["seconds"])
        if re.search(
            r"(?:my name is|I'm .+ from|good (?:afternoon|morning|evening))",
            lines[i]["text"], re.IGNORECASE,
        ):
            natural_bounds.append(lines[i]["seconds"])

    # Pick boundaries nearest to ideal 10-min intervals
    n_seg = max(2, round(total / TARGET))
    ideal_times = [first_ts + (k * total // n_seg) for k in range(1, n_seg)]

    chosen: list[int] = []
    for ideal in ideal_times:
        nearby = [b for b in natural_bounds if abs(b - ideal) < 120]
        if nearby:
            best = min(nearby, key=lambda b: abs(b - ideal))
        else:
            best = min(lines, key=lambda l: abs(l["seconds"] - ideal))["seconds"]
        if best not in chosen:
            chosen.append(best)
    chosen.sort()

    # Build segments
    bounds = [first_ts] + chosen + [last_ts + 5]
    segments: list[dict] = []
    for i in range(len(bounds) - 1):
        text = build_segment_text(lines, bounds[i], bounds[i + 1])
        segments.append({
            "start_seconds": bounds[i],
            "end_seconds": bounds[i + 1],
            "agenda_items": [],
            "agenda_descriptions": [],
            "raw_content": text,
            "substantive": True,
            "auto_note": "",
        })
    return segments


# ---------------------------------------------------------------------------
# Entity / Department / Amount Extraction
# ---------------------------------------------------------------------------

def extract_entities(text: str, registry: dict) -> list[dict]:
    """Detect known persons and organizations from the entity registry."""
    entities: list[dict] = []
    t = text.lower()

    for person in registry.get("persons", []):
        if person["canonical_name"].lower() in t:
            entities.append({
                "name": person["canonical_name"],
                "type": "person",
                "canonical_id": person["canonical_id"],
            })

    for org in registry.get("organizations", []):
        if org["canonical_name"].lower() in t:
            entities.append({
                "name": org["canonical_name"],
                "type": "organization",
                "canonical_id": org["canonical_id"],
            })

    return entities


def extract_departments(text: str, taxonomy: dict) -> list[str]:
    """Detect department names using taxonomy synonym resolution."""
    depts: list[str] = []
    t = text.lower()

    for entry in taxonomy.get("synonym_resolution", {}).get("departments", []):
        canonical = entry["canonical_ref"]
        if canonical.lower() in t:
            if canonical not in depts:
                depts.append(canonical)
            continue
        for variant in entry.get("variants", []):
            if len(variant) <= 5:
                # Short variants need word-boundary matching to avoid
                # substring false positives (e.g. "tech" in "technically").
                # All-caps abbreviations (IT, HR, DPW) also need case-sensitivity
                # to avoid matching common words like "it".
                flags = 0 if (variant.isupper() and len(variant) <= 3) else re.IGNORECASE
                if re.search(r"\b" + re.escape(variant) + r"\b", text, flags):
                    if canonical not in depts:
                        depts.append(canonical)
                    break
            elif variant.lower() in t:
                if canonical not in depts:
                    depts.append(canonical)
                break
    return depts


def extract_systems(text: str, taxonomy: dict) -> list[dict]:
    """Detect system references using taxonomy synonym resolution."""
    systems: list[dict] = []
    t = text.lower()

    for entry in taxonomy.get("synonym_resolution", {}).get("systems", []):
        cn = entry["canonical_name"]
        # Word-boundary match for short canonical names (e.g. "IPS") to avoid
        # false positives like "tips", "trips", "professorships"
        if len(cn) <= 3:
            flags = 0 if cn.isupper() else re.IGNORECASE
            found = bool(re.search(r"\b" + re.escape(cn) + r"\b", text, flags))
        else:
            found = cn.lower() in t
        if not found:
            for variant in entry.get("variants", []):
                if len(variant) <= 5:
                    flags = 0 if (variant.isupper() and len(variant) <= 3) else re.IGNORECASE
                    if re.search(r"\b" + re.escape(variant) + r"\b", text, flags):
                        found = True
                        break
                elif variant.lower() in t:
                    found = True
                    break
        if found:
            systems.append({
                "name": entry["canonical_name"],
                "type": "system",
                "canonical_id": entry["canonical_id"],
            })
    return systems


def extract_amounts(text: str) -> list[str]:
    amounts = re.findall(r"\$[\d,]+(?:\.\d{2})?", text)
    return list(dict.fromkeys(amounts))


def detect_speakers(text: str, registry: dict) -> list[str]:
    """List known persons whose names appear in the segment text."""
    speakers: list[str] = []
    t = text.lower()
    for person in registry.get("persons", []):
        if person["canonical_name"].lower() in t:
            speakers.append(person["canonical_name"])
    return speakers


# ---------------------------------------------------------------------------
# Draft Builder
# ---------------------------------------------------------------------------

def build_draft(video_id: str, header: dict, meeting_type: str,
                pub_date: date | None, agenda_date: str | None,
                agenda: dict | None, segments: list[dict],
                registry: dict, taxonomy: dict) -> dict:
    """Construct the full draft JSON with mechanical extractions."""

    draft_segments: list[dict] = []
    for i, seg in enumerate(segments):
        text = seg["raw_content"]
        entities = extract_entities(text, registry) + extract_systems(text, taxonomy)
        depts = extract_departments(text, taxonomy)
        amounts = extract_amounts(text)
        speakers = detect_speakers(text, registry)

        draft_segments.append({
            "segment_number": i + 1,
            "start_seconds": seg["start_seconds"],
            "end_seconds": seg["end_seconds"],
            "duration_seconds": seg["end_seconds"] - seg["start_seconds"],
            "word_count": _word_count(text),
            "agenda_items": seg.get("agenda_items", []),
            "agenda_descriptions": seg.get("agenda_descriptions", []),
            "raw_content": text,
            "detected_entities": entities,
            "detected_departments": depts,
            "detected_amounts": amounts,
            "detected_speakers": speakers,
            "substantive": seg["substantive"],
            "auto_note": seg.get("auto_note", ""),
            # ---- Fields for Claude to fill ----
            "title": "",
            "summary": "",
            "topic_tags": [],
            "sentiment": "",
            "political_signal": None,
            "speakers": [],
            "entity_refs": [],
            "department_refs": [],
            "cluster_ids": [],
            "processing_notes": [],
        })

    # Summary stats
    all_ents: set[str] = set()
    all_depts: set[str] = set()
    all_amounts: list[str] = []
    for s in draft_segments:
        for e in s["detected_entities"]:
            all_ents.add(e["name"])
        all_depts.update(s["detected_departments"])
        all_amounts.extend(s["detected_amounts"])

    n_sub = sum(1 for s in draft_segments if s["substantive"])

    return {
        "video_id": video_id,
        "video_title": header.get("title", ""),
        "meeting_type": meeting_type,
        "publication_date": pub_date.isoformat() if pub_date else None,
        "agenda_date": agenda_date,
        "agenda_available": agenda is not None,
        "duration_seconds": parse_duration_seconds(header.get("duration_str", "")),
        "corrections_applied": header.get("corrections_list", ""),
        "segments": draft_segments,
        "summary_stats": {
            "total_segments": len(draft_segments),
            "substantive_segments": n_sub,
            "procedural_segments": len(draft_segments) - n_sub,
            "entities_found": sorted(all_ents),
            "departments_found": sorted(all_depts),
            "amounts_found": list(dict.fromkeys(all_amounts)),
        },
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_video(video_id: str, agenda_override: str | None = None) -> dict | None:
    """Process a single video transcript into draft segments."""

    corrected_path = TRANSCRIPTS_DIR / f"{video_id}_corrected.txt"
    if not corrected_path.exists():
        # Support renamed files: {date}_{type}_{video_id}_corrected.txt
        matches = list(TRANSCRIPTS_DIR.glob(f"*_{video_id}_corrected.txt"))
        if matches:
            corrected_path = matches[0]
        else:
            print(f"  Error: Transcript not found: {corrected_path.name}")
            return None

    header = parse_header(corrected_path)
    title = header.get("title", "")
    if not title:
        print(f"  Error: Could not parse title from {corrected_path.name}")
        return None

    meeting_type = classify_meeting_type(title)
    pub_date = parse_date_from_title(title)

    agenda_path, agenda_date = find_matching_agenda(pub_date, meeting_type, agenda_override)
    agenda = load_agenda(agenda_path) if agenda_path else None

    lines = load_transcript_lines(corrected_path)
    if not lines:
        print(f"  Error: No transcript lines in {corrected_path.name}")
        return None

    # Status
    print(f"\n=== {video_id}: {title} ===")
    print(f"  Type: {meeting_type} | Date: {pub_date}")
    if agenda:
        print(f"  Agenda: {agenda_date} ({agenda.get('item_count', '?')} items)")
    else:
        print(f"  Agenda: not available")

    # Segment
    if meeting_type == "committee_meeting":
        segments = segment_committee_meeting(lines)
    else:
        item_refs = detect_item_references(lines)
        print(f"  Item references: {len(item_refs)}")
        if meeting_type == "regular_meeting":
            segments = segment_regular_meeting(lines, item_refs, agenda)
        else:
            segments = segment_by_items(lines, item_refs, agenda)

    # Extract & build draft
    registry = load_entity_registry()
    taxonomy = load_taxonomy()
    draft = build_draft(video_id, header, meeting_type, pub_date, agenda_date,
                        agenda, segments, registry, taxonomy)

    # Write
    DRAFTS_DIR.mkdir(parents=True, exist_ok=True)
    out = DRAFTS_DIR / f"{video_id}_drafts.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(draft, f, indent=2, ensure_ascii=False)

    stats = draft["summary_stats"]
    print(f"  Segments: {stats['total_segments']} ({stats['substantive_segments']} substantive, {stats['procedural_segments']} procedural)")
    if stats["entities_found"]:
        print(f"  Entities: {', '.join(stats['entities_found'])}")
    if stats["departments_found"]:
        print(f"  Departments: {', '.join(stats['departments_found'])}")
    if stats["amounts_found"]:
        print(f"  Amounts: {', '.join(stats['amounts_found'][:5])}")
    print(f"  Saved: {out.relative_to(PROJECT_ROOT)}")
    return draft


def main():
    args = sys.argv[1:]
    if not args:
        print("Usage:")
        print("  python scripts/draft_records.py <video_id>")
        print("  python scripts/draft_records.py <video_id> --agenda <path>")
        print("  python scripts/draft_records.py --all")
        sys.exit(1)

    # Parse --agenda flag
    agenda_override = None
    if "--agenda" in args:
        idx = args.index("--agenda")
        if idx + 1 < len(args):
            agenda_override = args[idx + 1]
            args = args[:idx] + args[idx + 2:]
        else:
            print("Error: --agenda requires a path")
            sys.exit(1)

    # Handle --all
    if "--all" in args:
        args.remove("--all")
        video_ids = []
        for f in sorted(TRANSCRIPTS_DIR.glob("*_corrected.txt")):
            vid = f.stem.replace("_corrected", "")
            if not (DRAFTS_DIR / f"{vid}_drafts.json").exists():
                video_ids.append(vid)
        if not video_ids:
            print("All transcripts already have drafts.")
            sys.exit(0)
        print(f"Processing {len(video_ids)} transcripts without drafts...")
    else:
        video_ids = [a for a in args if not a.startswith("--")]

    if not video_ids:
        print("No video IDs specified.")
        sys.exit(1)

    # Load config once for status
    registry = load_entity_registry()
    taxonomy = load_taxonomy()
    print(f"Entity registry: {len(registry.get('persons', []))} persons, {len(registry.get('organizations', []))} orgs")

    success = 0
    errors = 0
    for vid in video_ids:
        try:
            result = process_video(vid, agenda_override)
            if result:
                success += 1
            else:
                errors += 1
        except Exception as e:
            print(f"  Error processing {vid}: {e}")
            import traceback
            traceback.print_exc()
            errors += 1

    if len(video_ids) > 1:
        print(f"\n=== Batch Complete: {success} processed, {errors} errors ===")
        print(f"Drafts saved to: {DRAFTS_DIR.relative_to(PROJECT_ROOT)}/")


if __name__ == "__main__":
    main()
