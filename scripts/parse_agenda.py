"""
parse_agenda.py — Parse a Syracuse Common Council agenda PDF into structured JSON.

Targets the short agenda PDFs (~5-8 pages) posted on syr.gov, NOT the
hundreds-of-pages agenda book PDFs.

Usage:
    python scripts/parse_agenda.py "https://www.syr.gov/.../02.23.2026-agenda.pdf"
    python scripts/parse_agenda.py outputs/02.23.2026-agenda.pdf

Outputs (in outputs/agendas/):
    {date}_agenda.json  — full structured parse
    {date}_names.json   — extracted names for transcript matching
"""

import io
import json
import re
import shutil
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
ENTITY_REGISTRY = PROJECT_ROOT / "config" / "entity-registry.json"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "agendas"

MAX_FILE_SIZE = 2 * 1024 * 1024  # 2 MB — reject anything larger (likely agenda book)


# ---------------------------------------------------------------------------
# Dependency checks
# ---------------------------------------------------------------------------

def check_dependencies():
    """Verify Tesseract and required Python packages are available."""
    try:
        import fitz  # noqa: F401
    except ImportError:
        print("Error: PyMuPDF is not installed.")
        print("  Install it with: python -m pip install PyMuPDF")
        sys.exit(1)

    try:
        import pytesseract  # noqa: F401
    except ImportError:
        print("Error: pytesseract is not installed.")
        print("  Install it with: python -m pip install pytesseract")
        print("  You also need Tesseract OCR installed on your system.")
        print("  On Windows: winget install UB-Mannheim.TesseractOCR")
        sys.exit(1)

    import pytesseract as pt
    # Auto-detect Tesseract on Windows
    tesseract_path = shutil.which("tesseract")
    if not tesseract_path:
        import os
        for candidate in [
            r"C:\Program Files\Tesseract-OCR\tesseract.exe",
            r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
            r"C:\Users\{}\AppData\Local\Programs\Tesseract-OCR\tesseract.exe".format(
                os.environ.get("USERNAME", "")
            ),
        ]:
            if os.path.exists(candidate):
                tesseract_path = candidate
                break

    if not tesseract_path:
        print("Error: Tesseract OCR engine not found.")
        print("  On Windows: winget install UB-Mannheim.TesseractOCR")
        print("  Then restart your terminal.")
        sys.exit(1)

    pt.pytesseract.tesseract_cmd = tesseract_path


# ---------------------------------------------------------------------------
# PDF acquisition and OCR
# ---------------------------------------------------------------------------

def download_pdf(url: str) -> Path:
    """Download PDF to a temp file, return the path."""
    import urllib.request
    print(f"Downloading: {url}")
    tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    try:
        urllib.request.urlretrieve(url, tmp.name)
    except Exception as e:
        print(f"Error downloading PDF: {e}")
        print("  Check the URL and try again.")
        sys.exit(1)
    size = Path(tmp.name).stat().st_size
    if size > MAX_FILE_SIZE:
        print(f"Error: File is {size/1024/1024:.1f} MB — too large.")
        print("  This might be the full agenda book instead of the short agenda.")
        print("  The short agenda PDFs are typically under 1 MB.")
        Path(tmp.name).unlink()
        sys.exit(1)
    print(f"  Downloaded ({size/1024:.0f} KB)")
    return Path(tmp.name)


def ocr_pdf(pdf_path: Path) -> str:
    """Render each page at 300 DPI and OCR with Tesseract. Returns full text."""
    import fitz
    import pytesseract
    from PIL import Image

    doc = fitz.open(str(pdf_path))
    print(f"  Pages: {len(doc)}")

    all_text = []
    for i in range(len(doc)):
        pix = doc[i].get_pixmap(dpi=300)
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        text = pytesseract.image_to_string(img)
        all_text.append(text)
        print(f"  OCR page {i+1}/{len(doc)} done")

    doc.close()
    return "\n\n".join(all_text)


# ---------------------------------------------------------------------------
# Text parsing
# ---------------------------------------------------------------------------

def parse_meeting_header(text: str) -> dict:
    """Extract meeting type, date, and time from the header."""
    info = {"meeting_type": None, "date": None, "time": None, "roll_call": None}

    # Meeting type and date — look for "REGULAR MEETING – FEBRUARY 23, 2026" etc.
    # OCR often renders em-dash as various characters
    m = re.search(
        r"(REGULAR MEETING|STUDY SESSION|SPECIAL MEETING|COMMITTEE MEETING)"
        r"[^\n]*?"
        r"([A-Z]+)\s+(\d{1,2}),?\s+(\d{4})",
        text,
    )
    if m:
        info["meeting_type"] = m.group(1).title()
        month_str = m.group(2)
        day = int(m.group(3))
        year = int(m.group(4))
        months = {
            "JANUARY": 1, "FEBRUARY": 2, "MARCH": 3, "APRIL": 4,
            "MAY": 5, "JUNE": 6, "JULY": 7, "AUGUST": 8,
            "SEPTEMBER": 9, "OCTOBER": 10, "NOVEMBER": 11, "DECEMBER": 12,
        }
        month_num = months.get(month_str, 0)
        if month_num:
            info["date"] = f"{year}-{month_num:02d}-{day:02d}"

    # Time
    m = re.search(r"(\d{1,2}:\d{2}\s*[AP]\.?M\.?)", text)
    if m:
        info["time"] = m.group(1)

    # Roll call
    m = re.search(r"Roll Call.*?(\d+)", text)
    if m:
        info["roll_call"] = int(m.group(1))

    return info


def parse_sponsor_sections(text: str) -> list[dict]:
    """Split text into sponsor sections and extract items from each."""
    # Find all "BY COUNCILOR/PRESIDENT ..." markers
    sponsor_pattern = re.compile(
        r"BY\s+(COUNCILOR|PRESIDENT|COUNCILORS)\s+([^:]+):",
        re.IGNORECASE,
    )

    sections = []
    matches = list(sponsor_pattern.finditer(text))

    for idx, match in enumerate(matches):
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        section_text = text[start:end]

        # Parse sponsor name(s)
        sponsor_raw = match.group(2).strip()
        # Clean up multi-sponsor lines like "NAVE & CALDWELL; PRESIDENT PANIAGUA & ALL COUNCILORS"
        sponsors = parse_sponsor_names(sponsor_raw)

        # Parse items in this section
        items = parse_items(section_text, sponsors)
        sections.extend(items)

    return sections


def parse_sponsor_names(raw: str) -> list[str]:
    """Extract individual sponsor names from a sponsor line."""
    # Remove "ALL COUNCILORS" type phrases
    cleaned = re.sub(r"\b(ALL COUNCILORS?|PRESIDENT)\b", "", raw, flags=re.IGNORECASE)
    # Split on &, ;, and commas
    parts = re.split(r"[&;,]", cleaned)
    names = []
    for part in parts:
        name = part.strip().rstrip(":").strip()
        if name and len(name) > 1:
            names.append(name.title())
    return names


def parse_items(section_text: str, sponsors: list[str]) -> list[dict]:
    """Extract numbered agenda items from a section."""
    items = []

    # Split on item numbers — numbers at start of line or after blank line
    # Item numbers appear as "9." or "10." or sometimes "9," due to OCR
    item_splits = re.split(r"\n\s*(\d{1,3})[.,]\s", section_text)

    # item_splits[0] is text before first item (often vote notes, procedural text)
    # Then alternating: item_number, item_text, item_number, item_text, ...
    i = 1
    while i < len(item_splits) - 1:
        item_num_str = item_splits[i].strip()
        item_text = item_splits[i + 1].strip()

        try:
            item_num = int(item_num_str)
        except ValueError:
            i += 2
            continue

        # Clean up the item text
        item_text = clean_item_text(item_text)

        # Extract vote from the text (appears as 8-0, 7-0, 4-4, WD, H, etc.)
        vote = extract_vote(item_text)

        # Extract dollar amounts
        amounts = extract_amounts(item_text)

        # Extract organizations
        orgs = extract_organizations(item_text)

        # Extract people
        people = extract_people(item_text)

        # Extract departments
        departments = extract_departments(item_text)

        # Build description (first ~200 chars, cleaned)
        description = build_description(item_text)

        items.append({
            "number": item_num,
            "sponsors": sponsors,
            "description": description,
            "full_text": item_text,
            "vote": vote,
            "organizations": orgs,
            "people": people,
            "departments": departments,
            "amounts": amounts,
        })

        i += 2

    return items


def clean_item_text(text: str) -> str:
    """Clean OCR artifacts from item text."""
    # Remove page numbers that appear at right margin (standalone 2-3 digit numbers)
    text = re.sub(r"\n\s*\d{2,3}\s*$", "", text, flags=re.MULTILINE)
    # Remove form feed and extra whitespace
    text = re.sub(r"\f", "", text)
    # Normalize whitespace
    text = re.sub(r"\n\s*\n", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    # Replace OCR em-dash artifacts
    text = text.replace("�", "—")
    return text.strip()


def extract_vote(text: str) -> str | None:
    """Extract vote count from text. Returns e.g. '8-0', '4-4', 'WD', 'H', or None."""
    # Look for vote patterns — standalone on a line or at the start
    m = re.search(r"\b(\d-\d)\b", text)
    if m:
        return m.group(1)
    if re.search(r"\bWD\b", text):
        return "WD"
    if re.search(r"\b([Hh]eld)\b", text):
        return "H"
    return None


def extract_amounts(text: str) -> list[str]:
    """Extract dollar amounts from text."""
    amounts = re.findall(r"\$[\d,]+(?:\.\d{2})?(?:/\w+)?", text)
    return list(dict.fromkeys(amounts))  # deduplicate preserving order


def extract_organizations(text: str) -> list[str]:
    """Extract organization names from item text using pattern matching."""
    orgs = []

    # Normalize line breaks within the text so multi-line org names are joined
    normalized = re.sub(r"\n\s*", " ", text)

    # "With [Org Name]," or "With [Org Name], for"
    with_matches = re.findall(
        r"With\s+([A-Z][A-Za-z&,.\s]+?)(?:,\s+(?:for|formerly|LLC|Inc|PC|D\.P\.C)|\s+for\s)",
        normalized,
    )
    for org in with_matches:
        org = org.strip().rstrip(",")
        if len(org) > 3 and org not in orgs:
            orgs.append(org)

    # "From [Org Name]," pattern
    from_matches = re.findall(
        r"From\s+(?:the\s+)?([A-Z][A-Za-z&,.\s]+?)(?:,\s+(?:a|funds|for))",
        normalized,
    )
    for org in from_matches:
        org = org.strip().rstrip(",")
        if len(org) > 3 and org not in orgs:
            orgs.append(org)

    # "To [Org Name], Inc." or "To [Org Name], LLC"
    to_matches = re.findall(
        r"To\s+([A-Z][A-Za-z&,.\s]+?(?:Inc|LLC|Group|Corp))",
        normalized,
    )
    for org in to_matches:
        org = org.strip().rstrip(",")
        if len(org) > 3 and org not in orgs:
            orgs.append(org)

    return orgs


def extract_people(text: str) -> list[str]:
    """Extract people names from item text."""
    people = []

    # "Hon. [Name]" pattern — e.g. "Hon. William B. Magnarelli"
    for m in re.finditer(r"Hon\.\s+([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+)", text):
        people.append(m.group(1))

    # "Dr. [Name]" pattern — e.g. "Dr. Mohamed Khater", "Dr. Linda LeMura"
    for m in re.finditer(r"Dr\.\s+([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-zA-Z]+)", text):
        people.append(m.group(1))

    # "Officer [First] [Last]" pattern
    for m in re.finditer(r"Officer\s+([A-Z][a-z]+\s+[A-Z][a-z]+)", text):
        people.append(m.group(1))

    # "Ms./Mr./Mrs. [First] [Last]" pattern
    for m in re.finditer(r"(?:Ms|Mr|Mrs)\.\s+([A-Z][a-z]+\s+[A-Z][a-z]+)", text):
        people.append(m.group(1))

    # Honoring [Name] pattern — e.g. "Honoring Carmelita Sapp-Walker"
    # Skip titles like "Syracuse Police Officer" before the actual name
    for m in re.finditer(
        r"Honoring\s+(?:Syracuse\s+)?(?:Police\s+)?(?:Officer\s+)?(?:Fire\s+)?(?:Chief\s+)?"
        r"([A-Z][a-z]+(?:\s+[A-Z][a-zA-Z-]+){1,3})",
        text,
    ):
        people.append(m.group(1))

    # Deduplicate, then remove names that are substrings of longer names
    unique = list(dict.fromkeys(people))
    filtered = []
    for name in unique:
        is_substring = any(
            name != other and name in other for other in unique
        )
        if not is_substring:
            filtered.append(name)
    return filtered


def extract_departments(text: str) -> list[str]:
    """Extract department references from item text."""
    depts = []

    # Known Syracuse city departments — match these specifically
    known_depts = [
        "Department of Public Works",
        "Department of Permits and Development",
        "Department of Parks, Recreation & Youth Programs",
        "Department of Personnel and Labor Relations",
        "Department of Engineering",
        "Department of Finance",
        "Department of Analytics, Performance & Innovation",
        "Department of Police",
        "Division of Planning & Sustainability",
        "Information Technology",
        "Syracuse Police Department",
        "Syracuse Fire Department",
        "City Payment Center",
        "City Dog Control and Police Department",
    ]
    for dept in known_depts:
        # Case-insensitive search but with word boundaries
        if re.search(re.escape(dept), text, re.IGNORECASE):
            if dept not in depts:
                depts.append(dept)

    # Also catch "Department of [X]" patterns not in the known list
    for m in re.finditer(r"(?:Department|Division) of ([A-Z][A-Za-z,& ]{3,40}?)(?:\.|,|\.| for | to | Total)", text):
        dept_name = m.group(1).strip().rstrip(",.")
        prefix = "Department" if "Department" in m.group(0) else "Division"
        full = f"{prefix} of {dept_name}"
        # Only add if not already covered by a known department
        already_covered = any(full.lower() in known.lower() or known.lower() in full.lower() for known in depts)
        if not already_covered and full not in depts:
            depts.append(full)

    return depts


def build_description(text: str) -> str:
    """Build a concise description from item text."""
    # Take first sentence or first ~300 chars
    # Remove vote counts from description
    desc = re.sub(r"^\s*\d-\d\s*", "", text)
    desc = desc.strip()
    if len(desc) > 300:
        # Try to cut at a sentence boundary
        cut = desc[:300].rfind(".")
        if cut > 100:
            desc = desc[: cut + 1]
        else:
            desc = desc[:300] + "..."
    return desc


# ---------------------------------------------------------------------------
# Entity cross-referencing
# ---------------------------------------------------------------------------

def load_entity_registry() -> dict:
    """Load entity registry for cross-referencing."""
    if not ENTITY_REGISTRY.exists():
        return {"persons": [], "organizations": []}
    with open(ENTITY_REGISTRY, encoding="utf-8") as f:
        return json.load(f)


def cross_reference_names(
    items: list[dict], full_text: str, registry: dict
) -> dict:
    """Cross-reference detected names against entity registry and build names summary."""
    council_members = []
    external_people = []
    organizations = []

    # Check for council member names in the full text
    for person in registry.get("persons", []):
        canonical = person["canonical_name"]
        # Check last name (council members referenced by last name in agenda)
        last_name = canonical.split()[-1]
        if re.search(r"\b" + re.escape(last_name) + r"\b", full_text, re.IGNORECASE):
            council_members.append({
                "name": canonical,
                "canonical_id": person.get("canonical_id"),
                "role": person.get("role"),
            })

    # Collect unique external people from items
    seen_people = set()
    for item in items:
        for person in item.get("people", []):
            if person not in seen_people:
                seen_people.add(person)
                # Check if this person is in the registry
                in_registry = False
                for reg_person in registry.get("persons", []):
                    if person.lower() in reg_person["canonical_name"].lower():
                        in_registry = True
                        break
                if not in_registry:
                    external_people.append({
                        "name": person,
                        "context": f"Item {item['number']}",
                    })

    # Collect unique organizations from items
    seen_orgs = set()
    for item in items:
        for org in item.get("organizations", []):
            if org not in seen_orgs:
                seen_orgs.add(org)
                # Check if in registry
                canonical_id = None
                for reg_org in registry.get("organizations", []):
                    if org.lower() in reg_org["canonical_name"].lower() or \
                       reg_org["canonical_name"].lower() in org.lower():
                        canonical_id = reg_org.get("canonical_id")
                        break
                organizations.append({
                    "name": org,
                    "canonical_id": canonical_id,
                    "context": f"Item {item['number']}",
                })

    return {
        "council_members": council_members,
        "external_people": external_people,
        "organizations": organizations,
    }


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def save_outputs(
    meeting_info: dict,
    items: list[dict],
    names_detected: dict,
    date_str: str,
):
    """Save agenda.json and names.json to outputs/agendas/."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Build agenda JSON (without full_text to keep it readable)
    agenda_items = []
    for item in items:
        agenda_items.append({
            "number": item["number"],
            "sponsors": item["sponsors"],
            "description": item["description"],
            "vote": item["vote"],
            "organizations": item["organizations"],
            "people": item["people"],
            "departments": item["departments"],
            "amounts": item["amounts"],
        })

    agenda = {
        **meeting_info,
        "item_count": len(agenda_items),
        "items": agenda_items,
        "names_detected": {
            "council_members": [m["name"] for m in names_detected["council_members"]],
            "external_people": [p["name"] for p in names_detected["external_people"]],
            "organizations": [o["name"] for o in names_detected["organizations"]],
        },
    }

    agenda_path = OUTPUT_DIR / f"{date_str}_agenda.json"
    with open(agenda_path, "w", encoding="utf-8") as f:
        json.dump(agenda, f, indent=2, ensure_ascii=False)
    print(f"  Saved: {agenda_path.relative_to(PROJECT_ROOT)}")

    # Build names JSON (for transcript matching)
    names = {
        "date": date_str,
        "source": "agenda_parse",
        "names": [
            {"name": p["name"], "context": p.get("context", "")}
            for p in names_detected["external_people"]
        ],
        "organizations": [
            {"name": o["name"], "context": o.get("context", "")}
            for o in names_detected["organizations"]
            if not o.get("canonical_id")  # only new orgs not already in registry
        ],
    }

    names_path = OUTPUT_DIR / f"{date_str}_names.json"
    with open(names_path, "w", encoding="utf-8") as f:
        json.dump(names, f, indent=2, ensure_ascii=False)
    print(f"  Saved: {names_path.relative_to(PROJECT_ROOT)}")

    return agenda_path, names_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/parse_agenda.py <pdf_url_or_path>")
        print('  Example: python scripts/parse_agenda.py "https://www.syr.gov/.../02.23.2026-agenda.pdf"')
        print("  Parses the short agenda PDF (not the full agenda book).")
        sys.exit(1)

    check_dependencies()

    arg = sys.argv[1]
    is_url = arg.startswith("http://") or arg.startswith("https://")
    tmp_path = None

    try:
        if is_url:
            pdf_path = download_pdf(arg)
            tmp_path = pdf_path
        else:
            pdf_path = Path(arg)
            if not pdf_path.exists():
                print(f"Error: File not found: {pdf_path}")
                sys.exit(1)
            if pdf_path.stat().st_size > MAX_FILE_SIZE:
                print(f"Error: File is {pdf_path.stat().st_size/1024/1024:.1f} MB — too large.")
                print("  This might be the full agenda book. Use the short agenda PDF instead.")
                sys.exit(1)

        # OCR the PDF
        print("Running OCR...")
        full_text = ocr_pdf(pdf_path)

        # Parse meeting header
        print("Parsing meeting metadata...")
        meeting_info = parse_meeting_header(full_text)
        date_str = meeting_info.get("date", "unknown")
        print(f"  Type: {meeting_info.get('meeting_type', '?')}")
        print(f"  Date: {date_str}")
        print(f"  Time: {meeting_info.get('time', '?')}")
        print(f"  Roll call: {meeting_info.get('roll_call', '?')} present")

        # Parse agenda items
        print("Parsing agenda items...")
        items = parse_sponsor_sections(full_text)
        print(f"  Items found: {len(items)}")

        # Load entity registry and cross-reference
        print("Cross-referencing with entity registry...")
        registry = load_entity_registry()
        names_detected = cross_reference_names(items, full_text, registry)

        # Save outputs
        print("Saving outputs...")
        agenda_path, names_path = save_outputs(meeting_info, items, names_detected, date_str)

        # Console summary
        print()
        print("--- Summary ---")
        print(f"  Meeting:  {meeting_info.get('meeting_type', '?')} — {date_str}")
        print(f"  Items:    {len(items)}")
        print(f"  Votes:    {sum(1 for i in items if i['vote'])} recorded")

        cm = names_detected["council_members"]
        if cm:
            print(f"\n  Council members detected ({len(cm)}):")
            for m in cm:
                print(f"    - {m['name']} ({m.get('role', '?')})")

        ep = names_detected["external_people"]
        if ep:
            print(f"\n  External people ({len(ep)}):")
            for p in ep:
                print(f"    - {p['name']} ({p.get('context', '')})")

        orgs = names_detected["organizations"]
        if orgs:
            reg_orgs = [o for o in orgs if o.get("canonical_id")]
            new_orgs = [o for o in orgs if not o.get("canonical_id")]
            if reg_orgs:
                print(f"\n  Known organizations ({len(reg_orgs)}):")
                for o in reg_orgs:
                    print(f"    - {o['name']} [{o['canonical_id']}]")
            if new_orgs:
                print(f"\n  New organizations ({len(new_orgs)}) — consider adding to entity registry:")
                for o in new_orgs:
                    print(f"    - {o['name']} ({o.get('context', '')})")

        depts = set()
        for item in items:
            depts.update(item.get("departments", []))
        if depts:
            print(f"\n  Departments referenced ({len(depts)}):")
            for d in sorted(depts):
                print(f"    - {d}")

        print()

    finally:
        # Clean up temp file if we downloaded
        if tmp_path and tmp_path.exists():
            tmp_path.unlink()


if __name__ == "__main__":
    main()
