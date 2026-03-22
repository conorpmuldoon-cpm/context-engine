"""
collector_utils.py — Shared utilities for Context Engine collector agents

Provides config loading, mechanical extraction, relevance filtering,
Claude Haiku enrichment, record finalization, state management, and logging.

Used by: scan_news.py, scan_website.py
"""

import json
import logging
import os
import re
import smtplib
from collections import Counter
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

try:
    import anthropic
except ImportError:
    anthropic = None

try:
    import jsonschema
except ImportError:
    jsonschema = None

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
CONTEXT_STORE = PROJECT_ROOT / "context-store"
SCHEMA_PATH = PROJECT_ROOT / "schemas" / "context-record-schema.json"
TAXONOMY_PATH = PROJECT_ROOT / "config" / "taxonomy.json"
ENTITY_REGISTRY_PATH = PROJECT_ROOT / "config" / "entity-registry.json"
API_CONFIG_PATH = PROJECT_ROOT / "config" / "api-config.json"
EMAIL_CONFIG_PATH = PROJECT_ROOT / "config" / "email-config.json"
NEWS_ARTICLES_DIR = PROJECT_ROOT / "outputs" / "news-articles"
WEB_CONTENT_DIR = PROJECT_ROOT / "outputs" / "web-content"
COLLECTOR_LOGS_DIR = PROJECT_ROOT / "outputs" / "collector-logs"


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_taxonomy() -> dict:
    with open(TAXONOMY_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_entity_registry() -> dict:
    with open(ENTITY_REGISTRY_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_schema() -> dict:
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_api_config() -> dict:
    # Check environment variables first (for GitHub Actions / CI)
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        return {
            "anthropic_api_key": api_key,
            "model": os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001"),
        }
    # Fall back to config file (local development)
    if not API_CONFIG_PATH.exists():
        raise FileNotFoundError("config/api-config.json not found (set ANTHROPIC_API_KEY env var for CI)")
    with open(API_CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)
    if "YOUR_" in config.get("anthropic_api_key", "YOUR_"):
        raise ValueError("API key not set in config/api-config.json")
    return config


def load_email_config() -> dict | None:
    # Check environment variables first (for GitHub Actions / CI)
    sender = os.environ.get("GMAIL_SENDER")
    password = os.environ.get("GMAIL_APP_PASSWORD")
    recipient = os.environ.get("GMAIL_RECIPIENT")
    if sender and password and recipient:
        return {
            "enabled": True,
            "smtp_server": "smtp.gmail.com",
            "smtp_port": 587,
            "sender_email": sender,
            "sender_password": password,
            "recipient_email": recipient,
        }
    # Fall back to config file (local development)
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


# ---------------------------------------------------------------------------
# Tag utilities
# ---------------------------------------------------------------------------

def get_valid_tags(taxonomy: dict) -> set[str]:
    """Collect all valid tags from taxonomy (domain + cross-cutting)."""
    tags = set()
    for domain in taxonomy.get("domain_tags", {}).values():
        tags.update(domain.get("tags", []))
    for cross in taxonomy.get("cross_cutting_tags", {}).values():
        tags.update(cross.get("tags", []))
    return tags


def get_freshness_class(source_type: str, taxonomy: dict) -> str:
    mapping = taxonomy.get("source_type_to_freshness", {})
    return mapping.get(source_type, "medium")


def get_flat_tag_list(taxonomy: dict) -> str:
    """Return all valid tags as a flat comma-separated string for prompts."""
    tags = sorted(get_valid_tags(taxonomy))
    return ", ".join(tags)


# ---------------------------------------------------------------------------
# Mechanical extraction (from draft_records.py patterns)
# ---------------------------------------------------------------------------

def extract_entities(text: str, registry: dict) -> list[dict]:
    """Detect known persons and organizations from the entity registry."""
    entities = []
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
    depts = []
    t = text.lower()

    for entry in taxonomy.get("synonym_resolution", {}).get("departments", []):
        canonical = entry["canonical_ref"]
        if canonical.lower() in t:
            if canonical not in depts:
                depts.append(canonical)
            continue
        for variant in entry.get("variants", []):
            if len(variant) <= 5:
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
    systems = []
    t = text.lower()

    for entry in taxonomy.get("synonym_resolution", {}).get("systems", []):
        cn = entry["canonical_name"]
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


# ---------------------------------------------------------------------------
# Relevance filter
# ---------------------------------------------------------------------------

# Government keywords that indicate city government relevance
_GOV_KEYWORDS = [
    "city hall", "common council", "city council", "mayor",
    "ordinance", "resolution", "city budget", "municipal",
    "taxpayer", "city of syracuse", "syr.gov", "syracuse mayor",
    "city administrator", "city auditor", "city clerk",
    "public hearing", "zoning board", "planning commission",
    "department of public works", "dpw", "syracuse police",
    "syracuse fire", "code enforcement", "city officials",
    "city government", "housing court", "building inspector",
    "code violation", "property maintenance", "city contract",
    "city employee", "council president", "council member",
    "city comptroller", "assessment review",
]

# SU sports terms that indicate non-government content
_SU_SPORTS = [
    "orange basketball", "orange football", "orange lacrosse",
    "syracuse basketball", "syracuse football", "syracuse lacrosse",
    "dome", "carrier dome", "jma wireless dome",
    "ncaa", "acc tournament", "march madness",
]


def is_relevant(title: str, text: str, registry: dict, taxonomy: dict) -> tuple[bool, list[str]]:
    """Determine if an article is relevant to city government.

    Returns (is_relevant, reasons).

    Requires strong signals to avoid false positives from common words
    like "fire", "law", "finance" matching department variants.
    A single department match alone is NOT enough — needs entity,
    keyword, or system confirmation.
    """
    combined = (title + " " + text).lower()
    reasons = []

    has_entity = False
    has_keyword = False
    has_system = False
    has_dept = False

    # Check entities (strongest signal)
    entities = extract_entities(title + " " + text, registry)
    if entities:
        names = [e["name"] for e in entities[:3]]
        reasons.append(f"Entities: {', '.join(names)}")
        has_entity = True

    # Check departments
    depts = extract_departments(title + " " + text, taxonomy)
    if depts:
        reasons.append(f"Departments: {', '.join(depts[:3])}")
        has_dept = True

    # Check systems
    systems = extract_systems(title + " " + text, taxonomy)
    if systems:
        reasons.append(f"Systems: {', '.join(s['name'] for s in systems[:3])}")
        has_system = True

    # Check government keywords (word boundary matching to avoid substrings)
    matched_keywords = []
    for kw in _GOV_KEYWORDS:
        pattern = r"\b" + re.escape(kw) + r"\b"
        if re.search(pattern, combined):
            matched_keywords.append(kw)
    if matched_keywords:
        reasons.append(f"Keywords: {', '.join(matched_keywords[:3])}")
        has_keyword = True

    if not reasons:
        return False, []

    # Negative filter: SU sports without government angle
    if not has_entity and not has_keyword:
        for term in _SU_SPORTS:
            if term in combined:
                return False, [f"Filtered: SU sports content ('{term}')"]

    # Require strong evidence: entity, keyword, or system match alone is enough.
    # A department match alone is NOT enough (too many false positives from
    # common words like "fire", "law", "finance" in general news).
    # Department match needs at least one other signal type to confirm.
    if has_entity or has_keyword or has_system:
        return True, reasons

    if has_dept and len(depts) >= 2:
        # Two different departments mentioned = likely city government
        return True, reasons

    # Single department match only — not enough for general news
    return False, [f"Weak signal only (dept: {', '.join(depts)}) — filtered"]


# ---------------------------------------------------------------------------
# Claude Haiku enrichment
# ---------------------------------------------------------------------------

def _build_enrichment_prompt(
    title: str,
    publication_date: str,
    author: str | None,
    source_url: str,
    content: str,
    source_type: str,
    taxonomy: dict,
    mechanical_entities: list[dict],
    mechanical_departments: list[str],
    mechanical_amounts: list[str],
) -> str:
    """Build the prompt for Claude enrichment."""
    tag_list = get_flat_tag_list(taxonomy)

    # Truncate content for cost control
    if len(content) > 4000:
        content = content[:4000] + "\n[...truncated]"

    mech_entities_str = ", ".join(e["name"] for e in mechanical_entities) or "none detected"
    mech_depts_str = ", ".join(mechanical_departments) or "none detected"
    mech_amounts_str = ", ".join(mechanical_amounts) or "none detected"

    source_label = source_type.replace("_", " ")

    return f"""You are the Context Engine Librarian for the City of Syracuse Chief Innovation Officer.
Analyze this {source_label} and return structured metadata.

ARTICLE:
Title: {title}
Date: {publication_date}
Author: {author or 'Unknown'}
URL: {source_url}
Content:
{content}

MECHANICAL EXTRACTIONS (already detected — validate and augment):
Entities: {mech_entities_str}
Departments: {mech_depts_str}
Dollar amounts: {mech_amounts_str}

AVAILABLE TAGS (use ONLY these, or prefix new ones with PROVISIONAL:):
{tag_list}

Return ONLY a JSON object (no markdown fences, no explanation) with these exact keys:
{{
  "summary": "2-4 sentence summary focused on what the CIO needs to know (20-2000 chars)",
  "topic_tags": ["tag1", "tag2"],
  "department_refs": ["Full Canonical Department Name"],
  "entity_refs": [{{"name": "Person or Org Name", "type": "person|organization|system", "canonical_id": "PERSON-FIRSTNAME-LASTNAME|ORG-SHORT-NAME|SYS-SHORT-NAME|null"}}],
  "sentiment": "positive|neutral|critical|mixed|advocacy|procedural",
  "political_signal": {{"signal_type": "championship|opposition|scrutiny|constituent_pressure|priority_alignment|budget_commitment", "description": "...", "confidence": "high|medium|low"}} or null,
  "processing_notes": ["any notes about data quality or connections"]
}}"""


def _infer_tags_from_departments(departments: list[str], taxonomy: dict) -> list[str]:
    """Fallback: infer minimal topic tags from department names."""
    dept_tag_map = {
        "Department of Public Works": ["roads-bridges"],
        "Syracuse Police Department": ["police"],
        "Syracuse Fire Department": ["fire-ems"],
        "Department of Parks, Recreation and Youth Programs": ["parks-recreation"],
        "Department of Water": ["water-sewer"],
        "Department of Neighborhood and Business Development": ["economic-development"],
        "Department of Finance": ["budget"],
        "Department of Engineering": ["infrastructure"],
        "Department of Code Enforcement": ["code-enforcement"],
        "Department of Information Technology": ["technology-modernization"],
        "Department of Human Resources": ["hiring"],
        "Office of the Mayor": ["mayor"],
    }
    tags = set()
    for dept in departments:
        for dept_key, dept_tags in dept_tag_map.items():
            if dept == dept_key:
                tags.update(dept_tags)
    # Always add at least one cross-cutting tag
    if not tags:
        tags.add("announcement")
    return sorted(tags)


def enrich_with_claude(
    title: str,
    publication_date: str,
    author: str | None,
    source_url: str,
    content: str,
    source_type: str,
    taxonomy: dict,
    registry: dict,
    api_config: dict,
    mechanical_entities: list[dict],
    mechanical_departments: list[str],
    mechanical_amounts: list[str],
    logger: logging.Logger | None = None,
) -> dict:
    """Send article to Claude Haiku for enrichment. Returns structured dict.

    On API failure, returns minimal dict with mechanical extractions only.
    """
    if anthropic is None:
        if logger:
            logger.warning("anthropic not installed — using mechanical extraction only")
        return _mechanical_fallback(
            title, mechanical_entities, mechanical_departments, taxonomy,
            "anthropic library not installed"
        )

    prompt = _build_enrichment_prompt(
        title, publication_date, author, source_url, content, source_type,
        taxonomy, mechanical_entities, mechanical_departments, mechanical_amounts,
    )

    try:
        client = anthropic.Anthropic(api_key=api_config["anthropic_api_key"])
        model = api_config.get("model", "claude-haiku-4-5-20251001")

        response = client.messages.create(
            model=model,
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}],
        )

        raw_text = response.content[0].text.strip()

        # Handle markdown-wrapped JSON
        if raw_text.startswith("```"):
            lines = raw_text.split("\n")
            # Remove first and last lines (``` markers)
            lines = [l for l in lines if not l.strip().startswith("```")]
            raw_text = "\n".join(lines)

        result = json.loads(raw_text)

        # Validate expected keys
        required_keys = ["summary", "topic_tags", "department_refs", "entity_refs", "sentiment"]
        for key in required_keys:
            if key not in result:
                result[key] = [] if key.endswith("s") or key.endswith("refs") else ""

        if "processing_notes" not in result:
            result["processing_notes"] = []
        if "political_signal" not in result:
            result["political_signal"] = None

        result["processing_notes"].append(
            f"Enriched by Claude ({api_config.get('model', 'haiku')})"
        )

        return result

    except json.JSONDecodeError as e:
        msg = f"Failed to parse Claude response as JSON: {e}"
        if logger:
            logger.warning(msg)
        return _mechanical_fallback(title, mechanical_entities, mechanical_departments, taxonomy, msg)

    except Exception as e:
        msg = f"Claude API error: {e}"
        if logger:
            logger.warning(msg)
        return _mechanical_fallback(title, mechanical_entities, mechanical_departments, taxonomy, msg)


def _mechanical_fallback(
    title: str,
    mechanical_entities: list[dict],
    mechanical_departments: list[str],
    taxonomy: dict,
    error_msg: str,
) -> dict:
    """Produce a minimal enrichment dict from mechanical extraction only."""
    return {
        "summary": f"[Auto-generated] {title[:1980]}",
        "topic_tags": _infer_tags_from_departments(mechanical_departments, taxonomy),
        "department_refs": mechanical_departments,
        "entity_refs": mechanical_entities,
        "sentiment": "neutral",
        "political_signal": None,
        "processing_notes": [f"Mechanical extraction only — {error_msg}"],
    }


# ---------------------------------------------------------------------------
# Record finalization
# ---------------------------------------------------------------------------

def get_next_sequence(source_code: str, year: int) -> int:
    """Scan context-store for highest CTX-{source_code}-{year}-NNNNN, return next."""
    CONTEXT_STORE.mkdir(parents=True, exist_ok=True)
    max_seq = 0
    pattern = f"CTX-{source_code}-{year}-*.json"
    for path in CONTEXT_STORE.glob(pattern):
        parts = path.stem.split("-")
        # CTX-NEWS-2026-00001 → parts = ['CTX', 'NEWS', '2026', '00001']
        if len(parts) == 4:
            try:
                seq = int(parts[3])
                if seq > max_seq:
                    max_seq = seq
            except ValueError:
                pass
    return max_seq + 1


def load_existing_records() -> list[dict]:
    """Load all existing context records for dedup checking."""
    records = []
    CONTEXT_STORE.mkdir(parents=True, exist_ok=True)
    for path in sorted(CONTEXT_STORE.glob("CTX-*.json")):
        try:
            with open(path, encoding="utf-8") as f:
                records.append(json.load(f))
        except (json.JSONDecodeError, OSError):
            pass
    return records


def check_duplicate(record: dict, existing: list[dict]) -> str | None:
    """Check if record is a potential duplicate. Returns matching record_id or None.

    Criteria (all must match):
    1. Same publication_date (±2 days)
    2. At least 1 overlapping department_refs
    3. At least 2 overlapping topic_tags
    4. At least 1 overlapping entity_refs (by name)
    """
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

        if abs((new_date - ex_date).days) > 2:
            continue

        ex_depts = set(existing_rec.get("department_refs", []))
        if not new_depts & ex_depts:
            continue

        ex_tags = set(existing_rec.get("topic_tags", []))
        if len(new_tags & ex_tags) < 2:
            continue

        ex_entities = {e["name"] for e in existing_rec.get("entity_refs", []) if isinstance(e, dict)}
        if not new_entities & ex_entities:
            continue

        return existing_rec.get("record_id", "UNKNOWN")

    return None


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
    if jsonschema is None:
        return ["jsonschema not installed — skipping validation"]
    errors = []
    try:
        jsonschema.validate(instance=record, schema=schema)
    except jsonschema.ValidationError as e:
        errors.append(f"Schema validation: {e.message}")
    return errors


def build_context_record(
    record_id: str,
    source_agent: str,
    source_type: str,
    source_url: str,
    publication_date: str,
    title: str,
    enrichment: dict,
    freshness_class: str,
) -> dict:
    """Assemble a complete context record from enrichment results."""
    capture_dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "record_id": record_id,
        "source_agent": source_agent,
        "source_type": source_type,
        "source_url": source_url,
        "publication_date": publication_date,
        "capture_date": capture_dt,
        "title": title,
        "summary": enrichment.get("summary", ""),
        "raw_content": None,
        "transcript_ref": None,
        "topic_tags": enrichment.get("topic_tags", []),
        "department_refs": enrichment.get("department_refs", []),
        "entity_refs": enrichment.get("entity_refs", []),
        "speakers": None,
        "speaker_confidence": None,
        "sentiment": enrichment.get("sentiment", "neutral"),
        "political_signal": enrichment.get("political_signal"),
        "freshness_class": freshness_class,
        "cluster_ids": [],
        "engagement_relevance": [],
        "feedback": None,
        "schema_version": "1.0.0",
        "librarian_version": "1.0.0-auto",
        "processing_notes": enrichment.get("processing_notes", []),
        "last_relevance_update": None,
    }


def save_record(record: dict) -> Path:
    """Save record to context-store/{record_id}.json."""
    CONTEXT_STORE.mkdir(parents=True, exist_ok=True)
    path = CONTEXT_STORE / f"{record['record_id']}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)
    return path


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def load_state(state_path: Path) -> dict:
    if state_path.exists():
        with open(state_path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(state_path: Path, state: dict):
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logger(collector_name: str) -> logging.Logger:
    """Set up file + console logging."""
    COLLECTOR_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    log_path = COLLECTOR_LOGS_DIR / f"{collector_name}-{today}.log"

    logger = logging.getLogger(collector_name)
    logger.setLevel(logging.DEBUG)

    # Avoid duplicate handlers on repeated calls
    if logger.handlers:
        return logger

    # File handler (DEBUG)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    # Console handler (INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


# ---------------------------------------------------------------------------
# Email notification
# ---------------------------------------------------------------------------

def send_collector_email(config: dict, collector_name: str, subject: str, body: str) -> bool:
    """Send collector summary email."""
    msg = MIMEMultipart()
    msg["From"] = config["sender_email"]
    msg["To"] = config["recipient_email"]
    msg["Subject"] = f"[Context Engine Collector] {subject}"
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
# Raw content archiving
# ---------------------------------------------------------------------------

def archive_raw_content(content: str, filename: str, archive_dir: Path) -> Path:
    """Save raw article/page text to archive directory."""
    archive_dir.mkdir(parents=True, exist_ok=True)
    # Sanitize filename
    safe = re.sub(r'[<>:"/\\|?*]', '_', filename)[:200]
    path = archive_dir / safe
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path
