"""
audit_records.py — Data quality audit for the Context Engine context-store.

Scans all context records and reports issues organized by severity.
Mechanical issues can be auto-fixed with --fix.

Usage:
    python scripts/audit_records.py              # report only
    python scripts/audit_records.py --fix        # auto-fix mechanical issues
    python scripts/audit_records.py --verbose    # show per-record details
"""

import json
import re
import sys
from collections import defaultdict, namedtuple
from datetime import datetime
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
# Finding type
# ---------------------------------------------------------------------------
Finding = namedtuple("Finding", [
    "check_id",     # int: which check produced this
    "check_name",   # str: human-readable check name
    "record_id",    # str: affected record (or None for cross-record findings)
    "severity",     # "error" | "warning" | "info"
    "message",      # str: human-readable description
    "fixable",      # bool: can --fix handle this?
    "fix_data",     # dict | None: info needed to apply the fix
])

# ---------------------------------------------------------------------------
# Config loading
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


def load_all_records() -> list[tuple[Path, dict]]:
    """Load all context records. Returns list of (path, record) tuples."""
    records = []
    CONTEXT_STORE.mkdir(parents=True, exist_ok=True)
    for path in sorted(CONTEXT_STORE.glob("CTX-*.json")):
        try:
            with open(path, encoding="utf-8") as f:
                records.append((path, json.load(f)))
        except (json.JSONDecodeError, OSError) as e:
            print(f"  Warning: Could not read {path.name}: {e}")
    return records


def build_name_lookup(registry: dict) -> dict:
    """Build name→(canonical_name, canonical_id, type) lookup from registry.

    Matches canonical names and all transcript variants.
    """
    lookup = {}
    for person in registry.get("persons", []):
        cname = person["canonical_name"]
        cid = person["canonical_id"]
        lookup[cname.lower()] = (cname, cid, "person")
        for variant in person.get("transcript_variants", []):
            lookup[variant.lower()] = (cname, cid, "person")

    for org in registry.get("organizations", []):
        cname = org["canonical_name"]
        cid = org["canonical_id"]
        lookup[cname.lower()] = (cname, cid, "organization")
        for variant in org.get("transcript_variants", []):
            lookup[variant.lower()] = (cname, cid, "organization")

    return lookup


def build_dept_canonical_map(taxonomy: dict) -> dict:
    """Build variant→canonical department name map from taxonomy synonyms."""
    dept_map = {}
    for entry in taxonomy.get("synonym_resolution", {}).get("departments", []):
        canonical = entry["canonical_ref"]
        for variant in entry.get("variants", []):
            dept_map[variant.lower()] = canonical
    return dept_map


# ---------------------------------------------------------------------------
# Check 1: Missing canonical_ids on entity_refs
# ---------------------------------------------------------------------------

def check_missing_canonical_ids(records, name_lookup):
    findings = []
    for path, rec in records:
        rid = rec.get("record_id", path.stem)
        for i, eref in enumerate(rec.get("entity_refs", [])):
            if not isinstance(eref, dict):
                continue
            name = eref.get("name", "")
            existing_cid = eref.get("canonical_id")
            if existing_cid:
                continue  # Already has one

            key = name.lower()
            if key in name_lookup:
                canon_name, canon_id, etype = name_lookup[key]
                findings.append(Finding(
                    check_id=1,
                    check_name="Missing canonical_id",
                    record_id=rid,
                    severity="warning",
                    message=f"entity_refs[{i}] '{name}' matches registry → {canon_id}",
                    fixable=True,
                    fix_data={
                        "path": path,
                        "entity_index": i,
                        "canonical_name": canon_name,
                        "canonical_id": canon_id,
                    },
                ))
    return findings


# ---------------------------------------------------------------------------
# Check 2: Stale provisional notes in processing_notes
# ---------------------------------------------------------------------------

def check_stale_provisional_notes(records, taxonomy):
    """Find processing_notes that reference PROVISIONAL: tags that have been promoted."""
    promoted = {h["tag"] for h in taxonomy.get("promotion_history", [])}
    findings = []
    for path, rec in records:
        rid = rec.get("record_id", path.stem)
        for i, note in enumerate(rec.get("processing_notes", [])):
            if "PROVISIONAL:" in note:
                # Check if the tag mentioned has been promoted
                for tag in promoted:
                    if f"PROVISIONAL:{tag}" in note:
                        findings.append(Finding(
                            check_id=2,
                            check_name="Stale provisional note",
                            record_id=rid,
                            severity="info",
                            message=f"processing_notes[{i}] references promoted tag '{tag}'",
                            fixable=True,
                            fix_data={
                                "path": path,
                                "note_index": i,
                                "promoted_tag": tag,
                            },
                        ))
                        break
    return findings


# ---------------------------------------------------------------------------
# Check 3: Non-canonical department names
# ---------------------------------------------------------------------------

def check_department_names(records, dept_map):
    findings = []
    for path, rec in records:
        rid = rec.get("record_id", path.stem)
        for i, dept in enumerate(rec.get("department_refs", [])):
            key = dept.lower()
            if key in dept_map and dept != dept_map[key]:
                findings.append(Finding(
                    check_id=3,
                    check_name="Non-canonical department name",
                    record_id=rid,
                    severity="warning",
                    message=f"department_refs[{i}] '{dept}' → '{dept_map[key]}'",
                    fixable=True,
                    fix_data={
                        "path": path,
                        "dept_index": i,
                        "old_name": dept,
                        "canonical_name": dept_map[key],
                    },
                ))
    return findings


# ---------------------------------------------------------------------------
# Check 5: Potential missing clusters
# ---------------------------------------------------------------------------

def check_potential_clusters(records):
    """Find groups of 3+ records sharing date + dept + 2+ substantive tags, no common cluster.

    Excludes ubiquitous tags (council, vote, department-head, residents, etc.) from
    overlap counting since they match too broadly across same-meeting segments.
    Only flags groups where ALL records lack ANY cluster_id (truly unclustered).
    """
    findings = []

    # Tags that appear on most council records and don't indicate topical similarity
    UBIQUITOUS_TAGS = {"council", "vote", "department-head", "residents", "announcement"}

    # Group records by publication_date
    by_date = defaultdict(list)
    for path, rec in records:
        date = rec.get("publication_date", "")
        by_date[date].append((path, rec))

    for date, recs in by_date.items():
        if len(recs) < 3:
            continue

        # For each pair, check if they share dept + 2 substantive tags
        n = len(recs)
        adjacency = defaultdict(set)

        for i in range(n):
            ri = recs[i][1]
            depts_i = set(ri.get("department_refs", []))
            tags_i = set(ri.get("topic_tags", [])) - UBIQUITOUS_TAGS
            clusters_i = set(ri.get("cluster_ids", []))

            # Skip records already in a cluster
            if clusters_i:
                continue

            for j in range(i + 1, n):
                rj = recs[j][1]
                depts_j = set(rj.get("department_refs", []))
                tags_j = set(rj.get("topic_tags", [])) - UBIQUITOUS_TAGS
                clusters_j = set(rj.get("cluster_ids", []))

                # Skip records already in a cluster
                if clusters_j:
                    continue

                # Share dept + 2 substantive tags?
                if depts_i & depts_j and len(tags_i & tags_j) >= 2:
                    adjacency[i].add(j)
                    adjacency[j].add(i)

        # Find connected components of size 3+
        visited = set()
        for start in range(n):
            if start in visited or start not in adjacency:
                continue
            # BFS
            component = set()
            queue = [start]
            while queue:
                node = queue.pop(0)
                if node in component:
                    continue
                component.add(node)
                for neighbor in adjacency.get(node, set()):
                    if neighbor not in component:
                        queue.append(neighbor)
            visited |= component

            if len(component) >= 3:
                comp_recs = [recs[i] for i in sorted(component)]
                record_ids = [r[1].get("record_id", r[0].stem) for r in comp_recs]

                # Find common dept and tags
                all_depts = set.intersection(*(set(r[1].get("department_refs", [])) for r in comp_recs))
                all_tags = set.intersection(*(set(r[1].get("topic_tags", [])) for r in comp_recs))

                dept_str = ", ".join(sorted(all_depts)) if all_depts else "(mixed)"
                tag_str = ", ".join(sorted(all_tags)) if all_tags else "(varied)"

                findings.append(Finding(
                    check_id=5,
                    check_name="Potential missing cluster",
                    record_id=None,
                    severity="info",
                    message=(
                        f"{len(component)} records on {date} | {dept_str} | [{tag_str}]\n"
                        f"         Records: {', '.join(record_ids)}"
                    ),
                    fixable=False,
                    fix_data=None,
                ))
    return findings


# ---------------------------------------------------------------------------
# Check 6: Orphaned from existing cluster
# ---------------------------------------------------------------------------

def check_orphaned_from_cluster(records, registry):
    """Find records that share many features with a cluster but aren't in it.

    Excludes council members from entity overlap since they appear across
    almost all records and create false positive cluster matches.
    Uses non-council entity overlap + substantive tag overlap.
    """
    findings = []

    # Build set of council member names to exclude from overlap
    council_names = set()
    for person in registry.get("persons", []):
        if "COUNCIL" in person.get("canonical_id", "") or person.get("role", "").startswith("Council"):
            council_names.add(person["canonical_name"].lower())
            for v in person.get("transcript_variants", []):
                council_names.add(v.lower())

    UBIQUITOUS_TAGS = {"council", "vote", "department-head", "residents", "announcement"}

    # Build cluster membership
    cluster_members = defaultdict(list)
    for path, rec in records:
        rid = rec.get("record_id", path.stem)
        for cid in rec.get("cluster_ids", []):
            cluster_members[cid].append((rid, rec))

    # For each record NOT in any cluster, check if it overlaps heavily with one
    for path, rec in records:
        rid = rec.get("record_id", path.stem)
        my_clusters = set(rec.get("cluster_ids", []))
        if my_clusters:
            continue  # Only check unclustered records

        my_entities = {
            e.get("name", "").lower()
            for e in rec.get("entity_refs", [])
            if isinstance(e, dict) and e.get("name", "").lower() not in council_names
        }
        my_tags = set(rec.get("topic_tags", [])) - UBIQUITOUS_TAGS

        for cid, members in cluster_members.items():
            # Check overlap with cluster members
            for member_rid, member_rec in members:
                member_entities = {
                    e.get("name", "").lower()
                    for e in member_rec.get("entity_refs", [])
                    if isinstance(e, dict) and e.get("name", "").lower() not in council_names
                }
                member_tags = set(member_rec.get("topic_tags", [])) - UBIQUITOUS_TAGS

                entity_overlap = len(my_entities & member_entities)
                tag_overlap = len(my_tags & member_tags)

                # Higher bar: 2+ non-council entities AND 2+ substantive tags
                if entity_overlap >= 2 and tag_overlap >= 2:
                    findings.append(Finding(
                        check_id=6,
                        check_name="Possible cluster orphan",
                        record_id=rid,
                        severity="info",
                        message=f"Shares {entity_overlap} non-council entities + {tag_overlap} substantive tags with {cid} (via {member_rid})",
                        fixable=False,
                        fix_data=None,
                    ))
                    break  # One finding per cluster is enough

    return findings


# ---------------------------------------------------------------------------
# Check 7: Unregistered frequent entities
# ---------------------------------------------------------------------------

def check_unregistered_entities(records, name_lookup):
    """Find persons/orgs appearing in 3+ records without a registry match."""
    entity_counts = defaultdict(lambda: {"count": 0, "type": None, "records": [], "roles": set()})

    for path, rec in records:
        rid = rec.get("record_id", path.stem)
        seen_in_record = set()

        for eref in rec.get("entity_refs", []):
            if not isinstance(eref, dict):
                continue
            name = eref.get("name", "")
            if eref.get("canonical_id") or not name:
                continue  # Already registered
            key = name.lower()
            if key in name_lookup:
                continue  # Matches registry but missing canonical_id (caught by check 1)
            if key not in seen_in_record:
                seen_in_record.add(key)
                entity_counts[key]["count"] += 1
                entity_counts[key]["type"] = eref.get("type", "unknown")
                entity_counts[key]["records"].append(rid)

        # Also check speakers
        for speaker in (rec.get("speakers") or []):
            if not isinstance(speaker, dict):
                continue
            name = speaker.get("name", "")
            key = name.lower()
            if key in name_lookup or not name:
                continue
            role = speaker.get("role", "")
            if role:
                entity_counts[key]["roles"].add(role)

    # Filter to 3+ occurrences
    findings = []
    for key, info in sorted(entity_counts.items(), key=lambda x: -x[1]["count"]):
        if info["count"] >= 3:
            roles_str = "; ".join(sorted(info["roles"])) if info["roles"] else ""
            name_display = info["records"][0] if info["records"] else key  # Use first record to show
            # Get the actual name (not lowered) from first record
            actual_name = key.title()
            for path, rec in records:
                for eref in rec.get("entity_refs", []):
                    if isinstance(eref, dict) and eref.get("name", "").lower() == key:
                        actual_name = eref["name"]
                        break
                else:
                    continue
                break

            findings.append(Finding(
                check_id=7,
                check_name="Unregistered frequent entity",
                record_id=None,
                severity="info",
                message=(
                    f"{actual_name} ({info['type']}, {info['count']} records)"
                    + (f" — {roles_str}" if roles_str else "")
                ),
                fixable=False,
                fix_data=None,
            ))
    return findings


# ---------------------------------------------------------------------------
# Check 8: Schema validation
# ---------------------------------------------------------------------------

def check_schema_validation(records, schema):
    findings = []
    for path, rec in records:
        rid = rec.get("record_id", path.stem)
        try:
            jsonschema.validate(instance=rec, schema=schema)
        except jsonschema.ValidationError as e:
            findings.append(Finding(
                check_id=8,
                check_name="Schema validation failure",
                record_id=rid,
                severity="error",
                message=str(e.message)[:200],
                fixable=False,
                fix_data=None,
            ))
    return findings


# ---------------------------------------------------------------------------
# Check 9: Invalid tags
# ---------------------------------------------------------------------------

def check_invalid_tags(records, valid_tags):
    findings = []
    for path, rec in records:
        rid = rec.get("record_id", path.stem)
        for tag in rec.get("topic_tags", []):
            if tag.startswith("PROVISIONAL:"):
                continue
            if tag not in valid_tags:
                findings.append(Finding(
                    check_id=9,
                    check_name="Invalid tag",
                    record_id=rid,
                    severity="error",
                    message=f"Tag '{tag}' not in taxonomy",
                    fixable=False,
                    fix_data=None,
                ))
    return findings


# ---------------------------------------------------------------------------
# Check 10: Empty required fields
# ---------------------------------------------------------------------------

def check_empty_fields(records):
    findings = []
    for path, rec in records:
        rid = rec.get("record_id", path.stem)
        title = rec.get("title", "")
        summary = rec.get("summary", "")
        tags = rec.get("topic_tags", [])

        if not title or not title.strip():
            findings.append(Finding(
                check_id=10, check_name="Empty field", record_id=rid,
                severity="error", message="Empty title", fixable=False, fix_data=None,
            ))
        if not summary or not summary.strip():
            findings.append(Finding(
                check_id=10, check_name="Empty field", record_id=rid,
                severity="error", message="Empty summary", fixable=False, fix_data=None,
            ))
        if not tags:
            findings.append(Finding(
                check_id=10, check_name="Empty field", record_id=rid,
                severity="error", message="No topic_tags", fixable=False, fix_data=None,
            ))
    return findings


# ---------------------------------------------------------------------------
# Check 11: Cluster ID variants
# ---------------------------------------------------------------------------

def check_cluster_id_variants(records):
    """Find cluster IDs that look like duplicates (differing by small edit)."""
    findings = []
    all_clusters = set()
    for path, rec in records:
        all_clusters.update(rec.get("cluster_ids", []))

    cluster_list = sorted(all_clusters)
    reported = set()

    for i, c1 in enumerate(cluster_list):
        for c2 in cluster_list[i + 1:]:
            # Normalize: strip year/quarter suffix, compare stems
            stem1 = re.sub(r"-\d{4}Q\d$", "", c1)
            stem2 = re.sub(r"-\d{4}Q\d$", "", c2)

            # Check if one is a substring of the other (common variant pattern)
            if stem1 in stem2 or stem2 in stem1:
                pair = tuple(sorted([c1, c2]))
                if pair not in reported:
                    reported.add(pair)
                    findings.append(Finding(
                        check_id=11,
                        check_name="Possible cluster ID variant",
                        record_id=None,
                        severity="warning",
                        message=f"'{c1}' and '{c2}' may be the same cluster",
                        fixable=False,
                        fix_data=None,
                    ))
    return findings


# ---------------------------------------------------------------------------
# Check 12: Unlinked duplicates
# ---------------------------------------------------------------------------

def check_unlinked_duplicates(records):
    """Find record pairs matching dedup criteria but not linked via cluster_ids.

    Only flags cross-meeting duplicates (different source_url or different dates).
    Same-meeting segments naturally share date/dept/tags/entities and are expected
    to match dedup criteria — those are not flagged.
    """
    findings = []
    reported = set()

    for i, (path_i, rec_i) in enumerate(records):
        rid_i = rec_i.get("record_id", path_i.stem)
        try:
            date_i = datetime.strptime(rec_i.get("publication_date", ""), "%Y-%m-%d").date()
        except ValueError:
            continue

        # Extract video_id from source_url to identify same-meeting segments
        url_i = rec_i.get("source_url", "") or ""
        vid_i = ""
        if "youtube.com" in url_i and "v=" in url_i:
            vid_i = url_i.split("v=")[1].split("&")[0]

        depts_i = set(rec_i.get("department_refs", []))
        tags_i = set(rec_i.get("topic_tags", []))
        entities_i = {e.get("name", "").lower() for e in rec_i.get("entity_refs", []) if isinstance(e, dict)}
        clusters_i = set(rec_i.get("cluster_ids", []))

        for j, (path_j, rec_j) in enumerate(records[i + 1:], start=i + 1):
            rid_j = rec_j.get("record_id", path_j.stem)

            # Skip if already share a cluster
            clusters_j = set(rec_j.get("cluster_ids", []))
            if clusters_i & clusters_j:
                continue

            try:
                date_j = datetime.strptime(rec_j.get("publication_date", ""), "%Y-%m-%d").date()
            except ValueError:
                continue

            # Skip same-meeting segments (same date + same video)
            url_j = rec_j.get("source_url", "") or ""
            vid_j = ""
            if "youtube.com" in url_j and "v=" in url_j:
                vid_j = url_j.split("v=")[1].split("&")[0]

            if date_i == date_j and vid_i and vid_j and vid_i == vid_j:
                continue  # Same meeting, different segments — expected overlap

            if abs((date_i - date_j).days) > 2:
                continue

            depts_j = set(rec_j.get("department_refs", []))
            if not depts_i & depts_j:
                continue

            tags_j = set(rec_j.get("topic_tags", []))
            if len(tags_i & tags_j) < 2:
                continue

            entities_j = {e.get("name", "").lower() for e in rec_j.get("entity_refs", []) if isinstance(e, dict)}
            if not entities_i & entities_j:
                continue

            pair = tuple(sorted([rid_i, rid_j]))
            if pair not in reported:
                reported.add(pair)
                findings.append(Finding(
                    check_id=12,
                    check_name="Unlinked duplicate",
                    record_id=rid_i,
                    severity="warning",
                    message=f"Matches dedup criteria with {rid_j} but no shared cluster",
                    fixable=False,
                    fix_data=None,
                ))
    return findings


# ---------------------------------------------------------------------------
# Fix application
# ---------------------------------------------------------------------------

def apply_fixes(findings, records_by_path):
    """Apply auto-fixes for fixable findings. Returns count of fixes applied."""
    modified_paths = set()
    fix_count = 0

    today = datetime.now().strftime("%Y-%m-%d")

    # Group check-2 findings by path so we can remove notes in reverse index order
    # (avoids index shift problems when removing multiple notes from same record)
    check2_by_path = defaultdict(list)
    other_findings = []
    for f in findings:
        if not f.fixable or not f.fix_data:
            continue
        if f.check_id == 2:
            check2_by_path[f.fix_data["path"]].append(f)
        else:
            other_findings.append(f)

    # Apply check-2 fixes grouped by path, removing notes in reverse index order
    for path, path_findings in check2_by_path.items():
        rec = records_by_path.get(path)
        if not rec:
            continue
        notes = rec.get("processing_notes", [])
        # Sort by note_index descending so removals don't shift earlier indices
        path_findings.sort(key=lambda f: f.fix_data["note_index"], reverse=True)
        promoted_tags = []
        for f in path_findings:
            idx = f.fix_data["note_index"]
            if idx < len(notes):
                notes.pop(idx)
                promoted_tags.append(f.fix_data["promoted_tag"])
                fix_count += 1
        if promoted_tags:
            notes.append(
                f"{today}: Cleaned stale provisional tag notes "
                f"({', '.join(reversed(promoted_tags))} promoted to taxonomy)."
            )
            modified_paths.add(path)

    # Apply other fixes
    for f in other_findings:
        path = f.fix_data.get("path")
        if not path:
            continue

        rec = records_by_path.get(path)
        if not rec:
            continue

        if f.check_id == 1:
            # Fix missing canonical_id
            idx = f.fix_data["entity_index"]
            erefs = rec.get("entity_refs", [])
            if idx < len(erefs):
                erefs[idx]["canonical_id"] = f.fix_data["canonical_id"]
                # Also fix name to canonical if different
                old_name = erefs[idx].get("name", "")
                new_name = f.fix_data["canonical_name"]
                if old_name != new_name:
                    erefs[idx]["name"] = new_name
                modified_paths.add(path)
                fix_count += 1

        elif f.check_id == 3:
            # Canonicalize department name
            depts = rec.get("department_refs", [])
            idx = f.fix_data["dept_index"]
            if idx < len(depts):
                depts[idx] = f.fix_data["canonical_name"]
                modified_paths.add(path)
                fix_count += 1

    return fix_count, modified_paths


def save_modified_records(records_by_path, modified_paths):
    """Write back modified records to disk."""
    for path in modified_paths:
        rec = records_by_path[path]
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rec, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_report(all_findings, verbose):
    """Print organized audit report."""
    # Group by check_id
    by_check = defaultdict(list)
    for f in all_findings:
        by_check[f.check_id].append(f)

    fixable_checks = [1, 2, 3, 4]
    report_checks = [5, 6, 7, 8, 9, 10, 11, 12]

    total_fixable = sum(1 for f in all_findings if f.fixable)
    total_recommendations = sum(1 for f in all_findings if not f.fixable)
    total_errors = sum(1 for f in all_findings if f.severity == "error")

    # Auto-fixable section
    print("\n--- AUTO-FIXABLE ISSUES (use --fix to apply) ---\n")
    any_fixable = False
    for cid in fixable_checks:
        items = by_check.get(cid, [])
        if not items:
            continue
        any_fixable = True
        check_name = items[0].check_name
        affected = len(set(f.record_id for f in items if f.record_id))
        print(f"  [{cid}] {check_name}: {len(items)} issues across {affected} records")
        if verbose:
            for f in items:
                print(f"      {f.record_id}: {f.message}")
        else:
            # Show first 3
            for f in items[:3]:
                print(f"      {f.record_id}: {f.message}")
            if len(items) > 3:
                print(f"      ... and {len(items) - 3} more")
        print()
    if not any_fixable:
        print("  (none)\n")

    # Recommendations section
    print("--- RECOMMENDATIONS (require review) ---\n")
    any_recommendations = False
    for cid in report_checks:
        items = by_check.get(cid, [])
        if not items:
            continue
        any_recommendations = True
        check_name = items[0].check_name
        if items[0].record_id:
            affected = len(set(f.record_id for f in items if f.record_id))
            print(f"  [{cid}] {check_name}: {len(items)} issues across {affected} records")
        else:
            print(f"  [{cid}] {check_name}: {len(items)} found")
        if verbose:
            for f in items:
                prefix = f.record_id or "     "
                print(f"      {prefix}: {f.message}")
        else:
            for f in items[:5]:
                prefix = f.record_id or "     "
                print(f"      {prefix}: {f.message}")
            if len(items) > 5:
                print(f"      ... and {len(items) - 5} more")
        print()
    if not any_recommendations:
        print("  (none)\n")

    # Summary
    print("--- SUMMARY ---\n")
    print(f"  Auto-fixable:     {total_fixable} issues")
    print(f"  Recommendations:  {total_recommendations} issues")
    print(f"  Errors:           {total_errors}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = sys.argv[1:]
    do_fix = "--fix" in args
    verbose = "--verbose" in args

    # Load configs
    print("Loading configuration...")
    schema = load_schema()
    taxonomy = load_taxonomy()
    registry = load_entity_registry()
    valid_tags = get_valid_tags(taxonomy)
    name_lookup = build_name_lookup(registry)
    dept_map = build_dept_canonical_map(taxonomy)

    # Load records
    print("Loading context records...")
    records = load_all_records()
    records_by_path = {path: rec for path, rec in records}

    today = datetime.now().strftime("%Y-%m-%d")
    print(f"\n{'=' * 55}")
    print(f"  Context Engine Data Quality Audit")
    print(f"  {len(records)} records scanned | {today}")
    print(f"{'=' * 55}")

    # Run all checks
    all_findings = []

    print("\nRunning checks...")

    # Auto-fixable checks
    all_findings.extend(check_missing_canonical_ids(records, name_lookup))
    all_findings.extend(check_stale_provisional_notes(records, taxonomy))
    all_findings.extend(check_department_names(records, dept_map))

    # Report-only checks
    all_findings.extend(check_potential_clusters(records))
    all_findings.extend(check_orphaned_from_cluster(records, registry))
    all_findings.extend(check_unregistered_entities(records, name_lookup))
    all_findings.extend(check_schema_validation(records, schema))
    all_findings.extend(check_invalid_tags(records, valid_tags))
    all_findings.extend(check_empty_fields(records))
    all_findings.extend(check_cluster_id_variants(records))
    all_findings.extend(check_unlinked_duplicates(records))

    # Print report
    print_report(all_findings, verbose)

    # Apply fixes if requested
    if do_fix:
        fixable = [f for f in all_findings if f.fixable]
        if fixable:
            print("\n--- APPLYING FIXES ---\n")
            fix_count, modified = apply_fixes(fixable, records_by_path)
            if modified:
                save_modified_records(records_by_path, modified)
                print(f"  Applied {fix_count} fixes across {len(modified)} records.")
                print(f"  Modified files:")
                for p in sorted(modified):
                    print(f"    {p.name}")
            else:
                print("  No fixes applied.")
        else:
            print("\n  No fixable issues found.")
    elif any(f.fixable for f in all_findings):
        print("\n  Tip: Run with --fix to auto-fix mechanical issues.")

    # Schema validity line
    schema_fails = sum(1 for f in all_findings if f.check_id == 8)
    print(f"\n  Schema valid: {len(records) - schema_fails}/{len(records)}")


if __name__ == "__main__":
    main()
