#!/usr/bin/env python3
"""
Duplicate Detection Script for Context Engine
Scans all context records and identifies potential duplicates based on:
1. Same publication_date (+/- 2 days)
2. Same primary department_refs
3. At least 2 overlapping topic_tags
4. At least 1 overlapping entity_refs

Also performs fuzzy title matching for near-duplicate detection.
"""

import json
import os
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from difflib import SequenceMatcher
import itertools

STORE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "context-store")


def load_all_records():
    """Load all JSON records from context-store/"""
    records = []
    for fname in os.listdir(STORE_DIR):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(STORE_DIR, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                rec = json.load(f)
                records.append(rec)
        except Exception as e:
            print(f"WARNING: Could not load {fname}: {e}", file=sys.stderr)
    return records


def parse_date(date_str):
    """Parse a date string, return datetime.date or None"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def get_entity_names(entity_refs):
    """Extract entity names from entity_refs list"""
    if not entity_refs:
        return set()
    names = set()
    for e in entity_refs:
        if isinstance(e, dict) and e.get("name"):
            names.add(e["name"].lower().strip())
        elif isinstance(e, str):
            names.add(e.lower().strip())
    return names


def get_entity_canonical_ids(entity_refs):
    """Extract canonical IDs from entity_refs"""
    if not entity_refs:
        return set()
    ids = set()
    for e in entity_refs:
        if isinstance(e, dict) and e.get("canonical_id"):
            ids.add(e["canonical_id"])
    return ids


def entities_overlap(refs_a, refs_b):
    """Check if two entity_refs lists have overlap (by name or canonical_id)"""
    names_a = get_entity_names(refs_a)
    names_b = get_entity_names(refs_b)

    # Direct name overlap
    name_overlap = names_a & names_b

    # Canonical ID overlap
    ids_a = get_entity_canonical_ids(refs_a)
    ids_b = get_entity_canonical_ids(refs_b)
    id_overlap = ids_a & ids_b if (ids_a and ids_b) else set()

    # Combined
    all_overlapping_names = set(name_overlap)

    # If canonical IDs overlap, find the names for those IDs
    if id_overlap:
        for e in (refs_a or []) + (refs_b or []):
            if isinstance(e, dict) and e.get("canonical_id") in id_overlap:
                all_overlapping_names.add(e.get("name", "").lower().strip())

    return all_overlapping_names


def departments_overlap(deps_a, deps_b):
    """Check if department_refs overlap"""
    if not deps_a or not deps_b:
        return set()
    set_a = {d.lower().strip() for d in deps_a}
    set_b = {d.lower().strip() for d in deps_b}
    return set_a & set_b


def tags_overlap(tags_a, tags_b):
    """Check tag overlap"""
    if not tags_a or not tags_b:
        return set()
    set_a = set(tags_a)
    set_b = set(tags_b)
    return set_a & set_b


def title_similarity(title_a, title_b):
    """Compute fuzzy similarity between two titles"""
    if not title_a or not title_b:
        return 0.0
    return SequenceMatcher(None, title_a.lower(), title_b.lower()).ratio()


def check_cluster_link(rec_a, rec_b):
    """Check if two records already share a cluster_id"""
    clusters_a = set(rec_a.get("cluster_ids") or [])
    clusters_b = set(rec_b.get("cluster_ids") or [])
    if not clusters_a or not clusters_b:
        return False, set()
    shared = clusters_a & clusters_b
    return bool(shared), shared


def main():
    records = load_all_records()
    print(f"Loaded {len(records)} records from context-store/\n")

    # ---- PART 1: Full 4-criteria duplicate detection ----
    full_duplicates = []

    for i, rec_a in enumerate(records):
        for j in range(i + 1, len(records)):
            rec_b = records[j]

            # Criterion 1: publication_date within 2 days
            date_a = parse_date(rec_a.get("publication_date"))
            date_b = parse_date(rec_b.get("publication_date"))
            if not date_a or not date_b:
                continue
            if abs((date_a - date_b).days) > 2:
                continue

            # Criterion 2: department overlap
            dept_overlap = departments_overlap(
                rec_a.get("department_refs"),
                rec_b.get("department_refs")
            )
            if not dept_overlap:
                continue

            # Criterion 3: at least 2 overlapping topic_tags
            tag_overlap = tags_overlap(
                rec_a.get("topic_tags"),
                rec_b.get("topic_tags")
            )
            if len(tag_overlap) < 2:
                continue

            # Criterion 4: at least 1 overlapping entity_refs
            entity_overlap = entities_overlap(
                rec_a.get("entity_refs"),
                rec_b.get("entity_refs")
            )
            if not entity_overlap:
                continue

            # Compute confidence score
            date_diff = abs((date_a - date_b).days)
            title_sim = title_similarity(rec_a.get("title", ""), rec_b.get("title", ""))

            # Confidence: higher = more likely duplicate
            confidence = 0.0
            confidence += (3 - date_diff) * 10  # 0-day = 30, 1-day = 20, 2-day = 10
            confidence += len(tag_overlap) * 8
            confidence += len(dept_overlap) * 10
            confidence += len(entity_overlap) * 8
            confidence += title_sim * 30  # up to 30 points for exact title match

            # Same source URL = definite duplicate
            same_url = (rec_a.get("source_url") and rec_b.get("source_url") and
                       rec_a["source_url"] == rec_b["source_url"])
            if same_url:
                confidence += 50

            already_linked, shared_clusters = check_cluster_link(rec_a, rec_b)

            full_duplicates.append({
                "id_a": rec_a["record_id"],
                "id_b": rec_b["record_id"],
                "title_a": rec_a.get("title", ""),
                "title_b": rec_b.get("title", ""),
                "source_a": rec_a.get("source_agent", ""),
                "source_b": rec_b.get("source_agent", ""),
                "date_a": str(date_a),
                "date_b": str(date_b),
                "dept_overlap": sorted(dept_overlap),
                "tag_overlap": sorted(tag_overlap),
                "entity_overlap": sorted(entity_overlap),
                "title_similarity": round(title_sim, 3),
                "same_url": same_url,
                "already_linked": already_linked,
                "shared_clusters": sorted(shared_clusters),
                "confidence": round(confidence, 1),
                "type": "full_4criteria"
            })

    # Sort by confidence descending
    full_duplicates.sort(key=lambda x: x["confidence"], reverse=True)

    # ---- PART 2: Fuzzy title matching (even if not all 4 criteria met) ----
    # Only check pairs not already in full_duplicates
    full_dup_pairs = {(d["id_a"], d["id_b"]) for d in full_duplicates}

    title_duplicates = []
    for i, rec_a in enumerate(records):
        for j in range(i + 1, len(records)):
            rec_b = records[j]
            pair = (rec_a["record_id"], rec_b["record_id"])
            if pair in full_dup_pairs:
                continue

            title_a = rec_a.get("title", "")
            title_b = rec_b.get("title", "")
            if not title_a or not title_b:
                continue

            sim = title_similarity(title_a, title_b)
            if sim < 0.70:
                continue

            # Check what criteria they do meet
            date_a = parse_date(rec_a.get("publication_date"))
            date_b = parse_date(rec_b.get("publication_date"))
            date_close = False
            if date_a and date_b:
                date_close = abs((date_a - date_b).days) <= 2

            dept_overlap = departments_overlap(rec_a.get("department_refs"), rec_b.get("department_refs"))
            tag_overlap = tags_overlap(rec_a.get("topic_tags"), rec_b.get("topic_tags"))
            entity_overlap = entities_overlap(rec_a.get("entity_refs"), rec_b.get("entity_refs"))

            already_linked, shared_clusters = check_cluster_link(rec_a, rec_b)
            same_url = (rec_a.get("source_url") and rec_b.get("source_url") and
                       rec_a["source_url"] == rec_b["source_url"])

            confidence = sim * 40  # up to 40 for title
            if date_close:
                confidence += 15
            if dept_overlap:
                confidence += 10
            if len(tag_overlap) >= 2:
                confidence += 10
            if entity_overlap:
                confidence += 10
            if same_url:
                confidence += 50

            title_duplicates.append({
                "id_a": rec_a["record_id"],
                "id_b": rec_b["record_id"],
                "title_a": title_a,
                "title_b": title_b,
                "source_a": rec_a.get("source_agent", ""),
                "source_b": rec_b.get("source_agent", ""),
                "date_a": str(date_a) if date_a else "N/A",
                "date_b": str(date_b) if date_b else "N/A",
                "title_similarity": round(sim, 3),
                "date_within_2_days": date_close,
                "dept_overlap": sorted(dept_overlap) if dept_overlap else [],
                "tag_overlap": sorted(tag_overlap) if tag_overlap else [],
                "entity_overlap": sorted(entity_overlap) if entity_overlap else [],
                "same_url": same_url,
                "already_linked": already_linked,
                "shared_clusters": sorted(shared_clusters),
                "confidence": round(confidence, 1),
                "type": "title_fuzzy_only"
            })

    title_duplicates.sort(key=lambda x: x["confidence"], reverse=True)

    # ---- PART 3: Same URL detection (regardless of other criteria) ----
    url_map = defaultdict(list)
    for rec in records:
        url = rec.get("source_url")
        if url:
            url_map[url].append(rec["record_id"])

    same_url_groups = {url: ids for url, ids in url_map.items() if len(ids) > 1}

    # ---- OUTPUT ----
    print("=" * 100)
    print("DUPLICATE DETECTION REPORT")
    print("=" * 100)

    # Same URL groups
    if same_url_groups:
        print(f"\n{'=' * 80}")
        print(f"SAME-URL GROUPS ({len(same_url_groups)} groups)")
        print(f"{'=' * 80}")
        for url, ids in sorted(same_url_groups.items()):
            print(f"\n  URL: {url}")
            print(f"  Records: {', '.join(sorted(ids))}")

    # Full 4-criteria duplicates
    print(f"\n{'=' * 80}")
    print(f"FULL 4-CRITERIA DUPLICATES ({len(full_duplicates)} pairs)")
    print(f"{'=' * 80}")

    for idx, dup in enumerate(full_duplicates, 1):
        linked_status = "YES (ALREADY LINKED)" if dup["already_linked"] else "NO (UNLINKED)"
        url_status = " [SAME URL]" if dup["same_url"] else ""
        print(f"\n--- Pair {idx} | Confidence: {dup['confidence']}{url_status} | Linked: {linked_status} ---")
        print(f"  A: {dup['id_a']} ({dup['source_a']})")
        print(f"     \"{dup['title_a']}\"")
        print(f"     Date: {dup['date_a']}")
        print(f"  B: {dup['id_b']} ({dup['source_b']})")
        print(f"     \"{dup['title_b']}\"")
        print(f"     Date: {dup['date_b']}")
        print(f"  Departments: {', '.join(dup['dept_overlap'])}")
        print(f"  Tags ({len(dup['tag_overlap'])}): {', '.join(dup['tag_overlap'])}")
        print(f"  Entities ({len(dup['entity_overlap'])}): {', '.join(dup['entity_overlap'])}")
        print(f"  Title similarity: {dup['title_similarity']}")
        if dup["already_linked"]:
            print(f"  Shared clusters: {', '.join(dup['shared_clusters'])}")

    # Title-fuzzy duplicates
    if title_duplicates:
        print(f"\n{'=' * 80}")
        print(f"FUZZY TITLE MATCHES (>= 70% similarity, not in 4-criteria list) ({len(title_duplicates)} pairs)")
        print(f"{'=' * 80}")

        for idx, dup in enumerate(title_duplicates[:100], 1):  # Cap at 100
            linked_status = "YES" if dup["already_linked"] else "NO"
            print(f"\n--- Title Match {idx} | Similarity: {dup['title_similarity']} | Confidence: {dup['confidence']} | Linked: {linked_status} ---")
            print(f"  A: {dup['id_a']} ({dup['source_a']})")
            print(f"     \"{dup['title_a']}\"")
            print(f"     Date: {dup['date_a']}")
            print(f"  B: {dup['id_b']} ({dup['source_b']})")
            print(f"     \"{dup['title_b']}\"")
            print(f"     Date: {dup['date_b']}")
            if dup["dept_overlap"]:
                print(f"  Departments: {', '.join(dup['dept_overlap'])}")
            if dup["tag_overlap"]:
                print(f"  Tags: {', '.join(dup['tag_overlap'])}")
            if dup["entity_overlap"]:
                print(f"  Entities: {', '.join(dup['entity_overlap'])}")
            criteria_met = []
            if dup["date_within_2_days"]:
                criteria_met.append("date")
            if dup["dept_overlap"]:
                criteria_met.append("dept")
            if len(dup["tag_overlap"]) >= 2:
                criteria_met.append("tags>=2")
            if dup["entity_overlap"]:
                criteria_met.append("entities")
            missing = {"date", "dept", "tags>=2", "entities"} - set(criteria_met)
            print(f"  Criteria met: {', '.join(criteria_met) or 'none (title only)'}")
            print(f"  Criteria missing: {', '.join(missing)}")

    # Summary stats
    linked_count = sum(1 for d in full_duplicates if d["already_linked"])
    unlinked_count = len(full_duplicates) - linked_count
    cross_source = sum(1 for d in full_duplicates if d["source_a"] != d["source_b"])
    same_source = len(full_duplicates) - cross_source

    print(f"\n{'=' * 80}")
    print(f"SUMMARY")
    print(f"{'=' * 80}")
    print(f"  Total records scanned: {len(records)}")
    print(f"  Same-URL groups: {len(same_url_groups)}")
    print(f"  Full 4-criteria duplicate pairs: {len(full_duplicates)}")
    print(f"    - Already linked via cluster_ids: {linked_count}")
    print(f"    - UNLINKED (action needed): {unlinked_count}")
    print(f"    - Cross-source pairs: {cross_source}")
    print(f"    - Same-source pairs: {same_source}")
    print(f"  Fuzzy title matches (>=70%, not in 4-criteria): {len(title_duplicates)}")

    # Save full results as JSON
    output_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                "outputs", "duplicate-report.json")
    report = {
        "scan_date": datetime.now().isoformat(),
        "total_records": len(records),
        "same_url_groups": {url: ids for url, ids in same_url_groups.items()},
        "full_4criteria_duplicates": full_duplicates,
        "fuzzy_title_matches": title_duplicates[:100],
        "summary": {
            "full_duplicate_pairs": len(full_duplicates),
            "already_linked": linked_count,
            "unlinked": unlinked_count,
            "cross_source": cross_source,
            "same_source": same_source,
            "fuzzy_title_matches": len(title_duplicates)
        }
    }
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\n  Full report saved to: {output_path}")


if __name__ == "__main__":
    main()
