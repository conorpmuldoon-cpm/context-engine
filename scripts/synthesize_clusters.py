"""
synthesize_clusters.py — Batch-update cluster_ids across all context records.

Applies:
1. Same-URL dedup links
2. New thematic clusters (12 new + 7 expansions)
3. Cross-record dedup links for same-event coverage

Usage:
    python scripts/synthesize_clusters.py             # apply all updates
    python scripts/synthesize_clusters.py --dry-run   # preview changes
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STORE = PROJECT_ROOT / "context-store"


def load_record(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_record(path, record):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Define all cluster assignments
# ---------------------------------------------------------------------------

# SAME-URL DEDUP GROUPS — link records from the same source URL
SAME_URL_DEDUP = {
    "DEDUP-URL-WEB-SOUTH-AVE-BRIDGE": [
        "CTX-WEB-2026-00001", "CTX-WEB-2026-00017",
        "CTX-WEB-2026-00026", "CTX-WEB-2026-00028",
    ],
    "DEDUP-URL-WEB-VEO-RETURN": [
        "CTX-WEB-2026-00005", "CTX-WEB-2026-00020", "CTX-WEB-2026-00029",
    ],
    "DEDUP-URL-WEB-SENATE-BUDGET": [
        "CTX-WEB-2026-00003", "CTX-WEB-2026-00018", "CTX-WEB-2026-00027",
    ],
    "DEDUP-URL-WEB-KIRKPATRICK-BRIDGE": [
        "CTX-WEB-2026-00008", "CTX-WEB-2026-00023", "CTX-WEB-2026-00030",
    ],
    "DEDUP-URL-WEB-BRAVEST-FINEST": [
        "CTX-WEB-2026-00004", "CTX-WEB-2026-00019",
    ],
    "DEDUP-URL-WEB-ST-PATRICKS": [
        "CTX-WEB-2026-00006", "CTX-WEB-2026-00021",
    ],
    "DEDUP-URL-WEB-COMMUNITY-SOLAR": [
        "CTX-WEB-2026-00009", "CTX-WEB-2026-00024",
    ],
    "DEDUP-URL-WEB-LEAD-STANDARD": [
        "CTX-WEB-2026-00010", "CTX-WEB-2026-00025",
    ],
    "DEDUP-URL-WEB-FINANCE-SEARCH": [
        "CTX-WEB-2026-00007", "CTX-WEB-2026-00022",
    ],
    "DEDUP-URL-NEWS-COUNCIL-BALLOT": [
        "CTX-NEWS-2025-00177", "CTX-NEWS-2025-00179",
    ],
    "DEDUP-URL-NEWS-WAGE-ORDINANCE": [
        "CTX-NEWS-2025-00207", "CTX-NEWS-2025-00257",
    ],
    "DEDUP-URL-NEWS-STATE-AID": [
        "CTX-NEWS-2025-00176", "CTX-NEWS-2025-00178",
    ],
    "DEDUP-URL-NEWS-PLAN-A-VISIT": [
        "CTX-NEWS-2022-00001", "CTX-NEWS-2022-00002",
    ],
}

# SAME-EVENT DEDUP — different articles covering the same event
SAME_EVENT_DEDUP = {
    "DEDUP-EVENT-SANCTUARY-CITY": [
        "CTX-NEWS-2025-00190", "CTX-NEWS-2025-00271",
    ],
    "DEDUP-EVENT-SURGE-LINK": [
        "CTX-NEWS-2025-00078", "CTX-NEWS-2025-00079",
    ],
}

# EXISTING CLUSTER EXPANSIONS
CLUSTER_EXPANSIONS = {
    "CLUSTER-PAYROLL-MODERNIZATION-2026Q1": [
        "CTX-NEWS-2025-00038", "CTX-NEWS-2025-00057",
        "CTX-NEWS-2025-00061", "CTX-NEWS-2025-00129",
    ],
    "CLUSTER-VEO-MICROMOBILITY-2026Q1": [
        "CTX-WEB-2026-00005",
    ],
    "CLUSTER-LEAD-REMEDIATION-2026Q1": [
        "CTX-WEB-2026-00010", "CTX-WEB-2026-00025", "CTX-WEB-2026-00033",
    ],
    "CLUSTER-AXON-LPR-2026Q1": [
        "CTX-COUNCIL-2026-00039", "CTX-COUNCIL-2026-00040",
    ],
    "CLUSTER-FIRST-RESPONDER-WELLNESS-2026Q1": [
        "CTX-COUNCIL-2026-00016", "CTX-COUNCIL-2026-00017",
        "CTX-COUNCIL-2026-00018", "CTX-COUNCIL-2026-00144",
    ],
    "CLUSTER-EV-CHARGING-PILOT-2026Q1": [
        "CTX-COUNCIL-2026-00036", "CTX-COUNCIL-2026-00129",
        "CTX-WEB-2026-00009",
    ],
    "CLUSTER-OCRRA-BATTERY-RECYCLING-2026Q1": [
        "CTX-COUNCIL-2026-00014", "CTX-COUNCIL-2026-00143",
        "CTX-COUNCIL-2026-00146",
    ],
}

# NEW CLUSTERS
NEW_CLUSTERS = {
    "CLUSTER-BUDGET-BATTLE-2025Q2": [
        "CTX-NEWS-2025-00001", "CTX-NEWS-2025-00002", "CTX-NEWS-2025-00007",
        "CTX-NEWS-2025-00014", "CTX-NEWS-2025-00020", "CTX-NEWS-2025-00021",
        "CTX-NEWS-2025-00024", "CTX-NEWS-2025-00025", "CTX-NEWS-2025-00026",
        "CTX-NEWS-2025-00027", "CTX-NEWS-2025-00029", "CTX-NEWS-2025-00032",
        "CTX-NEWS-2025-00034", "CTX-NEWS-2025-00035", "CTX-NEWS-2025-00039",
        "CTX-NEWS-2025-00040", "CTX-NEWS-2025-00041", "CTX-NEWS-2025-00042",
        "CTX-NEWS-2025-00043", "CTX-NEWS-2025-00044", "CTX-NEWS-2025-00045",
        "CTX-NEWS-2025-00046", "CTX-NEWS-2025-00186", "CTX-NEWS-2025-00224",
        "CTX-NEWS-2025-00269", "CTX-NEWS-2025-00270", "CTX-NEWS-2025-00292",
    ],
    "CLUSTER-SCHOOL-ZONE-CAMERAS-2025Q4": [
        "CTX-NEWS-2025-00097", "CTX-NEWS-2025-00114", "CTX-NEWS-2025-00131",
        "CTX-NEWS-2025-00147", "CTX-NEWS-2025-00211", "CTX-NEWS-2025-00228",
        "CTX-NEWS-2025-00267", "CTX-NEWS-2025-00287",
        "CTX-WEB-2026-00038", "CTX-WEB-2026-00039",
    ],
    "CLUSTER-PBA-ARMORY-SQUARE-2025Q3": [
        "CTX-NEWS-2025-00073", "CTX-NEWS-2025-00074", "CTX-NEWS-2025-00075",
        "CTX-NEWS-2025-00087", "CTX-NEWS-2025-00089", "CTX-NEWS-2025-00109",
        "CTX-NEWS-2025-00196", "CTX-NEWS-2025-00248",
    ],
    "CLUSTER-CRIME-TRENDS-2025Q3": [
        "CTX-NEWS-2025-00076", "CTX-NEWS-2025-00111", "CTX-NEWS-2025-00140",
        "CTX-NEWS-2025-00156", "CTX-NEWS-2025-00167", "CTX-NEWS-2025-00172",
        "CTX-NEWS-2025-00238", "CTX-NEWS-2025-00252",
    ],
    "CLUSTER-DPW-WINTER-OPS-2025Q4": [
        "CTX-NEWS-2025-00158", "CTX-NEWS-2025-00163", "CTX-NEWS-2025-00168",
        "CTX-NEWS-2025-00173",
        "CTX-WEB-2026-00035", "CTX-WEB-2026-00036",
    ],
    "CLUSTER-LEAD-WATER-2025Q3": [
        "CTX-NEWS-2025-00096", "CTX-NEWS-2025-00102",
        "CTX-NEWS-2025-00063", "CTX-NEWS-2025-00233",
        "CTX-WEB-2026-00040", "CTX-WEB-2026-00042",
    ],
    "CLUSTER-FISCAL-AUDIT-2026Q1": [
        "CTX-COUNCIL-2026-00100", "CTX-COUNCIL-2026-00147",
        "CTX-COUNCIL-2026-00148", "CTX-COUNCIL-2026-00149",
        "CTX-COUNCIL-2026-00150", "CTX-COUNCIL-2026-00151",
        "CTX-WEB-2026-00003", "CTX-WEB-2026-00018",
    ],
    "CLUSTER-SPD-1153-MOVE-2026Q1": [
        "CTX-COUNCIL-2026-00065", "CTX-COUNCIL-2026-00078",
        "CTX-COUNCIL-2026-00091", "CTX-COUNCIL-2026-00092",
        "CTX-COUNCIL-2026-00095", "CTX-COUNCIL-2026-00130",
        "CTX-COUNCIL-2026-00145",
    ],
    "CLUSTER-PEDESTRIAN-SAFETY-2026Q1": [
        "CTX-COUNCIL-2026-00048", "CTX-COUNCIL-2026-00049",
        "CTX-COUNCIL-2026-00050", "CTX-COUNCIL-2026-00051",
        "CTX-COUNCIL-2026-00063", "CTX-COUNCIL-2026-00082",
        "CTX-WEB-2026-00032",
    ],
    "CLUSTER-MAYORAL-TRANSITION-2025Q4": [
        "CTX-NEWS-2025-00160", "CTX-NEWS-2025-00165", "CTX-NEWS-2025-00175",
        "CTX-NEWS-2025-00206", "CTX-NEWS-2025-00215", "CTX-NEWS-2025-00247",
        "CTX-NEWS-2025-00280",
    ],
    "CLUSTER-CRB-OVERSIGHT-2025": [
        "CTX-NEWS-2025-00159", "CTX-NEWS-2025-00182",
        "CTX-NEWS-2025-00272",
        "CTX-COUNCIL-2026-00011",
    ],
    "CLUSTER-SANCTUARY-CITY-2025Q2": [
        "CTX-NEWS-2025-00085", "CTX-NEWS-2025-00190",
        "CTX-NEWS-2025-00233", "CTX-NEWS-2025-00271",
        "CTX-NEWS-2025-00285", "CTX-NEWS-2025-00286",
    ],
}


def main():
    dry_run = "--dry-run" in sys.argv

    # Build a map of record_id -> list of cluster_ids to add
    updates = {}  # record_id -> set of new cluster_ids

    # Process all assignment groups
    all_groups = {}
    all_groups.update(SAME_URL_DEDUP)
    all_groups.update(SAME_EVENT_DEDUP)
    all_groups.update(CLUSTER_EXPANSIONS)
    all_groups.update(NEW_CLUSTERS)

    for cluster_id, record_ids in all_groups.items():
        for rid in record_ids:
            if rid not in updates:
                updates[rid] = set()
            updates[rid].add(cluster_id)

    print(f"Records to update: {len(updates)}")
    print(f"Cluster/dedup groups: {len(all_groups)}")

    # Load and update records
    modified = 0
    missing = 0
    already = 0

    for record_id, new_clusters in sorted(updates.items()):
        # Find the record file
        fname = f"{record_id}.json"
        fpath = STORE / fname
        if not fpath.exists():
            print(f"  MISSING: {record_id}")
            missing += 1
            continue

        record = load_record(fpath)
        existing = set(record.get("cluster_ids", []))
        to_add = new_clusters - existing

        if not to_add:
            already += 1
            continue

        record["cluster_ids"] = sorted(existing | new_clusters)

        if dry_run:
            print(f"  [DRY RUN] {record_id}: +{len(to_add)} clusters -> {sorted(to_add)}")
        else:
            save_record(fpath, record)

        modified += 1

    print(f"\nSummary:")
    print(f"  Modified: {modified}")
    print(f"  Already linked: {already}")
    print(f"  Missing files: {missing}")

    if dry_run:
        print("\n(Dry run — no files changed)")


if __name__ == "__main__":
    main()
