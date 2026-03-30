"""Batch briefing generator — runs gen_briefing.py for all clusters, departments, and themes."""

import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
GEN_BRIEFING = SCRIPT_DIR / "gen_briefing.py"

# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------

CLUSTER_TARGETS = [
    "CLUSTER-BUDGET-BATTLE-2025Q2",
    "CLUSTER-PAYROLL-MODERNIZATION-2026Q1",
    "CLUSTER-SCHOOL-ZONE-CAMERAS-2025Q4",
    "CLUSTER-PARKSIDE-COMMONS-2026Q1",
    "CLUSTER-LEAD-REMEDIATION-2026Q1",
    "CLUSTER-CRIME-TRENDS-2025Q3",
    "CLUSTER-SHA-GOVERNANCE-CRISIS-2025",
    "CLUSTER-EAST-ADAMS-TRANSFORMATION-2025",
    "CLUSTER-VEO-MICROMOBILITY-2026Q1",
    "CLUSTER-LEAD-WATER-2025Q3",
    "CLUSTER-FISCAL-AUDIT-2026Q1",
    "CLUSTER-SKYLINE-APARTMENTS-2025",
    "CLUSTER-PBA-ARMORY-SQUARE-2025Q3",
    "CLUSTER-DPW-WINTER-OPS-2025Q4",
    "CLUSTER-SANCTUARY-CITY-2025Q2",
    "CLUSTER-SU-COMSTOCK-CONSTRUCTION-2026Q1",
    "CLUSTER-FIRST-RESPONDER-WELLNESS-2026Q1",
    "CLUSTER-PEDESTRIAN-SAFETY-2026Q1",
    "CLUSTER-SUSTAINABILITY-CLIMATE-2025",
    "CLUSTER-SCSD-GOVERNANCE-2025",
    "CLUSTER-MAYORAL-TRANSITION-2025Q4",
    "CLUSTER-AXON-LPR-2026Q1",
    "CLUSTER-DPW-CAPITAL-2026Q1",
    "CLUSTER-SPD-1153-MOVE-2026Q1",
    "CLUSTER-SPD-USE-OF-FORCE-2025",
    "CLUSTER-HOMELESSNESS-SERVICES-2025",
    "CLUSTER-MARIA-REGINA-CONGEL-2025Q2",
    "CLUSTER-FIRE-APPARATUS-2026Q1",
    "CLUSTER-PARKS-CAPITAL-2025",
    "CLUSTER-CODE-ENFORCEMENT-REFORM-2026Q1",
    "CLUSTER-EV-CHARGING-PILOT-2026Q1",
    "CLUSTER-COLUMBUS-STATUE-2025",
    "CLUSTER-OCRRA-BATTERY-RECYCLING-2026Q1",
    "CLUSTER-CRB-OVERSIGHT-2025",
    "CLUSTER-SPD-GRANTS-PROGRAMS-2026Q1",
    "CLUSTER-PARKWAY-BRIDGE-STRIKES-2025",
    "CLUSTER-MICRON-ECONOMIC-DEV-2025",
    "CLUSTER-DOWNTOWN-DEVELOPMENT-2025",
    "CLUSTER-ROAD-RECONSTRUCTION-2026Q1",
    "CLUSTER-HUD-HOUSING-PROGRAMS-2026Q1",
    "CLUSTER-VACANT-BUILDING-FIRES-2025",
    "CLUSTER-DEER-MANAGEMENT-2025",
    "CLUSTER-GOOD-CAUSE-EVICTION-2026Q1",
    "CLUSTER-BESS-MORATORIUM-2026Q1",
    "CLUSTER-SENIOR-TAX-EXEMPTION-2026Q1",
]

DEPARTMENT_TARGETS = [
    "Syracuse Fire Department",
    "Department of Permits and Development",
    "Parks and Recreation",
    "Law Department",
    "Department of Water",
    "Neighborhood and Business Development",
    "Office of the Mayor",
    "Department of Assessment",
]

THEMATIC_TARGETS = [
    "Owens Administration Transition",
    "Infrastructure Overview",
    "Economic Development",
    "Intergovernmental Relations",
]

ALL_TARGETS = (
    [(t, 9999) for t in CLUSTER_TARGETS]       # all-time for clusters
    + [(t, 365) for t in DEPARTMENT_TARGETS]    # 12 months for departments
    + [(t, 365) for t in THEMATIC_TARGETS]      # 12 months for themes
)


def main():
    total = len(ALL_TARGETS)
    succeeded = 0
    failed = []
    skipped = 0

    print(f"=== Batch Briefing Generator ===")
    print(f"Total targets: {total}")
    print(f"  Clusters:    {len(CLUSTER_TARGETS)}")
    print(f"  Departments: {len(DEPARTMENT_TARGETS)}")
    print(f"  Thematic:    {len(THEMATIC_TARGETS)}")
    print()

    start_time = time.time()

    for i, (target, days) in enumerate(ALL_TARGETS, 1):
        elapsed = time.time() - start_time
        print(f"[{i}/{total}] Generating: {target}  (days={days})")

        try:
            result = subprocess.run(
                [sys.executable, str(GEN_BRIEFING),
                 "--target", target, "--days", str(days)],
                capture_output=True, text=True, timeout=120,
                encoding="utf-8", errors="replace",
            )

            if result.returncode == 0:
                succeeded += 1
                # Extract the "Briefing saved:" line
                for line in result.stdout.splitlines():
                    if "Briefing saved:" in line or "No records match" in line:
                        print(f"  -> {line.strip()}")
                        break
                    if "relevant records" in line:
                        print(f"  -> {line.strip()}")
            else:
                failed.append(target)
                print(f"  !! FAILED (exit {result.returncode})")
                if result.stderr:
                    print(f"     {result.stderr[:200]}")

        except subprocess.TimeoutExpired:
            failed.append(target)
            print(f"  !! TIMEOUT (120s)")

        except Exception as e:
            failed.append(target)
            print(f"  !! ERROR: {e}")

    elapsed = time.time() - start_time
    print()
    print(f"=== Batch Complete ===")
    print(f"Succeeded: {succeeded}/{total}")
    print(f"Failed:    {len(failed)}/{total}")
    print(f"Time:      {elapsed/60:.1f} minutes")

    if failed:
        print(f"\nFailed targets:")
        for t in failed:
            print(f"  - {t}")


if __name__ == "__main__":
    main()
