"""
rename_transcripts.py — Rename transcript files from opaque video IDs to
human-readable names: {date}_{meeting-type}_{video_id}_corrected.txt

Also creates outputs/transcripts/index.json mapping video IDs to titles.

Usage:
    python scripts/rename_transcripts.py           # dry run (show renames)
    python scripts/rename_transcripts.py --apply    # actually rename
"""

import json
import re
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
TRANSCRIPTS_DIR = PROJECT_ROOT / "outputs" / "transcripts"
DRAFTS_DIR = PROJECT_ROOT / "outputs" / "drafts"


def short_type(video_title: str, meeting_type: str) -> str:
    """Generate a short human-readable label from the meeting title/type."""
    if meeting_type == "study_session":
        return "Study-Session"
    if meeting_type == "regular_meeting":
        return "Regular-Meeting"
    # Committee meetings — extract committee name
    t = video_title.lower()
    if "public works" in t:
        return "Public-Works-Committee"
    if "finance" in t and "taxation" in t:
        return "Finance-Tax-Committee"
    if "finance" in t:
        return "Finance-Committee"
    if "public safety" in t and "economic" in t:
        return "Joint-Safety-EconDev-Committee"
    if "public transportation" in t:
        return "Public-Transportation-Committee"
    if "economic development" in t:
        return "Econ-Dev-Committee"
    return "Committee-Meeting"


def main():
    apply = "--apply" in sys.argv

    # Gather metadata from draft files
    index = {}
    renames = []

    for draft_path in sorted(DRAFTS_DIR.glob("*_drafts.json")):
        with open(draft_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        vid = data["video_id"]
        title = data["video_title"]
        pub_date = data["publication_date"]
        mtype = data["meeting_type"]

        label = short_type(title, mtype)
        prefix = f"{pub_date}_{label}_{vid}"

        index[vid] = {
            "video_title": title,
            "publication_date": pub_date,
            "meeting_type": mtype,
            "filename_prefix": prefix,
        }

        # Find existing transcript files for this video
        for suffix in ["_corrected.txt", "_raw.json"]:
            old_name = f"{vid}{suffix}"
            new_name = f"{prefix}{suffix}"
            old_path = TRANSCRIPTS_DIR / old_name
            new_path = TRANSCRIPTS_DIR / new_name

            if old_path.exists():
                renames.append((old_path, new_path))
            elif new_path.exists():
                pass  # already renamed
            else:
                # Check if already renamed with different prefix
                matches = list(TRANSCRIPTS_DIR.glob(f"*_{vid}{suffix}"))
                if matches:
                    pass  # already renamed
                else:
                    print(f"  Warning: not found: {old_name}")

    # Show plan
    print(f"Found {len(index)} videos, {len(renames)} files to rename.\n")
    for old_path, new_path in renames:
        print(f"  {old_path.name}")
        print(f"    -> {new_path.name}\n")

    if not apply:
        print("Dry run. Use --apply to rename.")
    else:
        for old_path, new_path in renames:
            old_path.rename(new_path)
        print(f"Renamed {len(renames)} files.")

    # Write index
    index_path = TRANSCRIPTS_DIR / "index.json"
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2, ensure_ascii=False)
    print(f"\nWrote index: {index_path.name} ({len(index)} entries)")


if __name__ == "__main__":
    main()
