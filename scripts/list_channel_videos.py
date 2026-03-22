"""
list_channel_videos.py — List videos from the City of Syracuse YouTube channel
and cross-reference against already-processed transcripts.

Council meetings appear under the "Streams" tab on YouTube (they're live-streamed).
Regular uploads appear under the "Videos" tab. By default this script checks
the Streams tab since that's where meeting content lives.

Usage:
    python scripts/list_channel_videos.py                    # recent 50 streams (default)
    python scripts/list_channel_videos.py --limit 20         # recent 20 streams
    python scripts/list_channel_videos.py --all              # all streams
    python scripts/list_channel_videos.py --tab videos       # check Videos tab instead

Outputs:
    Console table showing each video with [DONE] or [NEW] status
    outputs/channel_inventory.json — full inventory for reference
"""

import json
import subprocess
import sys
from datetime import date
from pathlib import Path


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
TRANSCRIPTS_DIR = PROJECT_ROOT / "outputs" / "transcripts"
INVENTORY_PATH = PROJECT_ROOT / "outputs" / "channel_inventory.json"

CHANNEL_ID = "UCMSBFyCUupuEqT3ouBfm7ag"
CHANNEL_BASE = f"https://www.youtube.com/channel/{CHANNEL_ID}"

DEFAULT_LIMIT = 50
DEFAULT_TAB = "streams"  # meetings are live-streamed, so they're under /streams


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_processed_video_ids() -> set[str]:
    """Scan outputs/transcripts/ for *_raw.json files and extract video IDs."""
    processed = set()
    if TRANSCRIPTS_DIR.exists():
        for f in TRANSCRIPTS_DIR.glob("*_raw.json"):
            video_id = f.stem.replace("_raw", "")
            processed.add(video_id)
    return processed


def format_duration(seconds: int) -> str:
    """Format seconds as H:MM:SS or M:SS."""
    if seconds <= 0:
        return "?:??"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def fetch_channel_videos(tab: str, limit: int | None) -> list[dict]:
    """Use yt-dlp --flat-playlist to fetch video metadata from the channel.
    Returns videos in channel order (newest first on YouTube).
    Note: upload dates are not available in flat-playlist mode."""

    url = f"{CHANNEL_BASE}/{tab}"
    cmd = [
        "yt-dlp",
        "--flat-playlist",
        "--print", "%(id)s|%(title)s|%(duration)s",
        url,
    ]
    if limit:
        cmd.extend(["--playlist-items", f"1:{limit}"])

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except FileNotFoundError:
        print("Error: yt-dlp not found.")
        print("Install with: python -m pip install yt-dlp")
        sys.exit(1)
    except subprocess.TimeoutExpired:
        print("Error: yt-dlp timed out after 120 seconds.")
        print("The channel may have too many videos or there may be a network issue.")
        sys.exit(1)

    if result.returncode != 0:
        # yt-dlp may still produce output even with non-zero exit code
        if not result.stdout.strip():
            stderr = result.stderr.strip()
            print(f"Error: yt-dlp failed (exit code {result.returncode})")
            if stderr:
                print(f"  {stderr[:500]}")
            sys.exit(1)

    videos = []
    for line in result.stdout.strip().splitlines():
        parts = line.split("|", 2)
        if len(parts) < 3:
            continue

        video_id, title, duration_str = parts

        try:
            duration = int(float(duration_str))
        except (ValueError, TypeError):
            duration = 0

        videos.append({
            "video_id": video_id.strip(),
            "title": title.strip(),
            "duration_seconds": duration,
        })

    return videos


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # Parse arguments
    args = sys.argv[1:]
    limit = DEFAULT_LIMIT
    show_all = False
    tab = DEFAULT_TAB

    if "--all" in args:
        limit = None
        show_all = True
        args.remove("--all")
    if "--limit" in args:
        idx = args.index("--limit")
        if idx + 1 < len(args):
            try:
                limit = int(args[idx + 1])
            except ValueError:
                print("Error: --limit requires a number")
                sys.exit(1)
        else:
            print("Error: --limit requires a number")
            sys.exit(1)
    if "--tab" in args:
        idx = args.index("--tab")
        if idx + 1 < len(args):
            tab = args[idx + 1]
        else:
            print("Error: --tab requires a value (streams or videos)")
            sys.exit(1)

    # Fetch videos
    label = "all" if show_all else f"latest {limit}"
    print(f"Fetching {label} from City of Syracuse YouTube channel ({tab} tab)...")
    videos = fetch_channel_videos(tab, limit)

    if not videos:
        print("No videos found. The channel URL may have changed.")
        print(f"  Tried: {CHANNEL_BASE}/{tab}")
        sys.exit(1)

    # Cross-reference with processed transcripts
    processed = get_processed_video_ids()
    for v in videos:
        v["processed"] = v["video_id"] in processed

    # Print table
    print()
    print("City of Syracuse YouTube Channel — Video Inventory")
    print(f"Channel ID: {CHANNEL_ID}")
    print(f"Tab: {tab}")
    if not show_all:
        print(f"Showing: latest {limit} (use --all for full list)")
    print()

    # Number column for easy reference
    for i, v in enumerate(videos, 1):
        status = "[DONE]" if v["processed"] else "[NEW]"
        dur = format_duration(v["duration_seconds"])
        title = v["title"]
        if len(title) > 65:
            title = title[:62] + "..."
        print(f"  {i:>3}.  {v['video_id']}  {dur:>8}  {title:<65}  {status}")

    # Summary
    n_total = len(videos)
    n_done = sum(1 for v in videos if v["processed"])
    n_new = n_total - n_done
    print()
    print(f"Summary: {n_total} videos | {n_done} processed | {n_new} remaining")

    # Remind how to process
    if n_new > 0:
        example_new = next(v for v in videos if not v["processed"])
        print(f"\nTo process a video:")
        print(f"  python scripts/pull_transcript.py {example_new['video_id']}")

    # Save inventory JSON
    INVENTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    inventory = {
        "channel_id": CHANNEL_ID,
        "fetched_date": date.today().isoformat(),
        "total_videos": n_total,
        "processed": n_done,
        "remaining": n_new,
        "videos": videos,
    }
    with open(INVENTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(inventory, f, indent=2, ensure_ascii=False)
    print(f"\nSaved inventory: {INVENTORY_PATH.relative_to(PROJECT_ROOT)}")
    print()


if __name__ == "__main__":
    main()
