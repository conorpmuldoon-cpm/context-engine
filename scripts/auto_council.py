"""
auto_council.py — Automated council transcript pipeline orchestrator.

Detects new City of Syracuse YouTube meeting videos, pulls transcripts,
generates drafts, enriches via Claude API, and finalizes context records.
Designed to run unattended on GitHub Actions.

Usage:
    python scripts/auto_council.py                  # full run
    python scripts/auto_council.py --dry-run        # detect + report only
    python scripts/auto_council.py --no-email       # skip email summary
    python scripts/auto_council.py --limit 2        # process max N new videos
"""

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
TRANSCRIPTS_DIR = PROJECT_ROOT / "outputs" / "transcripts"
DRAFTS_DIR = PROJECT_ROOT / "outputs" / "drafts"
STATE_PATH = PROJECT_ROOT / "outputs" / "council-pipeline-state.json"

sys.path.insert(0, str(SCRIPT_DIR))
from collector_utils import (
    load_state,
    save_state,
    load_email_config,
    send_collector_email,
    setup_logger,
)

# Channel info (same as list_channel_videos.py)
CHANNEL_ID = "UCMSBFyCUupuEqT3ouBfm7ag"


# ---------------------------------------------------------------------------
# Step 1: Detect new videos
# ---------------------------------------------------------------------------

def get_processed_video_ids() -> set[str]:
    """Get video IDs from state file + existing draft files."""
    processed = set()

    # From state file (primary source)
    state = load_state(STATE_PATH)
    processed.update(state.get("processed_videos", []))

    # From existing draft files (reliable — they have a video_id JSON field)
    if DRAFTS_DIR.exists():
        for f in DRAFTS_DIR.glob("*_drafts.json"):
            try:
                with open(f, encoding="utf-8") as fh:
                    draft = json.load(fh)
                vid = draft.get("video_id")
                if vid:
                    processed.add(vid)
            except (json.JSONDecodeError, OSError):
                pass

    return processed


def fetch_recent_videos(limit: int = 10, logger=None) -> list[dict]:
    """Fetch recent videos from the City of Syracuse YouTube Streams tab."""
    url = f"https://www.youtube.com/channel/{CHANNEL_ID}/streams"
    cmd = [
        "yt-dlp",
        "--flat-playlist",
        "--print", "%(id)s|%(title)s|%(duration)s",
        "--playlist-items", f"1:{limit}",
        url,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except FileNotFoundError:
        if logger:
            logger.error("yt-dlp not found — install with: pip install yt-dlp")
        return []
    except subprocess.TimeoutExpired:
        if logger:
            logger.error("yt-dlp timed out fetching channel videos")
        return []

    if result.returncode != 0 and not result.stdout.strip():
        if logger:
            logger.error(f"yt-dlp failed: {result.stderr[:300]}")
        return []

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


def detect_new_videos(limit: int = 10, logger=None) -> list[dict]:
    """Detect new (unprocessed) videos on the channel."""
    processed = get_processed_video_ids()
    recent = fetch_recent_videos(limit=max(limit * 3, 20), logger=logger)

    new_videos = [v for v in recent if v["video_id"] not in processed]

    # Filter: only meetings (> 5 minutes, title suggests meeting content)
    meeting_keywords = [
        "common council", "regular meeting", "study session",
        "committee", "public hearing", "special session",
    ]
    meetings = []
    for v in new_videos:
        title_lower = v["title"].lower()
        is_meeting = any(kw in title_lower for kw in meeting_keywords)
        is_long_enough = v["duration_seconds"] > 300  # > 5 minutes
        if is_meeting and is_long_enough:
            meetings.append(v)

    return meetings


# ---------------------------------------------------------------------------
# Step 2-5: Pipeline steps (subprocess calls)
# ---------------------------------------------------------------------------

def run_step(cmd: list[str], step_name: str, logger=None) -> bool:
    """Run a pipeline step as a subprocess. Returns True on success."""
    if logger:
        logger.info(f"Running: {step_name}")
        logger.debug(f"  Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 min per step
            cwd=str(PROJECT_ROOT),
        )

        if result.stdout:
            for line in result.stdout.strip().splitlines():
                if logger:
                    logger.debug(f"  {line}")

        if result.returncode != 0:
            if logger:
                logger.error(f"  {step_name} failed (exit {result.returncode})")
                if result.stderr:
                    logger.error(f"  stderr: {result.stderr[:500]}")
            return False

        return True

    except subprocess.TimeoutExpired:
        if logger:
            logger.error(f"  {step_name} timed out after 300s")
        return False
    except Exception as e:
        if logger:
            logger.error(f"  {step_name} error: {e}")
        return False


def process_video(video: dict, dry_run: bool = False, logger=None) -> dict:
    """Run the full pipeline for one video. Returns result dict."""
    video_id = video["video_id"]
    title = video["title"]
    result = {
        "video_id": video_id,
        "title": title,
        "success": False,
        "steps_completed": [],
        "error": None,
    }

    if logger:
        logger.info(f"Processing: {title} ({video_id})")

    if dry_run:
        if logger:
            logger.info(f"  DRY RUN — would process {video_id}")
        result["steps_completed"].append("dry_run")
        return result

    python = sys.executable

    # Step 1: Pull transcript
    ok = run_step(
        [python, str(SCRIPT_DIR / "pull_transcript.py"), video_id],
        f"pull_transcript ({video_id})",
        logger,
    )
    if not ok:
        result["error"] = "pull_transcript failed"
        return result
    result["steps_completed"].append("pull_transcript")

    # Step 2: Draft records
    ok = run_step(
        [python, str(SCRIPT_DIR / "draft_records.py"), video_id],
        f"draft_records ({video_id})",
        logger,
    )
    if not ok:
        result["error"] = "draft_records failed"
        return result
    result["steps_completed"].append("draft_records")

    # Find the draft file
    draft_path = DRAFTS_DIR / f"{video_id}_drafts.json"
    if not draft_path.exists():
        # Check for renamed files
        matches = list(DRAFTS_DIR.glob(f"*{video_id}*_drafts.json"))
        if matches:
            draft_path = matches[0]
        else:
            result["error"] = f"Draft file not found: {draft_path.name}"
            return result

    # Step 3: Enrich drafts
    ok = run_step(
        [python, str(SCRIPT_DIR / "enrich_drafts.py"), str(draft_path)],
        f"enrich_drafts ({video_id})",
        logger,
    )
    if not ok:
        result["error"] = "enrich_drafts failed"
        return result
    result["steps_completed"].append("enrich_drafts")

    # Step 4: Finalize records
    ok = run_step(
        [python, str(SCRIPT_DIR / "finalize_records.py"), str(draft_path)],
        f"finalize_records ({video_id})",
        logger,
    )
    if not ok:
        result["error"] = "finalize_records failed"
        return result
    result["steps_completed"].append("finalize_records")

    result["success"] = True
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = sys.argv[1:]
    dry_run = "--dry-run" in args
    no_email = "--no-email" in args
    limit = 5  # default: process up to 5 new videos per run

    if "--dry-run" in args:
        args.remove("--dry-run")
    if "--no-email" in args:
        args.remove("--no-email")
    if "--limit" in args:
        idx = args.index("--limit")
        if idx + 1 < len(args):
            try:
                limit = int(args[idx + 1])
            except ValueError:
                print("Error: --limit requires a number")
                sys.exit(1)

    logger = setup_logger("auto-council")
    logger.info(f"=== Auto Council Pipeline {'(DRY RUN)' if dry_run else ''} ===")

    # Step 0: Scan for new agendas (best-effort — continues if it fails)
    python = sys.executable
    logger.info("Scanning for new council agendas...")
    agenda_ok = run_step(
        [python, str(SCRIPT_DIR / "scan_agendas.py")],
        "scan_agendas",
        logger,
    )
    if not agenda_ok:
        logger.warning("Agenda scan failed — continuing without new agendas")

    # Detect new videos
    logger.info("Checking for new council meeting videos...")
    new_videos = detect_new_videos(limit=limit, logger=logger)

    if not new_videos:
        logger.info("No new council meeting videos found.")
        return

    logger.info(f"Found {len(new_videos)} new video(s):")
    for v in new_videos:
        dur_min = v["duration_seconds"] // 60
        logger.info(f"  {v['video_id']} — {v['title']} ({dur_min} min)")

    # Apply limit
    if len(new_videos) > limit:
        logger.info(f"Limiting to {limit} video(s)")
        new_videos = new_videos[:limit]

    # Process each video
    results = []
    for video in new_videos:
        result = process_video(video, dry_run=dry_run, logger=logger)
        results.append(result)

        # Update state after each successful video
        if result["success"] and not dry_run:
            state = load_state(STATE_PATH)
            processed = state.get("processed_videos", [])
            if video["video_id"] not in processed:
                processed.append(video["video_id"])
            state["processed_videos"] = processed
            state["last_run"] = datetime.now(timezone.utc).isoformat()
            state["total_processed"] = len(processed)
            save_state(STATE_PATH, state)

    # Summary
    successes = [r for r in results if r["success"]]
    failures = [r for r in results if not r["success"] and r.get("error")]

    logger.info(f"\n{'=' * 50}")
    logger.info(f"PIPELINE SUMMARY {'(DRY RUN)' if dry_run else ''}")
    logger.info(f"  Videos detected: {len(new_videos)}")
    logger.info(f"  Successful: {len(successes)}")
    logger.info(f"  Failed: {len(failures)}")

    for r in failures:
        logger.error(f"  FAILED: {r['video_id']} — {r['error']}")

    # Email summary (unless --no-email or dry run)
    if not no_email and not dry_run and (successes or failures):
        email_config = load_email_config()
        if email_config:
            subject = f"Council Transcriber: {len(successes)} processed, {len(failures)} failed"
            body_lines = [
                f"Auto Council Pipeline — {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                "",
                f"Videos detected: {len(new_videos)}",
                f"Successfully processed: {len(successes)}",
                f"Failed: {len(failures)}",
                "",
            ]
            for r in successes:
                body_lines.append(f"  OK: {r['title']}")
            for r in failures:
                body_lines.append(f"  FAIL: {r['title']} — {r['error']}")

            send_collector_email(email_config, "auto_council", subject, "\n".join(body_lines))


if __name__ == "__main__":
    main()
