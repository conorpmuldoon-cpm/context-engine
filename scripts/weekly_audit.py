"""
weekly_audit.py — Scheduled wrapper for audit_records.py

Runs the data quality audit with --fix, saves a dated report, and
emails a summary to the configured recipient.

Usage:
    python scripts/weekly_audit.py              # run audit + email
    python scripts/weekly_audit.py --no-email   # run audit, skip email
    python scripts/weekly_audit.py --no-fix     # report only, no auto-fix
    python scripts/weekly_audit.py --test-email  # send a test email to verify config

Designed to be called from Windows Task Scheduler on a weekly basis.
"""

import json
import smtplib
import subprocess
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
AUDIT_SCRIPT = SCRIPT_DIR / "audit_records.py"
EMAIL_CONFIG_PATH = PROJECT_ROOT / "config" / "email-config.json"
REPORTS_DIR = PROJECT_ROOT / "outputs" / "audit-reports"


# ---------------------------------------------------------------------------
# Email config
# ---------------------------------------------------------------------------

def load_email_config() -> dict | None:
    """Load email configuration. Returns None if not configured."""
    if not EMAIL_CONFIG_PATH.exists():
        print("No email config found at config/email-config.json")
        return None

    with open(EMAIL_CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)

    if not config.get("enabled"):
        print("Email is disabled in config (enabled: false)")
        return None

    required = ["smtp_server", "smtp_port", "sender_email", "sender_password", "recipient_email"]
    for key in required:
        if not config.get(key) or "YOUR_" in str(config.get(key, "")):
            print(f"Email config incomplete: {key} not set")
            return None

    return config


# ---------------------------------------------------------------------------
# Run audit
# ---------------------------------------------------------------------------

def run_audit(fix: bool = True) -> tuple[str, int]:
    """Run audit_records.py and capture output. Returns (output_text, return_code)."""
    cmd = [sys.executable, str(AUDIT_SCRIPT)]
    if fix:
        cmd.append("--fix")
    cmd.append("--verbose")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
        timeout=300,  # 5 minute timeout
    )

    output = result.stdout
    if result.stderr:
        output += "\n--- STDERR ---\n" + result.stderr

    return output, result.returncode


# ---------------------------------------------------------------------------
# Save report
# ---------------------------------------------------------------------------

def save_report(output: str) -> Path:
    """Save audit output to a dated report file."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    today = datetime.now().strftime("%Y-%m-%d")
    report_path = REPORTS_DIR / f"audit-{today}.txt"

    # If already run today, add a sequence number
    if report_path.exists():
        seq = 2
        while True:
            report_path = REPORTS_DIR / f"audit-{today}-{seq}.txt"
            if not report_path.exists():
                break
            seq += 1

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"Context Engine Data Quality Audit — {today}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(output)

    return report_path


# ---------------------------------------------------------------------------
# Extract summary for email
# ---------------------------------------------------------------------------

def extract_summary(output: str) -> str:
    """Build a rich email summary from audit output."""
    lines = output.split("\n")

    # Extract key numbers
    record_count = ""
    auto_fixable = ""
    recommendations = ""
    schema_valid = ""
    for line in lines:
        if "records scanned" in line.lower():
            record_count = line.strip()
        if "auto-fixable:" in line.lower():
            auto_fixable = line.strip()
        if "recommendations:" in line.lower():
            recommendations = line.strip()
        if "schema valid:" in line.lower():
            schema_valid = line.strip()

    # Extract check-specific highlights
    highlights = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("[") and "]" in stripped and ("found" in stripped or "issues across" in stripped):
            highlights.append(stripped)

    today = datetime.now().strftime("%Y-%m-%d")
    summary = f"Context Engine Weekly Audit — {today}\n"
    summary += f"{'=' * 45}\n\n"
    summary += f"{record_count}\n\n" if record_count else ""
    summary += f"{auto_fixable}\n" if auto_fixable else ""
    summary += f"{recommendations}\n" if recommendations else ""
    summary += f"{schema_valid}\n" if schema_valid else ""

    if highlights:
        summary += f"\nHighlights:\n"
        for h in highlights:
            summary += f"  {h}\n"

    if not highlights and "0 issues" in (auto_fixable + recommendations):
        summary += "\nAll clear — no issues found.\n"

    return summary


# ---------------------------------------------------------------------------
# Send email
# ---------------------------------------------------------------------------

def send_email(config: dict, subject: str, body: str, report_path: Path) -> bool:
    """Send the audit summary email. Returns True on success."""
    prefix = config.get("subject_prefix", "[Context Engine Audit]")
    full_subject = f"{prefix} {subject}"

    msg = MIMEMultipart()
    msg["From"] = config["sender_email"]
    msg["To"] = config["recipient_email"]
    msg["Subject"] = full_subject

    # Build email body
    email_body = body
    email_body += f"\n\n---\nFull report saved to: {report_path.name}"
    email_body += f"\nPath: {report_path}"
    email_body += "\n\nThis is an automated report from Context Engine weekly_audit.py"

    msg.attach(MIMEText(email_body, "plain"))

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
# Test email
# ---------------------------------------------------------------------------

def test_email(config: dict) -> bool:
    """Send a test email to verify configuration."""
    today = datetime.now().strftime("%Y-%m-%d")
    subject = f"Test — {today}"
    body = (
        "This is a test email from Context Engine weekly_audit.py.\n\n"
        "If you're reading this, your email configuration is working correctly.\n"
        "The weekly audit will send reports to this address."
    )

    msg = MIMEMultipart()
    msg["From"] = config["sender_email"]
    msg["To"] = config["recipient_email"]
    prefix = config.get("subject_prefix", "[Context Engine Audit]")
    msg["Subject"] = f"{prefix} {subject}"
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(config["smtp_server"], config["smtp_port"]) as server:
            server.starttls()
            server.login(config["sender_email"], config["sender_password"])
            server.send_message(msg)
        print("Test email sent successfully!")
        return True
    except Exception as e:
        print(f"Test email failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = set(sys.argv[1:])
    skip_email = "--no-email" in args
    skip_fix = "--no-fix" in args
    do_test = "--test-email" in args

    # Test email mode
    if do_test:
        config = load_email_config()
        if not config:
            print("Cannot test email — config not set up. Edit config/email-config.json")
            sys.exit(1)
        success = test_email(config)
        sys.exit(0 if success else 1)

    # Load email config (before audit, so we fail fast if misconfigured)
    email_config = None
    if not skip_email:
        email_config = load_email_config()
        if not email_config:
            print("Email not configured — will save report only.")
            print("To set up email, edit config/email-config.json\n")

    # Run audit
    today = datetime.now().strftime("%Y-%m-%d")
    fix = not skip_fix
    print(f"Running audit {'with --fix' if fix else '(report only)'}...")
    output, return_code = run_audit(fix=fix)

    if return_code != 0:
        print(f"Audit script exited with code {return_code}")

    # Save report
    report_path = save_report(output)
    print(f"Report saved: {report_path}")

    # Send email
    if email_config:
        summary = extract_summary(output)
        subject = f"Weekly Report — {today}"
        if send_email(email_config, subject, summary, report_path):
            print("Email sent successfully!")
        else:
            print("Email failed — report still saved to file.")

    # Print summary to console
    print("\n" + output)


if __name__ == "__main__":
    main()
