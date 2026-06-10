"""
Monthly Zeni inventory report.

Sends Zeni (our accountants) the number of fully assembled units on hand
(DC-1 + Kids) at the end of each month, per Anjan's request (Jun 2026).

How it works
------------
DCL emails an "Items Status" inventory snapshot every Monday at 00:01 PT.
The first snapshot of a new month is the closest thing we have to the
prior month-end position (e.g. the file dated Jun 1 reflects the close of
May 31). This script:

  1. Looks at the most recent Items Status emails in the inbox.
  2. Picks the first snapshot dated on/after the last day of the previous
     month (falls back to the latest one before it, flagged as stale).
  3. Counts finished units: DC-1 family (SKUs 1, 6, 6-k) + Kids (SKU 7).
  4. Emails Zeni a short summary with the source CSV attached.

Scheduling: the GitHub Actions workflow runs daily on the 1st-7th of each
month. The script only sends on the day the chosen snapshot actually
arrived (snapshot date == today), so it fires exactly once per month -
the morning the first new-month DCL email lands - and no-ops otherwise.

Flags:
  --force    send now using the best available snapshot (manual/backfill)
  --dry-run  compute and print, but don't send
"""
import os
import io
import sys
import argparse
import smtplib
from datetime import date, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

import pandas as pd
from imap_tools import MailBox, AND
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'), override=True)

IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")
IMAP_USERNAME = os.getenv("IMAP_USERNAME")
IMAP_PASSWORD = os.getenv("IMAP_PASSWORD")
EMAIL_SUBJECT_KEYWORD = os.getenv("EMAIL_SUBJECT_KEYWORD", "Items Status")
EMAIL_SENDER = os.getenv("EMAIL_SENDER", "reports@notifications.dclcorp.com")

SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

# Where the report goes. Comma-separated lists. Until Zeni confirms their
# address, ZENI_RECIPIENTS should stay pointed at us (preview mode).
ZENI_RECIPIENTS = os.getenv("ZENI_RECIPIENTS", "")
ZENI_CC = os.getenv("ZENI_CC", "")
REPORT_RECIPIENT = os.getenv("REPORT_RECIPIENT")  # fallback / preview

# What counts as a fully assembled, sellable unit.
# SKU 1 = Daylight DC-1, 6 / 6-k = POS units (usually zero), 7 = Kids DC-1.
# Open-box/returned units (4-x-x), bundles (N-type virtual SKUs) and
# accessories are excluded.
DC1_SKUS = {"1", "6", "6-k"}
KIDS_SKUS = {"7"}


def fetch_snapshots(limit=10):
    """Return recent Items Status snapshots as [(date, filename, df)], oldest first."""
    snaps = []
    with MailBox(IMAP_SERVER).login(IMAP_USERNAME, IMAP_PASSWORD) as mb:
        crit = AND(subject=EMAIL_SUBJECT_KEYWORD, from_=EMAIL_SENDER)
        for msg in mb.fetch(crit, limit=limit, reverse=True, bulk=True):
            for att in msg.attachments:
                if att.filename.lower().endswith(".csv"):
                    try:
                        df = pd.read_csv(io.BytesIO(att.payload))
                        snaps.append((msg.date.date(), att.filename, df, att.payload))
                    except Exception as e:
                        print(f"Could not parse {att.filename}: {e}")
                    break
    snaps.sort(key=lambda s: s[0])
    return snaps


def pick_snapshot(snaps, month_end):
    """
    Prefer the first snapshot dated on/after month_end (within 10 days) -
    that file reflects the month-end position. Fall back to the latest
    snapshot before month_end (flagged stale).
    Returns (snap, is_stale).
    """
    after = [s for s in snaps if month_end <= s[0] <= month_end + timedelta(days=10)]
    if after:
        return after[0], False
    before = [s for s in snaps if s[0] < month_end]
    if before:
        return before[-1], True
    return None, False


def count_units(df):
    """Count finished units in a snapshot dataframe."""
    items = df["Item #"].astype(str).str.strip()
    qty = pd.to_numeric(df["Q On Hand"], errors="coerce").fillna(0)
    dc1 = int(qty[items.isin(DC1_SKUS)].sum())
    kids = int(qty[items.isin(KIDS_SKUS)].sum())
    return dc1, kids


def send_email(subject, html_body, text_body, to_list, cc_list, attachment):
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = SMTP_USERNAME
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(text_body, "plain"))
    alt.attach(MIMEText(html_body, "html"))
    msg.attach(alt)

    if attachment:
        fname, data = attachment
        part = MIMEApplication(data, Name=fname)
        part["Content-Disposition"] = f'attachment; filename="{fname}"'
        msg.attach(part)

    if SMTP_PORT == 465:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as s:
            s.login(SMTP_USERNAME, SMTP_PASSWORD)
            s.send_message(msg)
    else:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
            s.starttls()
            s.login(SMTP_USERNAME, SMTP_PASSWORD)
            s.send_message(msg)
    print(f"Sent to {to_list} (cc {cc_list or '-'})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true",
                    help="send now using the best available snapshot")
    ap.add_argument("--dry-run", action="store_true",
                    help="compute and print, don't send")
    args = ap.parse_args()

    today = date.today()
    month_end = today.replace(day=1) - timedelta(days=1)
    month_label = month_end.strftime("%B %Y")

    print(f"Reporting month: {month_label} (month-end {month_end})")
    snaps = fetch_snapshots()
    if not snaps:
        print("No DCL Items Status emails found - aborting.")
        sys.exit(1)

    chosen, stale = pick_snapshot(snaps, month_end)
    if not chosen:
        print("No usable snapshot found - aborting.")
        sys.exit(1)
    snap_date, fname, df, payload = chosen
    print(f"Chosen snapshot: {fname} (dated {snap_date}, stale={stale})")

    # Cron guard: without --force, only send the day the snapshot arrived,
    # within the first week of the month. Daily 1st-7th runs -> fires once.
    if not args.force:
        if today.day > 7 or snap_date != today:
            print("Not send day (snapshot didn't arrive today) - exiting quietly.")
            return

    dc1, kids = count_units(df)
    total = dc1 + kids
    print(f"Fully assembled units: {total} (DC-1 {dc1} + Kids {kids})")

    if args.dry_run:
        print("Dry run - not sending.")
        return

    # Recipients: until Zeni's address is configured, send a preview to us.
    to_list = [a.strip() for a in ZENI_RECIPIENTS.split(",") if a.strip()]
    cc_list = [a.strip() for a in ZENI_CC.split(",") if a.strip()]
    preview = not to_list
    if preview:
        to_list = [REPORT_RECIPIENT]

    subject = f"Daylight Computer - Fully Assembled Units On Hand - {month_label} Month-End"
    if preview:
        subject = "[PREVIEW] " + subject

    stale_note = (
        f"<p style='color:#B7600E;'><b>Note:</b> the snapshot pre-dates month-end "
        f"by {(month_end - snap_date).days} day(s); no later report was available.</p>"
        if stale else ""
    )
    html = f"""
    <div style="font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;
                font-size:14px;color:#222;max-width:560px;">
      <p>Hi Zeni team,</p>
      <p>Fully assembled, ready-to-sell units on hand at our fulfillment
         warehouse (DCL) for the <b>{month_label}</b> close:</p>
      <table cellpadding="8" cellspacing="0"
             style="border-collapse:collapse;font-size:14px;margin:12px 0;">
        <tr style="background:#1E3A5F;color:#fff;">
          <th align="left">Product</th><th align="right">Units</th></tr>
        <tr><td>Daylight DC-1</td><td align="right">{dc1:,}</td></tr>
        <tr style="background:#F4F6F8;"><td>Daylight Kids DC-1</td>
            <td align="right">{kids:,}</td></tr>
        <tr style="border-top:2px solid #1E3A5F;"><td><b>Total</b></td>
            <td align="right"><b>{total:,}</b></td></tr>
      </table>
      <p style="font-size:12.5px;color:#555;">
        Source: DCL "Items Status" inventory report dated {snap_date}
        (attached). Counts new finished devices only - excludes open-box
        returns, accessories, components, and any units held at our office.
      </p>
      {stale_note}
      <p>This report is generated automatically on the first DCL inventory
         snapshot after each month-end. Reply to this email with any
         questions and the team will follow up.</p>
      <p>- Daylight Computer (automated report)</p>
    </div>
    """
    text = (
        f"Fully assembled units on hand at DCL - {month_label} close\n\n"
        f"  Daylight DC-1:      {dc1:,}\n"
        f"  Daylight Kids DC-1: {kids:,}\n"
        f"  TOTAL:              {total:,}\n\n"
        f"Source: DCL Items Status report dated {snap_date} (attached).\n"
        f"New finished devices only - excludes open-box returns, accessories,\n"
        f"components, and office units."
    )

    send_email(subject, html, text, to_list, cc_list, (fname, payload))


if __name__ == "__main__":
    main()
