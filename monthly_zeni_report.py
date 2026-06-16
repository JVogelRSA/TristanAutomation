"""
Monthly Zeni inventory report.

Sends Zeni (our accountants) the number of fully assembled units on hand
(DC-1 + Kids) at the end of each month, per Anjan's request (Jun 2026).

How it works
------------
DCL emails an "Items Status" inventory snapshot every Monday at 00:01 PT,
plus (since Jun 11 2026) daily "Items Shipped Today" / "Items Received
Today" flow reports each evening. This script:

  1. Picks the first Items Status snapshot dated on/after the last day of
     the previous month (falls back to the latest one before it, flagged
     stale). A snapshot dated D reflects on-hand at the close of D-1.
  2. If the snapshot is later than the 1st, walks it BACK to the exact
     month-end using the daily flow reports: month-end = snapshot
     + units shipped in between - units received in between.
  3. Counts finished units: DC-1 family (SKUs 1, 6, 6-k) + Kids (SKU 7),
     valued at retail price per product (DC1_VALUE_USD, default $729;
     KIDS_VALUE_USD, default $799).
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
SMTP_PORT = int((os.getenv("SMTP_PORT") or "465").strip() or "465")  # Gmail SSL=465
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

# Valuation per unit. Zeni asked for BOTH cost value and retail value.
#  - Retail = list price.        Override: DC1_VALUE_USD / KIDS_VALUE_USD
#  - Cost   = production cost.   Override: DC1_COST_USD / KIDS_COST_USD
# DC-1 cost ($425) is Anjan's production-cost figure from May 2026 ("what
# DCL books inventory at"). Kids cost is a placeholder (Kids is a bundle,
# not separately costed yet) - pending Anjan's confirmation.
def _envfloat(name, default):
    """float() of an env var, tolerant of unset/empty (GitHub passes an
    undefined secret as an empty string, which would crash float('')."""
    v = (os.getenv(name) or "").strip()
    try:
        return float(v) if v else float(default)
    except ValueError:
        return float(default)


DC1_VALUE = _envfloat("DC1_VALUE_USD", 729)
KIDS_VALUE = _envfloat("KIDS_VALUE_USD", 799)
DC1_COST = _envfloat("DC1_COST_USD", 425)
KIDS_COST = _envfloat("KIDS_COST_USD", 425)
# Set to "1" once Anjan has confirmed the cost figures; until then the
# report watermarks the cost column as provisional.
COST_CONFIRMED = os.getenv("COST_CONFIRMED", "0") == "1"


import re
from datetime import datetime


def date_from_filename(filename, fallback):
    """DCL files embed the report date, e.g. 'Items Status-2026-06-01_0000.csv'.
    Use that (timezone-proof) rather than the email's received timestamp."""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", filename or "")
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            pass
    return fallback


def fetch_snapshots(limit=10):
    """Return recent Items Status snapshots as [(date, filename, df, payload)], oldest first."""
    snaps = []
    with MailBox(IMAP_SERVER).login(IMAP_USERNAME, IMAP_PASSWORD) as mb:
        crit = AND(subject=EMAIL_SUBJECT_KEYWORD, from_=EMAIL_SENDER)
        for msg in mb.fetch(crit, limit=limit, reverse=True, bulk=True):
            for att in msg.attachments:
                if att.filename.lower().endswith(".csv"):
                    try:
                        df = pd.read_csv(io.BytesIO(att.payload))
                        d = date_from_filename(att.filename, msg.date.date())
                        snaps.append((d, att.filename, df, att.payload))
                    except Exception as e:
                        print(f"Could not parse {att.filename}: {e}")
                    break
    snaps.sort(key=lambda s: s[0])
    return snaps


def pick_snapshot(snaps, month_end):
    """
    A snapshot dated D reflects the close of D-1, so the month-end close is
    captured by the first snapshot dated >= month_end + 1 (e.g. the Jun 1
    file = close of May 31). Within ~10 days of that. The walk-back in main()
    then trims any extra days back to the exact month-end. Falls back to the
    latest snapshot before that (flagged stale).
    Returns (snap, is_stale).
    """
    target = month_end + timedelta(days=1)
    after = [s for s in snaps if target <= s[0] <= target + timedelta(days=10)]
    if after:
        return after[0], False
    before = [s for s in snaps if s[0] < target]
    if before:
        return before[-1], True
    return None, False


def norm_items(series):
    """Normalize the Item # column to clean strings.

    Critical: if a snapshot CSV contains any blank/NaN Item # cell (a trailing
    blank row, a summary row, etc.) pandas types the whole column float64, and
    a plain .astype(str) turns SKU '1' into '1.0' - which matches NONE of our
    SKU sets and silently collapses every count to 0. Stripping a trailing
    '.0' makes the match robust regardless of inferred dtype.
    """
    return series.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)


def count_units(df):
    """Count finished units in a snapshot dataframe."""
    items = norm_items(df["Item #"])
    qty = pd.to_numeric(df["Q On Hand"], errors="coerce").fillna(0)
    dc1 = int(qty[items.isin(DC1_SKUS)].sum())
    kids = int(qty[items.isin(KIDS_SKUS)].sum())
    return dc1, kids


def _sum_finished(df, qty_col):
    """Sum a flow report's quantity column for DC-1 / Kids SKUs."""
    if df.empty or "Item #" not in df.columns or qty_col not in df.columns:
        return 0, 0
    items = norm_items(df["Item #"])
    qty = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)
    return (int(qty[items.isin(DC1_SKUS)].sum()),
            int(qty[items.isin(KIDS_SKUS)].sum()))


def fetch_daily_flows(start, end):
    """
    Sum DCL's daily "Items Shipped Today" / "Items Received Today" reports
    for finished SKUs over [start, end] inclusive.
    Returns (shipped_dc1, shipped_kids, recv_dc1, recv_kids, days_found).
    """
    shipped = {"dc1": 0, "kids": 0}
    received = {"dc1": 0, "kids": 0}
    days_found = set()

    with MailBox(IMAP_SERVER).login(IMAP_USERNAME, IMAP_PASSWORD) as mb:
        for subj, qty_col, bucket in (
            ("Items Shipped Today", "Shipped QTY", shipped),
            ("Items Received Today", "Q Received", received),
        ):
            crit = AND(subject=subj, from_=EMAIL_SENDER,
                       date_gte=start, date_lt=end + timedelta(days=2))
            for msg in mb.fetch(crit, mark_seen=False, bulk=True):
                for att in msg.attachments:
                    fn = att.filename.lower()
                    if not fn.endswith((".csv", ".xlsx", ".xls")):
                        continue
                    # Use the report date embedded in the filename (tz-proof).
                    d = date_from_filename(att.filename, msg.date.date())
                    if not (start <= d <= end):
                        break
                    # Dedup: if DCL resends/corrects a day, only count it once
                    # (use the same key that gates completeness below).
                    if (subj, d) in days_found:
                        break
                    try:
                        df = (pd.read_csv(io.BytesIO(att.payload)) if fn.endswith(".csv")
                              else pd.read_excel(io.BytesIO(att.payload)))
                    except Exception as e:
                        print(f"Could not parse {att.filename}: {e}")
                        break
                    # A 0-row file means the report didn't populate (DCL's
                    # daily feed has been arriving empty) - treat as MISSING,
                    # not as a real zero-flow day, so we don't claim a false
                    # "exact" walk-back.
                    if len(df) == 0:
                        break
                    dc1, kids = _sum_finished(df, qty_col)
                    bucket["dc1"] += dc1
                    bucket["kids"] += kids
                    days_found.add((subj, d))
                    break

    return shipped["dc1"], shipped["kids"], received["dc1"], received["kids"], days_found


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
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, timeout=60) as s:
            s.login(SMTP_USERNAME, SMTP_PASSWORD)
            s.send_message(msg)
    else:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=60) as s:
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

    # A snapshot dated S (taken 00:01) reflects the close of S-1. If the
    # snapshot is later than month_end+1, walk it back to the exact close
    # using DCL's daily flow reports for the in-between days.
    adj_note = ""
    window_start = month_end + timedelta(days=1)
    window_end = snap_date - timedelta(days=1)
    if not stale and window_end >= window_start:
        s_dc1, s_kids, r_dc1, r_kids, days_found = fetch_daily_flows(window_start, window_end)
        n_days = (window_end - window_start).days + 1
        have_ship = len({d for s, d in days_found if s == "Items Shipped Today"})
        have_recv = len({d for s, d in days_found if s == "Items Received Today"})
        adj_dc1 = dc1 + s_dc1 - r_dc1
        adj_kids = kids + s_kids - r_kids
        # Sanity guard: if the daily-flow adjustment produces a negative or
        # wildly different figure (data hiccup / duplicate receipt), don't put
        # it in front of accountants - fall back to the snapshot as-is.
        implausible = (adj_dc1 < 0 or adj_kids < 0
                       or abs((adj_dc1 + adj_kids) - (dc1 + kids)) > (dc1 + kids))
        if have_ship >= n_days and have_recv >= n_days and not implausible:
            dc1, kids = adj_dc1, adj_kids
            adj_note = (
                f"Month-end position computed from DCL's {snap_date} snapshot, "
                f"adjusted back to {month_end} using DCL's daily shipped/received "
                f"reports ({window_start} to {window_end}: +{s_dc1 + s_kids} shipped, "
                f"-{r_dc1 + r_kids} received)."
            )
            print(f"Adjusted to exact month-end via daily flows: +{s_dc1+s_kids} shipped, -{r_dc1+r_kids} received")
        else:
            reason = "implausible" if implausible else "incomplete"
            adj_note = (
                f"Position as of DCL's {snap_date} snapshot (daily flow data "
                f"{reason} for {window_start}-{window_end}, so the close of "
                f"{month_end} is approximated by the nearest snapshot)."
            )
            print(f"Daily flows {reason} ({have_ship}/{n_days} shipped, {have_recv}/{n_days} received) - using snapshot as-is.")

    total = dc1 + kids
    dc1_ret, kids_ret = dc1 * DC1_VALUE, kids * KIDS_VALUE
    dc1_cost, kids_cost = dc1 * DC1_COST, kids * KIDS_COST
    total_ret = dc1_ret + kids_ret
    total_cost = dc1_cost + kids_cost
    print(f"Fully assembled units: {total} (DC-1 {dc1} + Kids {kids}) ~ "
          f"${total_cost:,.0f} cost / ${total_ret:,.0f} retail")

    if args.dry_run:
        print("Dry run - not sending.")
        return

    # Recipients. Two gates before anything reaches Zeni:
    #   1. ZENI_RECIPIENTS must be set, AND
    #   2. COST_CONFIRMED must be "1" - we must NOT send provisional/placeholder
    #      cost numbers to accountants. Until Anjan confirms DC-1 + Kids cost,
    #      this stays a [PREVIEW] to REPORT_RECIPIENT no matter what.
    to_list = [a.strip() for a in ZENI_RECIPIENTS.split(",") if a.strip()]
    cc_list = [a.strip() for a in ZENI_CC.split(",") if a.strip()]
    preview = (not to_list) or (not COST_CONFIRMED)
    if preview:
        if not REPORT_RECIPIENT:
            print("No REPORT_RECIPIENT set for preview - aborting (refusing to send).")
            sys.exit(1)
        to_list = [REPORT_RECIPIENT]
        cc_list = []
        if not COST_CONFIRMED:
            print("COST_CONFIRMED!=1 - sending PREVIEW only (won't send provisional cost to Zeni).")

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
          <th align="left">Product</th><th align="right">Units</th>
          <th align="right">Cost value</th><th align="right">Retail value</th></tr>
        <tr><td>Daylight DC-1</td><td align="right">{dc1:,}</td>
            <td align="right">${dc1_cost:,.0f}</td><td align="right">${dc1_ret:,.0f}</td></tr>
        <tr style="background:#F4F6F8;"><td>Daylight Kids DC-1</td>
            <td align="right">{kids:,}</td>
            <td align="right">${kids_cost:,.0f}</td><td align="right">${kids_ret:,.0f}</td></tr>
        <tr style="border-top:2px solid #1E3A5F;"><td><b>Total</b></td>
            <td align="right"><b>{total:,}</b></td>
            <td align="right"><b>${total_cost:,.0f}</b></td>
            <td align="right"><b>${total_ret:,.0f}</b></td></tr>
      </table>
      <p style="font-size:12.5px;color:#555;">
        Source: DCL "Items Status" inventory report dated {snap_date}
        (attached). {adj_note} Counts new finished devices only - excludes
        open-box returns, accessories, components, and any units held at
        our office.<br>
        <b>Cost value</b> = units &times; production cost (${DC1_COST:,.0f}
        per DC-1{'' if COST_CONFIRMED else ' — provisional, pending finance confirmation'};
        Kids ${KIDS_COST:,.0f}{'' if COST_CONFIRMED else ' placeholder'}).
        <b>Retail value</b> = units &times; list price (${DC1_VALUE:,.0f}
        per DC-1, ${KIDS_VALUE:,.0f} per Kids DC-1).
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
        f"  {'Product':<20}{'Units':>7}{'Cost value':>14}{'Retail value':>15}\n"
        f"  {'Daylight DC-1':<20}{dc1:>7,}{'$'+format(dc1_cost,',.0f'):>14}{'$'+format(dc1_ret,',.0f'):>15}\n"
        f"  {'Daylight Kids DC-1':<20}{kids:>7,}{'$'+format(kids_cost,',.0f'):>14}{'$'+format(kids_ret,',.0f'):>15}\n"
        f"  {'TOTAL':<20}{total:>7,}{'$'+format(total_cost,',.0f'):>14}{'$'+format(total_ret,',.0f'):>15}\n\n"
        f"Source: DCL Items Status report dated {snap_date} (attached). {adj_note}\n"
        f"New finished devices only - excludes open-box returns, accessories,\n"
        f"components, and office units.\n"
        f"Cost value = units x production cost (${DC1_COST:,.0f} DC-1"
        f"{'' if COST_CONFIRMED else ' provisional'}; Kids ${KIDS_COST:,.0f}"
        f"{'' if COST_CONFIRMED else ' placeholder'}).\n"
        f"Retail value = units x list price (${DC1_VALUE:,.0f} DC-1, ${KIDS_VALUE:,.0f} Kids)."
    )

    send_email(subject, html, text, to_list, cc_list, (fname, payload))


if __name__ == "__main__":
    main()
