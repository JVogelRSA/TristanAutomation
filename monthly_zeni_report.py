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
  3. Combines warehouse new + open-box with the office sheet (via
     inventory_core, shared with the internal report) and values them on
     Zeni's basis: New + Open-box at cost (shown at retail too), Warranty
     as a count-only memo.
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

import inventory_core as core
import office_inventory

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

# SKU sets and per-unit money live in inventory_core, shared with the internal
# (Anjan) report so the two can never disagree. Valuation basis confirmed by
# Zeni (Amol Taxali, 2026-06-17): New + Open-box valued at cost (shown at retail
# too); Warranty units are a count-only memo (carried at NRV, not full cost).
DC1_SKUS = core.DC1_SKUS
KIDS_SKUS = core.KIDS_SKUS
DC1_VALUE, KIDS_VALUE = core.DC1_RETAIL, core.KIDS_RETAIL   # retail list price
DC1_COST, KIDS_COST = core.DC1_COST, core.KIDS_COST          # production cost
norm_items = core.norm_items
date_from_filename = core.date_from_filename
# Set to "1" once cost is confirmed; until then cost stays a [PREVIEW] to us.
COST_CONFIRMED = os.getenv("COST_CONFIRMED", "0") == "1"


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

    wh = core.warehouse_breakdown(df)
    dc1, kids = wh["new_dc1"], wh["new_kids"]   # new units; walked back below

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

    # Merge the (possibly walked-back) warehouse new units with warehouse
    # open-box and the office sheet, then value on Zeni's basis: New + Open-box
    # at cost (+ retail), Warranty as a count-only memo.
    wh["new_dc1"], wh["new_kids"] = dc1, kids
    office = office_inventory.office_summary()
    office_ok = office is not None
    if not office_ok:
        office = core.empty_office()
    data = core.combine(wh, office)

    new_d, new_k = data["new"]
    ob_d, ob_k = data["openbox"]
    sell_units = data["sellable_units"]
    sell_cost, sell_ret = data["sellable_cost"], data["sellable_ret"]
    warranty_units = data["warranty_units"]
    print(f"Sellable (new+open-box): {sell_units} "
          f"(new {new_d + new_k} + open-box {ob_d + ob_k}) ~ "
          f"${sell_cost:,.0f} cost / ${sell_ret:,.0f} retail; "
          f"warranty memo {warranty_units}; office_ok={office_ok}")

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
    sell_d, sell_k = data["sellable"]
    new_units, ob_units = new_d + new_k, ob_d + ob_k
    new_cost, new_ret = data["new_cost"], data["new_ret"]
    ob_cost, ob_ret = data["ob_cost"], data["ob_ret"]
    cost_prov = "" if COST_CONFIRMED else " — provisional, pending confirmation"
    html = f"""
    <div style="font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;
                font-size:14px;color:#222;max-width:620px;">
      <p>Hi Amol,</p>
      <p>Fully assembled units (DC-1 + Kids) on hand for the <b>{month_label}</b>
         close, per your note — New and Open-box valued at cost, Warranty as a
         count-only memo:</p>
      <table cellpadding="8" cellspacing="0"
             style="border-collapse:collapse;font-size:13.5px;margin:12px 0;">
        <tr style="background:#1E3A5F;color:#fff;">
          <th align="left">Category</th><th align="right">DC-1</th>
          <th align="right">Kids</th><th align="right">Units</th>
          <th align="right">Cost value</th><th align="right">Retail value</th></tr>
        <tr><td>New (sellable)</td><td align="right">{new_d:,}</td>
            <td align="right">{new_k:,}</td><td align="right">{new_units:,}</td>
            <td align="right">${new_cost:,.0f}</td><td align="right">${new_ret:,.0f}</td></tr>
        <tr style="background:#F4F6F8;"><td>Open-box (graded returns)</td>
            <td align="right">{ob_d:,}</td><td align="right">{ob_k:,}</td>
            <td align="right">{ob_units:,}</td>
            <td align="right">${ob_cost:,.0f}</td>
            <td align="right" style="color:#777;">${ob_ret:,.0f}*</td></tr>
        <tr style="border-top:2px solid #1E3A5F;"><td><b>Total (New + Open-box)</b></td>
            <td align="right"><b>{sell_d:,}</b></td><td align="right"><b>{sell_k:,}</b></td>
            <td align="right"><b>{sell_units:,}</b></td>
            <td align="right"><b>${sell_cost:,.0f}</b></td>
            <td align="right"><b>${sell_ret:,.0f}</b></td></tr>
      </table>
      <p style="font-size:13px;color:#555;margin:6px 0;">
        <b>Warranty (memo):</b> {warranty_units:,} units awaiting repair — held at
        net realizable value, not included in the totals above.</p>
      <p style="font-size:12.5px;color:#555;">
        Units are DC-1 + Kids combined (split in the table). Open-box is carried
        at full production cost per your basis; the *asterisk notes that open-box
        <i>retail</i> is shown at list, though B-stock typically sells at a discount.<br>
        <b>Cost</b> = production cost (${DC1_COST:,.0f}/unit, both models{cost_prov}).
        <b>Retail</b> = list price (${DC1_VALUE:,.0f} DC-1, ${KIDS_VALUE:,.0f} Kids).<br>
        Source: DCL warehouse snapshot dated {snap_date} (as of the {month_label}
        close, attached); office stock is as of the report date. {adj_note}</p>
      {stale_note}
      <p>This is generated automatically after each month-end. Reply with any
         questions and the team will follow up.</p>
      <p>- Daylight Computer (automated report)</p>
    </div>
    """
    text = (
        f"Fully assembled inventory (DC-1 + Kids) - {month_label} close "
        f"(New + Open-box at cost; Warranty memo)\n\n"
        f"  {'Category':<26}{'Units':>7}{'Cost value':>14}{'Retail value':>15}\n"
        f"  {'New (sellable)':<26}{new_units:>7,}{'$'+format(new_cost,',.0f'):>14}{'$'+format(new_ret,',.0f'):>15}\n"
        f"  {'Open-box (returns)':<26}{ob_units:>7,}{'$'+format(ob_cost,',.0f'):>14}{'$'+format(ob_ret,',.0f'):>15}\n"
        f"  {'TOTAL (New+Open-box)':<26}{sell_units:>7,}{'$'+format(sell_cost,',.0f'):>14}{'$'+format(sell_ret,',.0f'):>15}\n\n"
        f"Warranty (memo): {warranty_units:,} units awaiting repair, held at NRV, not in totals.\n\n"
        f"Cost = production cost (${DC1_COST:,.0f}/unit both models{' provisional' if not COST_CONFIRMED else ''}); "
        f"open-box carried at full cost per your basis.\n"
        f"Retail = list price (${DC1_VALUE:,.0f} DC-1, ${KIDS_VALUE:,.0f} Kids); open-box retail at list.\n"
        f"Source: DCL warehouse snapshot {snap_date} (as of {month_label} close, attached); "
        f"office stock as of report date. {adj_note}"
    )

    send_email(subject, html, text, to_list, cc_list, (fname, payload))


if __name__ == "__main__":
    main()
