"""
Internal weekly inventory dashboard (for Jesse → Anjan).

Richer than the Zeni month-end report: current on-hand by product with
cost + retail value, open-box/returns, what's inbound (open POs), the last
7 days of shipped/received flow, and weeks-of-cover. Pulls entirely from
DCL's automated emails — no LLM, deterministic.

Sources:
  - "Items Status"        weekly on-hand snapshot (Mondays)
  - "Items Shipped Today" daily shipments (CSV)
  - "Items Received Today" daily receipts (XLSX)

Flags: --dry-run (compute + print, don't send).
"""
import os
import io
import re
import sys
import argparse
import smtplib
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

import pandas as pd
from imap_tools import MailBox, AND
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"), override=True)

IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")
IMAP_USERNAME = os.getenv("IMAP_USERNAME")
IMAP_PASSWORD = os.getenv("IMAP_PASSWORD")
EMAIL_SENDER = os.getenv("EMAIL_SENDER", "reports@notifications.dclcorp.com")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int((os.getenv("SMTP_PORT") or "465").strip() or "465")
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
# Internal report goes to Jesse (he forwards to Anjan). Override via INTERNAL_RECIPIENTS.
INTERNAL_RECIPIENTS = (os.getenv("INTERNAL_RECIPIENTS") or os.getenv("REPORT_RECIPIENT") or "")

DC1_SKUS = {"1", "6", "6-k"}
KIDS_SKUS = {"7"}


def _envfloat(name, default):
    """float() tolerant of unset/empty env (GitHub passes undefined secrets as '')."""
    v = (os.getenv(name) or "").strip()
    try:
        return float(v) if v else float(default)
    except ValueError:
        return float(default)


# Retail (list) price and production cost per unit. Env-overridable.
DC1_RETAIL = _envfloat("DC1_VALUE_USD", 729)
KIDS_RETAIL = _envfloat("KIDS_VALUE_USD", 799)
DC1_COST = _envfloat("DC1_COST_USD", 425)     # Anjan, May 2026 (production cost)
KIDS_COST = _envfloat("KIDS_COST_USD", 425)   # placeholder — Kids cost TBD (bundle)


def date_from_filename(filename, fallback):
    """DCL files embed the report date (timezone-proof) e.g. 'Items Status-2026-06-01_0000.csv'."""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", filename or "")
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            pass
    return fallback


def norm_items(series):
    """Clean Item # to strings, robust to pandas inferring the column as float
    (a blank/summary row turns '1' into '1.0', which would match no SKU and
    silently zero every count). Strip a trailing '.0' to be safe."""
    return series.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)


def _read_attachment(att):
    fn = att.filename.lower()
    if fn.endswith(".csv"):
        return pd.read_csv(io.BytesIO(att.payload))
    if fn.endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(att.payload))
    return None


def latest_status():
    """Most recent Items Status snapshot → (date, filename, payload, df)."""
    with MailBox(IMAP_SERVER).login(IMAP_USERNAME, IMAP_PASSWORD) as mb:
        crit = AND(subject="Items Status", from_=EMAIL_SENDER)
        for m in mb.fetch(crit, reverse=True, mark_seen=False, bulk=True):
            for att in m.attachments:
                if att.filename.lower().endswith(".csv"):
                    d = date_from_filename(att.filename, m.date.date())
                    return d, att.filename, att.payload, pd.read_csv(io.BytesIO(att.payload))
    return None


def daily_flows(start, end):
    """Sum daily shipped/received over [start, end] for DC-1 / Kids. Returns dict."""
    out = {"ship_dc1": 0, "ship_kids": 0, "recv_dc1": 0, "recv_kids": 0,
           "ship_days": set(), "recv_days": set()}
    with MailBox(IMAP_SERVER).login(IMAP_USERNAME, IMAP_PASSWORD) as mb:
        for subj, qty_col, sk, dk, dayset in (
            ("Items Shipped Today", "Shipped QTY", "ship_dc1", "ship_kids", "ship_days"),
            ("Items Received Today", "Q Received", "recv_dc1", "recv_kids", "recv_days"),
        ):
            crit = AND(subject=subj, from_=EMAIL_SENDER,
                       date_gte=start, date_lt=end + timedelta(days=2))
            for m in mb.fetch(crit, mark_seen=False, bulk=True):
                for att in m.attachments:
                    fn = att.filename.lower()
                    if not fn.endswith((".csv", ".xlsx", ".xls")):
                        continue
                    d = date_from_filename(att.filename, m.date.date())
                    if not (start <= d <= end):
                        break
                    if d in out[dayset]:   # dedup resends/corrections by date
                        break
                    df = _read_attachment(att)
                    if df is None or "Item #" not in df.columns or qty_col not in df.columns:
                        break
                    # Empty file = DCL feed didn't populate; treat as missing,
                    # not a real zero-flow day.
                    if len(df) == 0:
                        break
                    items = norm_items(df["Item #"])
                    qty = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)
                    out[sk] += int(qty[items.isin(DC1_SKUS)].sum())
                    out[dk] += int(qty[items.isin(KIDS_SKUS)].sum())
                    out[dayset].add(d)
                    break
    return out


def usd(x):
    return f"${x:,.0f}"


def build_report():
    snap = latest_status()
    if not snap:
        raise SystemExit("No Items Status snapshot found.")
    snap_date, fname, payload, df = snap
    df["Item #"] = norm_items(df["Item #"])
    oh = pd.to_numeric(df["Q On Hand"], errors="coerce").fillna(0)
    # df.get with a missing column returns a scalar, which breaks boolean
    # indexing below - default to a same-length zero Series instead.
    po_col = df["Open PO"] if "Open PO" in df.columns else pd.Series(0, index=df.index)
    po = pd.to_numeric(po_col, errors="coerce").fillna(0)

    dc1 = int(oh[df["Item #"].isin(DC1_SKUS)].sum())
    kids = int(oh[df["Item #"].isin(KIDS_SKUS)].sum())
    openbox = int(oh[df["Item #"].str.startswith("4-") | (df["Item #"] == "2")].sum())
    total = dc1 + kids

    dc1_po = int(po[df["Item #"].isin(DC1_SKUS)].sum())
    kids_po = int(po[df["Item #"].isin(KIDS_SKUS)].sum())

    # Trailing ~30 days of flow (covers the prior month for a monthly report)
    end = snap_date - timedelta(days=1)
    start = end - timedelta(days=29)
    flows = daily_flows(start, end)
    ship7 = flows["ship_dc1"] + flows["ship_kids"]
    recv7 = flows["recv_dc1"] + flows["recv_kids"]
    flow_avail = bool(flows["ship_days"] or flows["recv_days"])
    weeks_cover = (total / ship7) if ship7 > 0 else None

    return {
        "snap_date": snap_date, "fname": fname, "payload": payload,
        "dc1": dc1, "kids": kids, "openbox": openbox, "total": total,
        "dc1_po": dc1_po, "kids_po": kids_po,
        "flows": flows, "ship7": ship7, "recv7": recv7,
        "flow_avail": flow_avail, "weeks_cover": weeks_cover,
        "window": (start, end),
    }


def render_html(r):
    dc1_cost, kids_cost = r["dc1"] * DC1_COST, r["kids"] * KIDS_COST
    dc1_ret, kids_ret = r["dc1"] * DC1_RETAIL, r["kids"] * KIDS_RETAIL
    tot_cost, tot_ret = dc1_cost + kids_cost, dc1_ret + kids_ret
    s, e = r["window"]
    wc = f"{r['weeks_cover']:.1f} weeks" if r["weeks_cover"] else "n/a"
    flow_rows = (
        f"<tr><td>Shipped out</td><td align='right'>{r['flows']['ship_dc1']:,}</td>"
        f"<td align='right'>{r['flows']['ship_kids']:,}</td><td align='right'><b>{r['ship7']:,}</b></td></tr>"
        f"<tr style='background:#F4F6F8;'><td>Received in</td><td align='right'>{r['flows']['recv_dc1']:,}</td>"
        f"<td align='right'>{r['flows']['recv_kids']:,}</td><td align='right'><b>{r['recv7']:,}</b></td></tr>"
        if r["flow_avail"] else
        "<tr><td colspan='4' style='color:#888;'>No daily shipped/received reports in this window yet "
        "(DCL began sending them Jun 11 2026).</td></tr>"
    )
    return f"""
    <div style="font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px;color:#222;max-width:680px;">
      <p style="font-size:11px;letter-spacing:2px;color:#888;text-transform:uppercase;margin-bottom:2px;">
        Daylight · Inventory Snapshot</p>
      <p style="margin-top:0;"><b>As of {r['snap_date']}</b> (DCL fulfillment warehouse)</p>

      <p style="font-size:16px;"><b>{r['total']:,} fully-assembled units on hand</b>
         &nbsp;·&nbsp; {usd(tot_cost)} at cost &nbsp;·&nbsp; {usd(tot_ret)} at retail</p>

      <table cellpadding="7" cellspacing="0" style="border-collapse:collapse;font-size:13.5px;margin:10px 0;">
        <tr style="background:#1E3A5F;color:#fff;"><th align="left">Product</th>
          <th align="right">Units</th><th align="right">Cost value</th><th align="right">Retail value</th></tr>
        <tr><td>Daylight DC‑1</td><td align="right">{r['dc1']:,}</td>
            <td align="right">{usd(dc1_cost)}</td><td align="right">{usd(dc1_ret)}</td></tr>
        <tr style="background:#F4F6F8;"><td>Daylight Kids DC‑1</td><td align="right">{r['kids']:,}</td>
            <td align="right">{usd(kids_cost)}</td><td align="right">{usd(kids_ret)}</td></tr>
        <tr style="border-top:2px solid #1E3A5F;"><td><b>Total (new, sellable)</b></td>
            <td align="right"><b>{r['total']:,}</b></td><td align="right"><b>{usd(tot_cost)}</b></td>
            <td align="right"><b>{usd(tot_ret)}</b></td></tr>
        <tr style="color:#666;"><td>Open-box / returns (refurb)</td><td align="right">{r['openbox']:,}</td>
            <td align="right" colspan="2">valued separately</td></tr>
      </table>

      <table cellpadding="7" cellspacing="0" style="border-collapse:collapse;font-size:13.5px;margin:10px 0;">
        <tr style="background:#1E3A5F;color:#fff;"><th align="left">Last 7 days ({s} → {e})</th>
          <th align="right">DC‑1</th><th align="right">Kids</th><th align="right">Total</th></tr>
        {flow_rows}
      </table>

      <p style="font-size:13.5px;">
        <b>Inbound (open POs):</b> {r['dc1_po']:,} DC‑1 · {r['kids_po']:,} Kids on order arriving.<br>
        <b>Weeks of cover:</b> {wc} (on-hand ÷ last-week ship rate).
      </p>

      <p style="font-size:12px;color:#777;">
        New finished sellable units only (DC‑1 + Kids); excludes accessories, components, open-box
        (shown separately), and office stock. Cost = production cost per unit (${DC1_COST:,.0f} DC‑1;
        Kids ${KIDS_COST:,.0f} <i>placeholder — pending confirmation</i>). Retail = list price
        (${DC1_RETAIL:,.0f} DC‑1 / ${KIDS_RETAIL:,.0f} Kids). Source: DCL "Items Status" {r['snap_date']} (attached).
      </p>
      <p style="font-size:12px;color:#777;">— Automated weekly inventory snapshot.</p>
    </div>
    """


def send(r):
    html = render_html(r)
    to_list = [a.strip() for a in INTERNAL_RECIPIENTS.split(",") if a.strip()]
    if not to_list:
        raise SystemExit("No INTERNAL_RECIPIENTS / REPORT_RECIPIENT set.")
    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"Inventory Snapshot — {r['total']:,} units on hand (as of {r['snap_date']})"
    msg["From"] = SMTP_USERNAME
    msg["To"] = ", ".join(to_list)
    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(f"Inventory snapshot as of {r['snap_date']}: {r['total']:,} units on hand "
                        f"(DC-1 {r['dc1']:,} + Kids {r['kids']:,}).", "plain"))
    alt.attach(MIMEText(html, "html"))
    msg.attach(alt)
    part = MIMEApplication(r["payload"], Name=r["fname"])
    part["Content-Disposition"] = f'attachment; filename="{r["fname"]}"'
    msg.attach(part)
    if SMTP_PORT == 465:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as srv:
            srv.login(SMTP_USERNAME, SMTP_PASSWORD); srv.send_message(msg)
    else:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as srv:
            srv.starttls(); srv.login(SMTP_USERNAME, SMTP_PASSWORD); srv.send_message(msg)
    print(f"Sent internal snapshot to {to_list}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--monthly", action="store_true",
                    help="monthly cadence: only send on the day the first new-month "
                         "snapshot arrives (so a daily 1st-7th cron fires once)")
    args = ap.parse_args()
    r = build_report()

    if args.monthly and not args.dry_run:
        today = date.today()
        if today.day > 7 or r["snap_date"] != today:
            print(f"--monthly: snapshot {r['snap_date']} != today {today} (or past day 7) "
                  f"- exiting quietly.")
            return
    print(f"As of {r['snap_date']}: DC-1 {r['dc1']}, Kids {r['kids']}, total {r['total']}, "
          f"open-box {r['openbox']}, inbound POs DC1 {r['dc1_po']}/Kids {r['kids_po']}, "
          f"7d shipped {r['ship7']} / received {r['recv7']}")
    if args.dry_run:
        print("Dry run — not sending."); return
    send(r)


if __name__ == "__main__":
    main()
