"""
Internal monthly inventory report (for Jesse -> Anjan).

The full picture Anjan wants: DCL **warehouse** finished goods + the **office**
stock tracked in the "Daylight IMS" Google Sheet, merged into one report —
new / open-box / warranty by model and location, cost + retail valuation,
open-box grades, the office warranty-repair queue, accessories, inbound POs,
trailing flow and weeks-of-cover. Deterministic, no LLM.

Both reports combine warehouse + office on the same valuation basis (via
inventory_core); this internal one adds the operational detail (open-box
grades, warranty queue, accessories, flow) on top.

Sources:
  - DCL "Items Status"  on-hand snapshot (warehouse)            [email]
  - Daylight IMS sheet, Report tab (office)                     [office_inventory.py]
  - DCL "Items Shipped/Received Today" daily flow              [email]

Flags: --dry-run (compute + print, don't send); --monthly (fire once/month).
"""
import os
import io
import re
import argparse
import smtplib
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

import pandas as pd
from imap_tools import MailBox, AND
from dotenv import load_dotenv

import inventory_core as core
import office_inventory

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

# SKU sets, per-unit money, and the warehouse+office combine all live in
# inventory_core so this report and the Zeni report can never disagree.
DC1_SKUS = core.DC1_SKUS
KIDS_SKUS = core.KIDS_SKUS
DC1_COST, KIDS_COST = core.DC1_COST, core.KIDS_COST
DC1_RETAIL, KIDS_RETAIL = core.DC1_RETAIL, core.KIDS_RETAIL
norm_items = core.norm_items
date_from_filename = core.date_from_filename

# Accessory alignment: office sheet label -> warehouse SKU(s) to sum on-hand.
WH_ACC = {
    "Kids Case": ["31"],
    "Daylight Sling": ["23"],
    "Comfy Sleeve": ["28"],
    "Lamy Stylus": ["29"],
    "Stands": ["34-", "34-1"],
    "Wood Lamp Fixture": ["40"],
    "36 - (T45)": ["36-"],
    "36-1 - (T45 Deep Amber)": ["36-1"],
    "37 - (ST64)": ["37-"],
    "37-1 (ST64 Deep Amber)": ["37-1"],
    "37-2 (ST64 Bright 60W)": ["37-2"],
    "41 - (G80 Red)": ["41"],
    "Light Bulbs (All SKUs)": ["36-", "36-1", "37-", "37-1", "37-2", "41"],
}
# Warehouse-only accessories worth surfacing (no office row).
EXTRA_WH_ACC = [("Kids Stylus", ["30"]), ("Keyboard Case", ["32"]),
                ("Logitech keyboard", ["35-"]), ("Kids Night Light", ["38-"])]


def _read_attachment(att):
    fn = att.filename.lower()
    if fn.endswith(".csv"):
        return pd.read_csv(io.BytesIO(att.payload))
    if fn.endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(att.payload))
    return None


def latest_status():
    """Most recent Items Status snapshot -> (date, filename, payload, df)."""
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
    po_col = df["Open PO"] if "Open PO" in df.columns else pd.Series(0, index=df.index)
    po = pd.to_numeric(po_col, errors="coerce").fillna(0)
    oh_by_item = oh.groupby(df["Item #"]).sum()      # SKU -> on-hand (summed)

    def wh_qty(skus):
        return int(sum(oh_by_item.get(s, 0) for s in skus))

    dc1_po = int(po[df["Item #"].isin(DC1_SKUS)].sum())
    kids_po = int(po[df["Item #"].isin(KIDS_SKUS)].sum())

    # --- shared warehouse + office combine (same math the Zeni report uses) ---
    office = office_inventory.office_summary()
    office_ok = office is not None
    if not office_ok:
        office = core.empty_office()
    data = core.combine(core.warehouse_breakdown(df), office)

    # --- accessories: warehouse vs office, aligned ---
    acc_rows = []
    for label, val in office["accessories"].items():
        acc_rows.append((label, wh_qty(WH_ACC.get(label, [])), val, label in WH_ACC))
    for label, skus in EXTRA_WH_ACC:
        acc_rows.append((label, wh_qty(skus), None, True))

    # --- trailing ~30 days of flow ---
    end = snap_date - timedelta(days=1)
    start = end - timedelta(days=29)
    flows = daily_flows(start, end)
    ship30 = flows["ship_dc1"] + flows["ship_kids"]
    recv30 = flows["recv_dc1"] + flows["recv_kids"]
    flow_avail = bool(flows["ship_days"] or flows["recv_days"])
    weeks_cover = (data["sellable_units"] / (ship30 / 4.3)) if ship30 > 0 else None

    return {
        **data,
        "snap_date": snap_date, "fname": fname, "payload": payload,
        "office_ok": office_ok, "office": office,
        "dc1_po": dc1_po, "kids_po": kids_po,
        "flows": flows, "ship30": ship30, "recv30": recv30,
        "flow_avail": flow_avail, "weeks_cover": weeks_cover, "window": (start, end),
        "acc_rows": acc_rows,
    }


NAVY = "#1E3A5F"
GREY = "#F4F6F8"


def _row(cells, bold=False, top=None, bg=None):
    style = f"background:{bg};" if bg else ""
    if top:
        style += f"border-top:{top};"
    tds = "".join(
        f"<td align='{a}' style='padding:6px 9px;{'font-weight:bold;' if bold else ''}{c2}'>{v}</td>"
        for v, a, c2 in cells)
    return f"<tr style='{style}'>{tds}</tr>"


def render_html(r):
    nd, nk = r["new"]; od, ok = r["openbox"]; wd, wk = r["warranty"]
    sd, sk = r["sellable"]

    core = "".join([
        f"<tr style='background:{NAVY};color:#fff;'>"
        f"<th align='left' style='padding:6px 9px;'>Category</th>"
        f"<th align='right' style='padding:6px 9px;'>DC-1</th>"
        f"<th align='right' style='padding:6px 9px;'>Kids</th>"
        f"<th align='right' style='padding:6px 9px;'>Total</th>"
        f"<th align='right' style='padding:6px 9px;'>Cost</th>"
        f"<th align='right' style='padding:6px 9px;'>Retail</th></tr>",
        _row([("New <span style='color:#888;font-size:11px;'>· sellable</span>", "left", ""),
              (f"{nd:,}", "right", ""), (f"{nk:,}", "right", ""), (f"{nd+nk:,}", "right", "font-weight:bold;"),
              (usd(r['new_cost']), "right", ""), (usd(r['new_ret']), "right", "")]),
        _row([("Open-box <span style='color:#888;font-size:11px;'>· B-stock</span>", "left", ""),
              (f"{od:,}", "right", ""), (f"{ok:,}", "right", ""), (f"{od+ok:,}", "right", "font-weight:bold;"),
              (usd(r['ob_cost']), "right", ""), (f"<span style='color:#888;'>{usd(r['ob_ret'])}*</span>", "right", "")],
             bg=GREY),
        _row([("Warranty <span style='color:#888;font-size:11px;'>· repair</span>", "left", ""),
              (f"{wd:,}", "right", ""), (f"{wk:,}", "right", ""), (f"{wd+wk:,}", "right", "font-weight:bold;"),
              ("<span style='color:#888;'>memo</span>", "right", ""), ("<span style='color:#888;'>—</span>", "right", "")]),
        _row([("Total assembled", "left", ""), (f"{nd+od+wd:,}", "right", ""), (f"{nk+ok+wk:,}", "right", ""),
              (f"{r['census_units']:,}", "right", ""), ("", "right", ""), ("", "right", "")],
             bold=True, top=f"2px solid {NAVY}"),
        _row([("of which sellable", "left", ""), (f"{sd:,}", "right", ""), (f"{sk:,}", "right", ""),
              (f"{r['sellable_units']:,}", "right", ""), (usd(r['sellable_cost']), "right", ""),
              (usd(r['sellable_ret']), "right", "")], bold=True, bg=GREY),
    ])

    # office warranty queue
    queue = r["office"]["queue"]
    queue_txt = " · ".join(f"{k} {v}" for k, v in
                           sorted(queue.items(), key=lambda kv: -kv[1])) if queue else "—"
    # open-box grades
    g = r["office"]["grades"]
    grades_txt = (f"Office: VIP {g.get('VIP',0)} · Sellable {g.get('Sellable',0)} · "
                  f"Warranty-grade {g.get('Warranty',0)}") if g else ""

    # accessories table
    acc = "".join(
        _row([(label, "left", ""),
              (f"{whq:,}" if whq else "—", "right", ""),
              (f"{offq:,}" if offq is not None else "—", "right", "")],
             bg=(GREY if i % 2 else None))
        for i, (label, whq, offq, _m) in enumerate(r["acc_rows"]))

    s, e = r["window"]
    wc = f"{r['weeks_cover']:.1f} weeks" if r["weeks_cover"] else "n/a"
    flow_line = (f"{r['ship30']:,} shipped · {r['recv30']:,} received "
                 f"(last 30 days, {s} → {e})") if r["flow_avail"] else \
        "No daily shipped/received reports populated in this window yet."
    office_note = "" if r["office_ok"] else \
        ("<p style='color:#B00;font-size:12px;'>⚠ Office (IMS) sheet could not be read — "
         "showing warehouse only. Check the service-account share / credentials.</p>")

    return f"""
    <div style="font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px;color:#222;max-width:680px;">
      <p style="font-size:11px;letter-spacing:2px;color:#888;text-transform:uppercase;margin-bottom:2px;">
        Daylight · Internal Inventory (Warehouse + Office)</p>
      <p style="margin-top:0;"><b>As of {r['snap_date']}</b></p>
      {office_note}

      <p style="font-size:16px;margin:6px 0;">
        <b>{r['census_units']:,} fully-assembled units on hand (DC-1 + Kids)</b>
        &nbsp;·&nbsp; {r['wh_units']:,} warehouse + {r['office_units']:,} office</p>
      <p style="font-size:14px;margin:0 0 10px;color:#333;">
        Sellable: <b>{r['sellable_units']:,}</b> &nbsp;·&nbsp; {usd(r['sellable_cost'])} at cost
        &nbsp;·&nbsp; {usd(r['sellable_ret'])} at retail
        &nbsp;·&nbsp; <span style="color:#777;">{r['warranty_units']:,} warranty (memo)</span></p>

      <table cellpadding="0" cellspacing="0" style="border-collapse:collapse;font-size:13.5px;margin:10px 0;width:100%;">
        {core}
      </table>
      <p style="font-size:11.5px;color:#888;margin:2px 0 16px;">
        *open-box retail at list; B-stock sells at a discount. Warranty units shown as a memo
        (carried at NRV, not full cost). Sellable = new + open-box.</p>

      <p style="font-size:13.5px;margin:14px 0 4px;"><b>Open-box grades</b> ({od+ok:,})</p>
      <p style="font-size:13px;color:#444;margin:0 0 12px;">
        Warehouse: graded returns (4-x). {grades_txt}</p>

      <p style="font-size:13.5px;margin:14px 0 4px;"><b>Office warranty queue</b> ({wd+wk:,} awaiting repair)</p>
      <p style="font-size:13px;color:#444;margin:0 0 12px;">{queue_txt}</p>

      <p style="font-size:13.5px;margin:14px 0 4px;"><b>Accessories &amp; peripherals</b>
        <span style="color:#888;font-size:11px;">(not counted as devices)</span></p>
      <table cellpadding="0" cellspacing="0" style="border-collapse:collapse;font-size:13px;margin:2px 0 12px;width:100%;">
        <tr style="background:{NAVY};color:#fff;"><th align="left" style="padding:5px 9px;">Item</th>
          <th align="right" style="padding:5px 9px;">Warehouse</th><th align="right" style="padding:5px 9px;">Office</th></tr>
        {acc}
      </table>

      <p style="font-size:13.5px;margin:14px 0 4px;"><b>Flow &amp; cover</b></p>
      <p style="font-size:13px;color:#444;margin:0;">
        Inbound (open POs): {r['dc1_po']:,} DC-1 · {r['kids_po']:,} Kids on order.<br>
        {flow_line}<br>
        Weeks of cover: {wc} (sellable ÷ weekly ship rate).</p>

      <p style="font-size:12px;color:#777;margin-top:16px;">
        Cost = production cost (${DC1_COST:,.0f} DC-1 · Kids ${KIDS_COST:,.0f}, bundle).
        Retail = list (${DC1_RETAIL:,.0f} DC-1 / ${KIDS_RETAIL:,.0f} Kids).
        Warehouse = DCL "Items Status" {r['snap_date']} (attached); Office = Daylight IMS sheet (live).</p>
      <p style="font-size:12px;color:#777;">— Automated monthly internal inventory report.</p>
    </div>
    """


def send(r):
    html = render_html(r)
    to_list = [a.strip() for a in INTERNAL_RECIPIENTS.split(",") if a.strip()]
    if not to_list:
        raise SystemExit("No INTERNAL_RECIPIENTS / REPORT_RECIPIENT set.")
    msg = MIMEMultipart("mixed")
    msg["Subject"] = (f"Inventory (internal) — {r['census_units']:,} units "
                      f"({r['wh_units']:,} wh / {r['office_units']:,} office), as of {r['snap_date']}")
    msg["From"] = SMTP_USERNAME
    msg["To"] = ", ".join(to_list)
    alt = MIMEMultipart("alternative")
    nd, nk = r["new"]
    alt.attach(MIMEText(
        f"Internal inventory as of {r['snap_date']}: {r['census_units']:,} fully-assembled units "
        f"({r['wh_units']:,} warehouse + {r['office_units']:,} office); {r['sellable_units']:,} sellable, "
        f"{usd(r['sellable_cost'])} at cost.", "plain"))
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
    print(f"Sent internal report to {to_list}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--monthly", action="store_true",
                    help="monthly cadence: only send on the day the first new-month "
                         "snapshot arrives (so a daily 1st-7th cron fires once)")
    ap.add_argument("--html-out", metavar="PATH", help="write rendered HTML to a file (debug)")
    args = ap.parse_args()
    r = build_report()

    if args.monthly and not args.dry_run:
        today = date.today()
        if today.day > 7 or r["snap_date"] != today:
            print(f"--monthly: snapshot {r['snap_date']} != today {today} (or past day 7) "
                  f"- exiting quietly.")
            return
    nd, nk = r["new"]
    print(f"As of {r['snap_date']}: census {r['census_units']} "
          f"(wh {r['wh_units']} / office {r['office_units']}), sellable {r['sellable_units']} "
          f"= {usd(r['sellable_cost'])} cost / {usd(r['sellable_ret'])} retail; "
          f"warranty {r['warranty_units']} memo; office_ok={r['office_ok']}")
    if args.html_out:
        with open(args.html_out, "w") as f:
            f.write(render_html(r))
        print(f"Wrote HTML to {args.html_out}")
    if args.dry_run:
        print("Dry run — not sending."); return
    send(r)


if __name__ == "__main__":
    main()
