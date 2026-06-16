"""
Compose the single unified weekly email (Sales + Spend + Inventory) and send it.

Design notes
------------
Gmail is our target client. That means:
  - Inline styles only (Gmail strips <style> blocks in some contexts).
  - Table-based layout (flexbox / grid don't render reliably).
  - System font stack (no web fonts — they silently fall back anyway).
  - Max content width 720 px (Gmail clips wider on narrow viewports).
  - Images embedded via CID references (Gmail displays these inline; data:
    URIs get blocked on many clients).

The LLM-generated HTML bodies from each bot are dropped into their own section
container. Each already contains inline styles for table highlighting, so they
render consistently alongside our wrapper chrome.
"""
from __future__ import annotations

import os
import smtplib
import re
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage


NAVY = "#1E3A5F"
SOFT_BG = "#F8F9FA"
BORDER = "#E5E7EB"
TEXT = "#222222"
MUTED = "#6B7280"
GOOD = "#1A7A42"
WARN = "#B7600E"
CRIT = "#A93326"


def _fmt_currency(val, include_cents: bool = False) -> str:
    if val is None:
        return "—"
    try:
        v = float(val)
    except (TypeError, ValueError):
        return str(val)
    if include_cents:
        return f"${v:,.2f}"
    return f"${v:,.0f}"


def _fmt_pct(curr, prev) -> tuple[str, str]:
    """Returns (text, color) for a WoW change."""
    if not prev or prev == 0:
        return "—", MUTED
    pct = ((curr - prev) / prev) * 100
    sign = "+" if pct >= 0 else ""
    color = GOOD if pct >= 0 else CRIT
    return f"{sign}{pct:.1f}% WoW", color


def _kpi_card(label: str, value: str, sub: str = "", sub_color: str = MUTED) -> str:
    return f"""
    <td width="33%" valign="top" style="padding:6px;">
      <table width="100%" cellpadding="0" cellspacing="0" border="0"
             style="background:#FFFFFF; border:1px solid {BORDER}; border-radius:6px;">
        <tr><td style="padding:16px 14px; text-align:center;">
          <div style="font-size:10.5px; letter-spacing:1px; color:{MUTED};
                      text-transform:uppercase; font-weight:600; margin-bottom:6px;">
            {label}
          </div>
          <div style="font-size:22px; font-weight:700; color:{NAVY}; line-height:1.15;">
            {value}
          </div>
          {f'<div style="font-size:11.5px; color:{sub_color}; margin-top:4px; font-weight:500;">{sub}</div>' if sub else ''}
        </td></tr>
      </table>
    </td>
    """


def _section_header(emoji: str, title: str, subtitle: str = "") -> str:
    return f"""
    <tr><td style="padding: 28px 30px 14px 30px; background:#FFFFFF;">
      <table width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
          <td style="border-bottom:2px solid {NAVY}; padding-bottom:8px;">
            <span style="font-size:18px; margin-right:8px;">{emoji}</span>
            <span style="font-size:18px; font-weight:700; color:{NAVY};
                         letter-spacing:0.3px; text-transform:uppercase;">{title}</span>
            {f'<span style="font-size:12px; color:{MUTED}; font-weight:500; margin-left:10px;">{subtitle}</span>' if subtitle else ''}
          </td>
        </tr>
      </table>
    </td></tr>
    """


def _sanitize_html_body(html: str) -> str:
    """
    Strip markdown fences and normalize the LLM HTML so it embeds nicely
    inside our section container.
    """
    h = html.strip()
    if h.startswith("```html"):
        h = h[7:]
    elif h.startswith("```"):
        h = h[3:]
    if h.endswith("```"):
        h = h[:-3]
    # Convert **bold** to <b>
    h = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", h, flags=re.DOTALL)
    return h.strip()


def _wrap_report_section(inner_html: str) -> str:
    """
    Wrap LLM HTML in a row with Gmail-friendly default table styles so
    any <table> inside inherits sensible borders + spacing even if the
    LLM forgot.
    """
    # Inject a <style>-like inline look by targeting the wrapping <div>.
    # Gmail supports inline style on <td>, <table>, <tr>, but rules within
    # a <style> tag are respected inside Gmail's web client too when placed
    # in <head>. We do both for maximum compatibility.
    return f"""
    <tr><td style="padding:6px 30px 24px 30px; background:#FFFFFF;
                   font-size:13.5px; color:{TEXT}; line-height:1.55;">
      <div class="report-body">
        {inner_html}
      </div>
    </td></tr>
    """


HEAD_STYLE = f"""
<style type="text/css">
  /* Gmail respects these inside <head>; other clients will too. */
  .report-body h3 {{
    color: {NAVY}; font-size: 14px; margin: 22px 0 10px; padding: 0;
    font-weight: 700; letter-spacing: 0.2px;
  }}
  .report-body p  {{ margin: 0 0 10px; }}
  .report-body ul {{ margin: 6px 0 14px; padding-left: 22px; }}
  .report-body li {{ margin: 4px 0; }}
  .report-body table {{
    border-collapse: collapse; width: 100%; margin: 8px 0 18px;
    font-size: 12.5px;
  }}
  .report-body th {{
    background: {NAVY}; color: #FFFFFF; text-align: left;
    padding: 8px 10px; font-weight: 600; font-size: 11.5px;
    text-transform: uppercase; letter-spacing: 0.5px;
  }}
  .report-body td {{
    padding: 7px 10px; border-bottom: 1px solid {BORDER};
    vertical-align: top;
  }}
  .report-body tr:nth-child(even) td {{ background: #FBFCFD; }}
  .report-body b, .report-body strong {{ color: {NAVY}; }}
</style>
"""


def compose_weekly_email(
    *,
    week_monday: str,
    sales: dict,
    spend: dict,
    inventory: dict,
) -> tuple[str, list]:
    """
    Build the HTML body.

    Each section dict expects:
      - 'html' (str)                — LLM-generated report HTML
      - 'headline' (dict, optional) — headline metrics for the top KPI strip

    Returns
    -------
    (html_body, inline_images)
      inline_images is always an empty list (kept in the signature so the
      send_unified_email plumbing stays unchanged if we re-introduce inline
      images later that actually earn their space).
    """
    generated_at = datetime.now().strftime("%B %d, %Y · %I:%M %p")

    # --- Exec snapshot cards --------------------------------------
    sales_h = sales.get("headline") or {}
    spend_h = spend.get("headline") or {}
    inv_h = inventory.get("headline") or {}

    rev_value = _fmt_currency(sales_h.get("gross_sales_dc1"))
    rev_sub, rev_col = _fmt_pct(
        sales_h.get("gross_sales_dc1"),
        sales_h.get("prev_gross_sales_dc1"),
    )

    spend_value = _fmt_currency(spend_h.get("total_spend"), include_cents=True)
    spend_sub, spend_col = _fmt_pct(
        spend_h.get("total_spend"),
        spend_h.get("prev_week_spend"),
    )
    # For spend, "increase" is BAD — flip the colour
    if spend_h.get("prev_week_spend") and spend_h.get("total_spend") is not None:
        spend_col = CRIT if spend_h["total_spend"] > spend_h["prev_week_spend"] else GOOD

    runway = spend_h.get("runway") or {}
    if runway.get("weeks_remaining"):
        runway_val = f"{runway['weeks_remaining']:.0f} wks"
        runway_sub = f"runs out {runway.get('runout_date', '—')}"
        runway_col = (
            CRIT if runway["weeks_remaining"] < 26
            else WARN if runway["weeks_remaining"] < 52
            else GOOD
        )
    else:
        runway_val = "—"
        runway_sub = "set CASH_BALANCE_USD"
        runway_col = MUTED

    critical_reorders = inv_h.get("critical_count", 0)
    reorder_sub = inv_h.get("critical_sub", "—")
    reorder_col = CRIT if critical_reorders > 0 else GOOD

    kpi_row_1 = f"""
    <tr>
      {_kpi_card("Revenue (DC-1)", rev_value, rev_sub, rev_col)}
      {_kpi_card("Total Spend", spend_value, spend_sub, spend_col)}
      {_kpi_card("Cash Runway", runway_val, runway_sub, runway_col)}
    </tr>
    """
    kpi_row_2 = f"""
    <tr>
      {_kpi_card("Orders", str(sales_h.get("order_count", "—")),
                 f"AOV {_fmt_currency(sales_h.get('aov'))}", MUTED)}
      {_kpi_card("Kids Revenue",
                 _fmt_currency(sales_h.get("kids_rev")),
                 f"{sales_h.get('kids_pct', '—')} of total", MUTED)}
      {_kpi_card("Reorders Due Now", str(critical_reorders),
                 reorder_sub, reorder_col)}
    </tr>
    """

    # --- Assemble final HTML --------------------------------------
    sales_html = _sanitize_html_body(sales.get("html", ""))
    spend_html = _sanitize_html_body(spend.get("html", ""))
    inv_html = _sanitize_html_body(inventory.get("html", ""))

    attachments_summary = sales.get("attachment_names", []) + \
                          spend.get("attachment_names", []) + \
                          inventory.get("attachment_names", [])

    attachments_list_html = "".join(
        f'<li style="margin:3px 0;">{name}</li>' for name in attachments_summary
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Daylight Weekly Report</title>
  {HEAD_STYLE}
</head>
<body style="margin:0; padding:0; background:#EEF1F5;
             font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI',
                          Helvetica, Arial, sans-serif;">
<!-- Outer wrapper -->
<table width="100%" cellpadding="0" cellspacing="0" border="0"
       style="background:#EEF1F5;">
  <tr><td align="center" style="padding:24px 12px;">

    <!-- Main container -->
    <table width="720" cellpadding="0" cellspacing="0" border="0"
           style="max-width:720px; background:#FFFFFF;
                  border-radius:10px; overflow:hidden;
                  box-shadow:0 2px 8px rgba(0,0,0,0.06);">

      <!-- Header band -->
      <tr><td style="background:{NAVY}; padding:28px 30px; text-align:center;">
        <div style="font-size:11px; letter-spacing:3px; color:#9FB4CC;
                    text-transform:uppercase; font-weight:600;">
          Daylight Computer · Weekly Ops Report
        </div>
        <div style="font-size:24px; font-weight:700; color:#FFFFFF;
                    margin-top:6px; letter-spacing:0.3px;">
          Week of {week_monday}
        </div>
        <div style="font-size:11.5px; color:#9FB4CC; margin-top:4px;">
          Sent {generated_at}
        </div>
      </td></tr>

      <!-- Executive snapshot -->
      <tr><td style="padding:22px 24px 10px 24px; background:{SOFT_BG};
                     border-bottom:1px solid {BORDER};">
        <div style="font-size:11px; letter-spacing:1.5px; color:{MUTED};
                    text-transform:uppercase; font-weight:600;
                    text-align:center; margin-bottom:14px;">
          Executive Snapshot
        </div>
        <table width="100%" cellpadding="0" cellspacing="0" border="0">
          {kpi_row_1}
          {kpi_row_2}
        </table>
      </td></tr>

      <!-- Sales -->
      {_section_header("📈", "Sales", f"DC-1 · Week of {week_monday}")}
      {_wrap_report_section(sales_html)}

      <!-- Spend -->
      {_section_header("💰", "Spend", "Brex · Mercury · Rippling")}
      {_wrap_report_section(spend_html)}

      <!-- Inventory -->
      {_section_header("📦", "Inventory", "Stockout ETA · Reorder Queue")}
      {_wrap_report_section(inv_html)}

      <!-- Attachments footer -->
      <tr><td style="background:{SOFT_BG}; padding:20px 30px;
                     border-top:1px solid {BORDER}; font-size:12px;
                     color:{MUTED};">
        <b style="color:{NAVY}; font-size:12.5px;">Attached files</b>
        <ul style="margin:8px 0 0; padding-left:20px;">
          {attachments_list_html}
        </ul>
      </td></tr>

      <!-- Closing footer -->
      <tr><td style="background:{NAVY}; padding:14px 30px; text-align:center;
                     font-size:11px; color:#9FB4CC;">
        Automated by weekly-automation ·
        <a href="https://github.com/JVogelRSA/TristanAutomation"
           style="color:#CFD8E3; text-decoration:underline;">source</a>
      </td></tr>

    </table>

  </td></tr>
</table>
</body>
</html>"""

    return html, []


def send_unified_email(
    *,
    subject: str,
    html_body: str,
    plain_fallback: str,
    recipient: str,
    file_attachments: list[tuple[str, bytes]],
    inline_images: list[tuple[str, bytes, str]],
) -> bool:
    """
    Send one email with:
      - multipart/mixed root
        - multipart/related
          - multipart/alternative (text/plain + text/html)
          - inline image parts (referenced by CID)
        - file attachment parts

    Returns True on success. Returns False for config problems; raises on
    SMTP failures so the caller's exit code reflects the delivery failure.
    """
    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", 587))
    smtp_username = os.getenv("SMTP_USERNAME")
    smtp_password = os.getenv("SMTP_PASSWORD")

    if not smtp_username or not smtp_password:
        print("Error: SMTP credentials not configured.")
        return False
    if not recipient:
        print("Error: No recipient specified.")
        return False

    # Root: mixed so file attachments sit alongside the HTML+inline bundle
    root = MIMEMultipart("mixed")
    root["Subject"] = subject
    root["From"] = smtp_username
    root["To"] = recipient

    # Related: HTML body + inline images
    related = MIMEMultipart("related")
    root.attach(related)

    alternative = MIMEMultipart("alternative")
    related.attach(alternative)
    alternative.attach(MIMEText(plain_fallback, "plain"))
    alternative.attach(MIMEText(html_body, "html"))

    for cid, data, subtype in inline_images:
        img = MIMEImage(data, _subtype=subtype)
        img.add_header("Content-ID", f"<{cid}>")
        img.add_header("Content-Disposition", "inline", filename=f"{cid}.{subtype}")
        related.attach(img)

    for filename, data in file_attachments:
        part = MIMEApplication(data, Name=filename)
        part["Content-Disposition"] = f'attachment; filename="{filename}"'
        root.attach(part)

    try:
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=60) as s:
                s.login(smtp_username, smtp_password)
                s.send_message(root)
        else:
            with smtplib.SMTP(smtp_server, smtp_port, timeout=60) as s:
                s.starttls()
                s.login(smtp_username, smtp_password)
                s.send_message(root)
        print(f"Unified weekly report sent to {recipient}.")
        return True
    except Exception as e:
        print(f"Error sending unified email: {e}")
        raise
