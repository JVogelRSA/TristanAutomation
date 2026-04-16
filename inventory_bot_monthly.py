"""
Monthly Inventory Analysis Bot
Runs on the last day of each month. Fetches up to 8 weeks of inventory CSV emails,
groups them into the current and previous calendar months, and emails a professional
DOCX report with month-over-month burn comparison charts.
"""
import os
import io
import calendar
from datetime import datetime, timedelta, date

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from openai import OpenAI
from imap_tools import MailBox, AND
from dotenv import load_dotenv

from utils.email_sender import send_report_email
from utils.docx_generator import html_to_docx
from utils.history import (
    get_month_key, save_monthly_snapshot, load_monthly_history,
    build_monthly_inventory_comparison,
)

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'), override=True)

IMAP_SERVER          = os.getenv("IMAP_SERVER", "imap.gmail.com")
IMAP_USERNAME        = os.getenv("IMAP_USERNAME")
IMAP_PASSWORD        = os.getenv("IMAP_PASSWORD")
REPORT_RECIPIENT     = os.getenv("REPORT_RECIPIENT")
OPENAI_API_KEY       = os.getenv("OPENAI_API_KEY")
EMAIL_SUBJECT_KEYWORD = os.getenv("EMAIL_SUBJECT_KEYWORD", "Inventory")
EMAIL_SENDER         = os.getenv("EMAIL_SENDER")

# ── SKU configuration (shared with weekly bot) ─────────────────
SKU_MAP = {
    '303': 'Winter Solstice',
    '36':  'Small Incandescent Light Bulb (T45 6 pack)',
    '1':   'Daylight DC-1 (Regular)',
    '401': 'Daylight Kids Bundle (Ages 8-14)',
    '400': 'Daylight Kids Bundle (Ages 3-7)',
    '28':  'Daylight Comfy Sleeve',
    '29':  'LAMY Pen',
    '31':  'Daylight Kids Case',
    '37':  'Large Incandescent Light Bulb (ST64 4 pack)',
    '302': 'Keyboard Case Bundle',
    '32':  'Daylight Keyboard Case',
    '35':  'Daylight Keyboard',
    '301': 'Amber Sunday Bundle (2025)',
    '23':  'Daylight Sling',
    '34':  'Daylight Stand',
    '3-36':'Small Incandescent Light Bulb For Friends (T45)',
    '3-37':'Large Incandescent Light Bulb For Friends (ST64)',
}

TOP_PRIORITY_SKUS = ['1', '401', '400', '28', '29', '31', '36', '37']

EXCLUDED_SKUS = ['3-36', '3-37', '34-', '34-1', '35-', '36-', '37-', '4-3-1', '7']


def get_month_windows():
    """
    Return (curr_start, curr_end, prev_start, prev_end) as date objects
    for the current and previous calendar months.
    """
    today = date.today()
    curr_start = today.replace(day=1)
    curr_last  = calendar.monthrange(today.year, today.month)[1]
    curr_end   = today.replace(day=curr_last)

    if today.month == 1:
        prev_start = today.replace(year=today.year - 1, month=12, day=1)
    else:
        prev_start = today.replace(month=today.month - 1, day=1)
    prev_last = calendar.monthrange(prev_start.year, prev_start.month)[1]
    prev_end  = prev_start.replace(day=prev_last)

    return curr_start, curr_end, prev_start, prev_end


def fetch_latest_emails(limit=8):
    """
    Fetch up to `limit` most recent inventory CSV emails, sorted oldest-first.
    Returns list of (date, DataFrame) tuples.
    """
    print(f"Connecting to {IMAP_SERVER}...")
    inventory_data = []

    try:
        with MailBox(IMAP_SERVER).login(IMAP_USERNAME, IMAP_PASSWORD) as mailbox:
            criteria = AND(subject=EMAIL_SUBJECT_KEYWORD)
            if EMAIL_SENDER:
                criteria = AND(subject=EMAIL_SUBJECT_KEYWORD, from_=EMAIL_SENDER)

            for msg in mailbox.fetch(criteria, limit=limit * 2, reverse=True):
                if len(inventory_data) >= limit:
                    break
                for att in msg.attachments:
                    if att.filename.lower().endswith('.csv'):
                        print(f"  Found: {att.filename} ({msg.date.strftime('%Y-%m-%d')})")
                        try:
                            df = pd.read_csv(io.BytesIO(att.payload))
                            inventory_data.append((msg.date, df))
                        except Exception as e:
                            print(f"  Error reading CSV: {e}")
                        break
    except Exception as e:
        print(f"Error fetching emails: {e}")
        return []

    inventory_data.sort(key=lambda x: x[0])
    return inventory_data


def compute_monthly_burns(dfs_data):
    """
    Compute total units consumed in each calendar month across all email snapshots.
    Returns a dict: {month_str: {sku: total_consumed}}
    """
    if len(dfs_data) < 2:
        return {}

    # Detect item and qty columns
    _, sample_df = dfs_data[0]
    item_col = next((c for c in sample_df.columns if 'item' in c.lower() or 'sku' in c.lower()), sample_df.columns[0])
    qty_col  = next((c for c in sample_df.columns if 'hand' in c.lower() or 'qty' in c.lower() or 'available' in c.lower()), sample_df.columns[-1])

    # Build per-week quantity lookups
    lookups = []
    for msg_date, df in dfs_data:
        df[item_col] = df[item_col].astype(str).str.strip()
        df[qty_col]  = pd.to_numeric(df[qty_col], errors='coerce').fillna(0)
        lookups.append((msg_date.date(), dict(zip(df[item_col], df[qty_col]))))

    # Accumulate drops between consecutive snapshots, keyed by the earlier snapshot's month
    monthly_burns = {}  # {YYYY-MM: {sku: consumed}}
    for i in range(1, len(lookups)):
        prev_date, prev_qty = lookups[i - 1]
        this_date, this_qty = lookups[i]
        month_key = prev_date.strftime('%Y-%m')
        if month_key not in monthly_burns:
            monthly_burns[month_key] = {}
        all_skus = set(prev_qty) | set(this_qty)
        for sku in all_skus:
            if sku in EXCLUDED_SKUS:
                continue
            drop = prev_qty.get(sku, 0) - this_qty.get(sku, 0)
            if drop > 0:
                monthly_burns[month_key][sku] = monthly_burns[month_key].get(sku, 0) + drop

    return monthly_burns, item_col, qty_col


def generate_mom_chart(curr_burns, prev_burns, curr_label, prev_label):
    """
    Grouped horizontal bar chart comparing monthly units consumed
    between the current and previous month.
    Returns PNG bytes or None.
    """
    all_skus = set(curr_burns) | set(prev_burns)
    # Only chart SKUs with any burn
    data = []
    for sku in all_skus:
        curr_val = curr_burns.get(sku, 0)
        prev_val = prev_burns.get(sku, 0)
        if curr_val > 0 or prev_val > 0:
            data.append({
                'product': SKU_MAP.get(sku, sku),
                'curr': curr_val,
                'prev': prev_val,
            })

    if not data:
        return None

    data.sort(key=lambda x: x['curr'] + x['prev'])

    products  = [d['product'] for d in data]
    curr_vals = [d['curr'] for d in data]
    prev_vals = [d['prev'] for d in data]

    y     = np.arange(len(products))
    h     = 0.36
    fig_h = max(3.5, len(products) * 0.60)
    fig, ax = plt.subplots(figsize=(8.5, fig_h))
    fig.patch.set_facecolor('#FAFAFA')
    ax.set_facecolor('#FAFAFA')

    b1 = ax.barh(y + h / 2, curr_vals, h, color='#1E3A5F', label=curr_label, edgecolor='white')
    b2 = ax.barh(y - h / 2, prev_vals, h, color='#7FB3D3', label=prev_label, edgecolor='white')

    for bar, val in zip(b1, curr_vals):
        if val > 0:
            ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height() / 2,
                    str(int(val)), va='center', ha='left', fontsize=8, color='#1E3A5F', fontweight='bold')
    for bar, val in zip(b2, prev_vals):
        if val > 0:
            ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height() / 2,
                    str(int(val)), va='center', ha='left', fontsize=8, color='#4A7FA5')

    ax.set_yticks(y)
    ax.set_yticklabels(products, fontsize=8.5)
    ax.set_xlabel('Units Consumed', fontsize=9, color='#444')
    ax.set_title('Monthly Units Consumed — Month-over-Month', fontsize=12,
                 fontweight='bold', color='#1E3A5F', pad=12)
    ax.legend(fontsize=8.5, loc='lower right', framealpha=0.8)
    for spine in ('top', 'right'):
        ax.spines[spine].set_visible(False)
    ax.spines['left'].set_color('#CCC')
    ax.spines['bottom'].set_color('#CCC')
    ax.tick_params(axis='both', labelsize=8.5, colors='#444')
    ax.xaxis.grid(True, linestyle=':', linewidth=0.6, alpha=0.6)
    ax.set_axisbelow(True)

    plt.tight_layout(pad=1.2)
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight', facecolor='#FAFAFA')
    plt.close(fig)
    return buf.getvalue()


def generate_runway_chart(summary_df):
    """Horizontal bar chart of current stock runway per product."""
    def parse_runway(s):
        try:
            return float(str(s).replace(' weeks', '').strip())
        except (ValueError, TypeError):
            return None

    df = summary_df[summary_df['Monthly Burn'] > 0].copy()
    df['Runway_Float'] = df['Runway (Est)'].apply(parse_runway)
    df = df.dropna(subset=['Runway_Float']).sort_values('Runway_Float')

    if df.empty:
        return None

    DISPLAY_CAP = 30
    df['Runway_Display'] = df['Runway_Float'].clip(upper=DISPLAY_CAP)
    colours = ['#C0392B' if r < 4 else '#E67E22' if r < 8 else '#27AE60' for r in df['Runway_Float']]

    fig, ax = plt.subplots(figsize=(8, max(3.0, len(df) * 0.48)))
    fig.patch.set_facecolor('#FAFAFA')
    ax.set_facecolor('#FAFAFA')

    bars = ax.barh(df['Product'], df['Runway_Display'], color=colours, edgecolor='white', height=0.58)
    for bar, val in zip(bars, df['Runway_Float']):
        label = f"{val:.1f} wks" if val <= DISPLAY_CAP else f"{val:.0f}+ wks"
        ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height() / 2,
                label, va='center', ha='left', fontsize=8.5, color='#333')

    ax.axvline(4, color='#C0392B', linestyle='--', linewidth=1.1, alpha=0.75, label='4-week Critical Zone')
    ax.axvline(8, color='#E67E22', linestyle='--', linewidth=1.1, alpha=0.75, label='8-week Caution Zone')
    ax.set_xlabel('Estimated Stock Runway (weeks)', fontsize=9, color='#444')
    ax.set_title('Current Stock Runway by Product', fontsize=12, fontweight='bold',
                 color='#1E3A5F', pad=12)
    ax.legend(fontsize=8, loc='lower right', framealpha=0.7)
    ax.set_xlim(0, max(DISPLAY_CAP + 2, df['Runway_Display'].max() + 3))
    for spine in ('top', 'right'):
        ax.spines[spine].set_visible(False)
    ax.spines['left'].set_color('#CCC')
    ax.spines['bottom'].set_color('#CCC')
    ax.tick_params(axis='both', labelsize=8.5, colors='#444')
    ax.xaxis.grid(True, linestyle=':', linewidth=0.6, alpha=0.6)
    ax.set_axisbelow(True)

    plt.tight_layout(pad=1.2)
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight', facecolor='#FAFAFA')
    plt.close(fig)
    return buf.getvalue()


def generate_llm_report(dfs_data, history=None):
    """
    Compute monthly burn rates and generate the LLM report.
    Returns (report_html, summary_df, curr_burns, prev_burns, snapshot).
    """
    print("Analysing monthly inventory data...")

    if len(dfs_data) < 2:
        print("Not enough email snapshots for monthly analysis.")
        return "<p>Error: not enough data.</p>", pd.DataFrame(), {}, {}, {}

    result = compute_monthly_burns(dfs_data)
    if not result or not result[0]:
        print("Could not compute burns.")
        return "<p>Error computing burn data.</p>", pd.DataFrame(), {}, {}, {}

    monthly_burns, item_col, qty_col = result

    curr_start, curr_end, prev_start, prev_end = get_month_windows()
    curr_month = curr_start.strftime('%Y-%m')
    prev_month = prev_start.strftime('%Y-%m')

    curr_burns = monthly_burns.get(curr_month, {})
    prev_burns = monthly_burns.get(prev_month, {})

    # If the current month key isn't present, use the latest month key available
    if not curr_burns and monthly_burns:
        latest_key = sorted(monthly_burns)[-1]
        curr_burns = monthly_burns[latest_key]
        curr_month = latest_key
    if not prev_burns and len(monthly_burns) >= 2:
        keys = sorted(monthly_burns)
        idx  = keys.index(curr_month) - 1 if curr_month in keys else -2
        prev_burns = monthly_burns[keys[idx]]
        prev_month = keys[idx]

    # Build summary from the most recent snapshot
    _, latest_df = dfs_data[-1]
    latest_df[item_col] = latest_df[item_col].astype(str).str.strip()
    latest_df[qty_col]  = pd.to_numeric(latest_df[qty_col], errors='coerce').fillna(0)
    curr_stock = dict(zip(latest_df[item_col], latest_df[qty_col]))

    all_skus = set(SKU_MAP) | set(curr_burns) | set(prev_burns)
    rows = []
    for sku in all_skus:
        if sku in EXCLUDED_SKUS:
            continue
        stock        = curr_stock.get(sku, 0)
        curr_b       = curr_burns.get(sku, 0)
        prev_b       = prev_burns.get(sku, 0)
        # Runway in weeks based on monthly burn ÷ 4
        wkly_equiv   = curr_b / 4.0
        runway       = (stock / wkly_equiv) if wkly_equiv > 0 else float('inf')
        runway_str   = f"{runway:.1f} weeks" if runway != float('inf') else "N/A"
        mom_pct      = ((curr_b - prev_b) / prev_b * 100) if prev_b > 0 else None
        mom_str      = f"{'+' if mom_pct >= 0 else ''}{mom_pct:.0f}%" if mom_pct is not None else "N/A"
        rows.append({
            'SKU':          sku,
            'Product':      SKU_MAP.get(sku, sku),
            'Priority':     'Yes' if sku in TOP_PRIORITY_SKUS else 'No',
            'Current Stock':int(stock),
            'Monthly Burn': round(curr_b, 1),
            'Prior Month':  round(prev_b, 1),
            'MoM Change':   mom_str,
            'Runway (Est)': runway_str,
        })

    summary_df = pd.DataFrame(rows).sort_values('Monthly Burn', ascending=False)

    # Build per-SKU dict for history
    current_skus = {
        r['SKU']: {
            'product':      r['Product'],
            'stock':        r['Current Stock'],
            'monthly_burn': r['Monthly Burn'],
        }
        for _, r in summary_df.iterrows() if r['Monthly Burn'] > 0
    }

    hist_comparison = build_monthly_inventory_comparison(history or [], current_skus)

    curr_label = curr_start.strftime('%B %Y')
    prev_label = prev_start.strftime('%B %Y')
    n_snapshots = len(dfs_data)

    active = summary_df[(summary_df['Monthly Burn'] > 0) | (summary_df['SKU'].isin(TOP_PRIORITY_SKUS))]

    prompt_data = f"""
Report: {curr_label} (compared to {prev_label})
Based on {n_snapshots} weekly snapshots covering ~{n_snapshots} weeks.

{active.to_string(index=False)}

{hist_comparison}
"""

    client = OpenAI(api_key=OPENAI_API_KEY)

    prompt = f"""
You are a Senior Supply Chain Analyst at a high-growth hardware company.
Analyse the monthly inventory data below and produce a Monthly Inventory Review for the Executive Team.

DATA SUMMARY:
{prompt_data}

--------------------------------------------------

Output ONLY valid HTML — no markdown, no code fences, no ** syntax, no intro or sign-off.
Start directly with the first <h3> tag.

REQUIRED SECTIONS:

<h3>1. Monthly Actions Required — {curr_label}</h3>
3–5 specific, urgent bullet points (<ul><li>…</li></ul>).
Name products, quantities, and exact actions (place PO, contact supplier, etc.).
Any Top Priority item with runway < 8 weeks must appear here.

<h3>2. Month-at-a-Glance Summary</h3>
A compact 2-column HTML table (Metric | Value) with:
  - Total units consumed across all SKUs this month
  - Most consumed product (and quantity)
  - Biggest MoM increase in burn rate
  - Biggest MoM decrease in burn rate
  - Number of SKUs with zero stock
  - Number of SKUs in critical zone (< 4 weeks runway)
Then a short <p><b>Supply Chain Commentary:</b> …</p> (2–3 sentences) on the month's overall inventory health.

<h3>3. Top Priority Items — Monthly Performance</h3>
Table for Top Priority SKUs only: SKU | Product | Current Stock | Monthly Burn ({curr_label}) | Monthly Burn ({prev_label}) | MoM Change | Runway | Status.
Status values: "CRITICAL – reorder now" (runway < 4 wks), "Low – prepare PO" (4–8 wks), "Healthy" (> 8 wks).
Apply inline style background-color:#FFE0DC on CRITICAL rows.
Below the table, write one sentence per CRITICAL item with specific reorder guidance.

<h3>4. Month-over-Month Burn Analysis</h3>
Table showing all active SKUs: Product | {curr_label} Burn | {prev_label} Burn | MoM Change | Driver (your interpretation).
Sorted by absolute MoM change descending.
Add a 2–3 sentence paragraph below the table interpreting the biggest movers.

<h3>5. Restock Planning for Next Month</h3>
Based on current burn rates, provide a procurement recommendation table:
Product | Current Stock | Monthly Burn Rate | Weeks of Runway | Recommended Reorder Qty | Priority.
Recommended Reorder Qty = target 12 weeks of stock minus current stock (floor at 0).
Apply inline style background-color:#FFE0DC on rows with Priority = HIGH, background-color:#FFF3CC on MEDIUM.

<h3>6. Full Inventory Data Table</h3>
All SKUs: SKU | Product | Current Stock | Monthly Burn | Prior Month | MoM % | Runway.
Sorted by Monthly Burn descending.
Apply inline row colours: background-color:#FFE0DC for runway < 4 wks, #FFF3CC for 4–8 wks, #D5F5E3 for > 8 wks with burn > 0.

<p><i>Methodology: Monthly Burn = total stock depletion across all weekly snapshots within the calendar month. Runway is based on current stock divided by the equivalent weekly burn rate (monthly burn ÷ 4).</i></p>

FORMAT RULES:
- Valid HTML only. No markdown. Use <b> tags.
- Tables must have <thead><tr><th> headers.
- Keep it scannable — executives will read this in under 3 minutes.
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a direct, data-driven supply chain analyst. Every recommendation must cite actual numbers."},
                {"role": "user", "content": prompt},
            ],
        )
        report_html = response.choices[0].message.content
        snapshot = {"skus": current_skus}
        return report_html, summary_df, curr_burns, prev_burns, snapshot
    except Exception as e:
        print(f"Error generating LLM report: {e}")
        return "<p>Error generating report.</p>", summary_df, {}, {}, {}


def main():
    if not all([IMAP_USERNAME, IMAP_PASSWORD, OPENAI_API_KEY, REPORT_RECIPIENT]):
        print("Error: Missing environment variables. Check .env file.")
        return

    print("Starting Monthly Inventory Analysis Bot...")

    dfs_data = fetch_latest_emails(limit=8)
    if len(dfs_data) < 2:
        print("Not enough inventory emails found (need at least 2).")
        return

    print(f"Found {len(dfs_data)} weekly snapshots covering approximately {len(dfs_data)} weeks.")

    history    = load_monthly_history("inventory_monthly")
    report_html, summary_df, curr_burns, prev_burns, snapshot = generate_llm_report(dfs_data, history=history)

    month_key = get_month_key()
    if snapshot:
        save_monthly_snapshot("inventory_monthly", month_key, snapshot)

    curr_start, _, prev_start, _ = get_month_windows()
    curr_label  = curr_start.strftime('%B %Y')
    prev_label  = prev_start.strftime('%B %Y')
    date_str    = curr_start.strftime('%Y-%m')

    print("\n--- REPORT PREVIEW (first 500 chars) ---")
    print(report_html[:500])
    print("...\n")

    # Charts
    mom_chart     = generate_mom_chart(curr_burns, prev_burns, curr_label, prev_label)
    runway_chart  = generate_runway_chart(summary_df)
    chart_images  = [c for c in [mom_chart, runway_chart] if c is not None]

    docx_bytes = html_to_docx(
        report_html, f"Monthly Inventory Review — {curr_label}",
        date_str=date_str, chart_images=chart_images,
    )

    summary_buf = io.BytesIO()
    summary_df.to_csv(summary_buf, index=False)

    attachments = [
        (f"monthly_inventory_report_{date_str}.docx", docx_bytes),
        (f"inventory_summary_{date_str}.csv",         summary_buf.getvalue()),
    ]

    # Also attach raw CSVs
    for msg_date, df in dfs_data:
        raw_buf = io.BytesIO()
        df.to_csv(raw_buf, index=False)
        attachments.append((f"inventory_raw_{msg_date.strftime('%Y%m%d')}.csv", raw_buf.getvalue()))

    send_report_email(
        subject=f"Monthly Inventory Review — {curr_label}",
        body_text=f"Your monthly inventory report for {curr_label} is attached.",
        recipient=REPORT_RECIPIENT,
        attachments=attachments,
    )


if __name__ == "__main__":
    main()
