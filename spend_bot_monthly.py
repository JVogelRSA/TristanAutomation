"""
Monthly Spend Analysis Bot
Runs on the last day of each month. Analyses the full calendar month vs the
previous calendar month and emails a professional DOCX report.
"""
import os
import io
import calendar
from datetime import datetime, timedelta, date

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from anthropic import Anthropic
from dotenv import load_dotenv

from adapters.brex import fetch_brex_transactions
from adapters.mercury import fetch_mercury_transactions
from adapters.rippling import fetch_rippling_expenses

from utils.email_sender import send_report_email
from utils.docx_generator import html_to_docx
from utils.history import (
    get_month_key, save_monthly_snapshot, load_monthly_history,
    build_monthly_spend_comparison,
)

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'), override=True)

REPORT_RECIPIENT  = os.getenv("REPORT_RECIPIENT")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
BREX_API_KEY      = os.getenv("BREX_API_KEY")
MERCURY_API_KEY   = os.getenv("MERCURY_API_KEY")
RIPPLING_API_KEY  = os.getenv("RIPPLING_API_KEY")


def fmt(x):
    return "${:,.2f}".format(x)


def get_month_windows():
    """
    Return pandas Timestamps for the start and end of the current and
    previous calendar months.
    """
    today = date.today()

    # Current month: 1st → last day (inclusive)
    curr_start = today.replace(day=1)
    curr_last  = calendar.monthrange(today.year, today.month)[1]
    curr_end   = today.replace(day=curr_last)

    # Previous month
    if today.month == 1:
        prev_start = today.replace(year=today.year - 1, month=12, day=1)
    else:
        prev_start = today.replace(month=today.month - 1, day=1)
    prev_last = calendar.monthrange(prev_start.year, prev_start.month)[1]
    prev_end  = prev_start.replace(day=prev_last)

    return (
        pd.Timestamp(curr_start), pd.Timestamp(curr_end) + pd.Timedelta(days=1),
        pd.Timestamp(prev_start), pd.Timestamp(prev_end) + pd.Timedelta(days=1),
    )


def generate_vendor_chart(curr_df, month_label):
    """
    Horizontal bar chart: top 12 vendors by total spend this month.
    Returns PNG bytes.
    """
    if curr_df.empty:
        return None

    top = (
        curr_df.groupby('Description')['Amount']
        .sum()
        .sort_values(ascending=False)
        .head(12)
        .iloc[::-1]   # Flip so highest is at the top
    )

    if top.empty:
        return None

    fig, ax = plt.subplots(figsize=(8, max(3.5, len(top) * 0.50)))
    fig.patch.set_facecolor('#FAFAFA')
    ax.set_facecolor('#FAFAFA')

    colours = ['#1E3A5F' if i >= len(top) - 3 else '#4A7FA5' for i in range(len(top))]
    bars = ax.barh(top.index, top.values, color=colours, edgecolor='white', height=0.60)

    for bar, val in zip(bars, top.values):
        ax.text(
            bar.get_width() + top.values.max() * 0.01,
            bar.get_y() + bar.get_height() / 2,
            fmt(val), va='center', ha='left', fontsize=8.5, color='#333'
        )

    ax.set_xlabel('Total Spend', fontsize=9, color='#444')
    ax.set_title(f'Top Vendors by Spend — {month_label}', fontsize=12,
                 fontweight='bold', color='#1E3A5F', pad=12)
    ax.xaxis.set_major_formatter(
        plt.FuncFormatter(lambda v, _: f'${v:,.0f}')
    )
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


def generate_monthly_report(unified_df, history=None):
    """
    Build the monthly HTML report using the LLM.
    Returns (report_html, curr_df, snapshot_dict).
    """
    print("Generating Monthly Spend Analysis with LLM...")

    unified_df['Date'] = pd.to_datetime(unified_df['Date'])

    curr_start, curr_end, prev_start, prev_end = get_month_windows()

    curr_df = unified_df[(unified_df['Date'] >= curr_start) & (unified_df['Date'] < curr_end)]
    prev_df = unified_df[(unified_df['Date'] >= prev_start) & (unified_df['Date'] < prev_end)]

    curr_total = curr_df['Amount'].sum()
    prev_total = prev_df['Amount'].sum()

    mom_pct  = ((curr_total - prev_total) / prev_total * 100) if prev_total > 0 else None
    mom_str  = f"{'+' if mom_pct >= 0 else ''}{mom_pct:.1f}% vs {prev_start.strftime('%B %Y')} ({fmt(prev_total)})" if mom_pct is not None else "N/A (no prior month data)"

    top_vendors   = curr_df.groupby('Description')['Amount'].sum().sort_values(ascending=False).head(10)
    month_label   = curr_start.strftime('%B %Y')
    prev_label    = prev_start.strftime('%B %Y')
    hist_text     = build_monthly_spend_comparison(history or [], curr_total)

    summary_text = f"""
--- MONTHLY SPEND DATA: {month_label} ---
Period: {curr_start.strftime('%Y-%m-%d')} to {(curr_end - pd.Timedelta(days=1)).strftime('%Y-%m-%d')}
Total Spend This Month: {fmt(curr_total)}
Previous Month ({prev_label}) Total: {fmt(prev_total)}
Month-over-Month: {mom_str}
Transaction Count: {len(curr_df)}

Top 10 Vendors This Month:
{top_vendors.to_string()}

Top 60 Transactions by Amount:
{curr_df.sort_values('Amount', ascending=False).head(60).to_string(index=False)}

{hist_text}
"""

    client = Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt = f"""
You are an elite Fractional CFO for a hardware/tech company. Analyse the full-month spend data below and produce a Monthly Financial Report for the CEO and Board.

DATA SUMMARY:
{summary_text}

--------------------------------------------------

Output ONLY valid HTML — no markdown, no code fences, no ** bold syntax (use <b> tags), no intro or sign-off text.
Start directly with the first <h3> tag.

Most transactions are labelled "Uncategorized" — infer logical business categories from vendor names
(e.g. Marketing, COGS / Inventory, Software & Subscriptions, Payroll & Benefits, Travel & Entertainment, Office & Facilities, Professional Services, Shipping & Logistics).

REQUIRED SECTIONS:

<h3>1. Monthly Executive Summary</h3>
A 2-column summary table (Metric | Value) containing:
  - Total spend for {month_label}
  - Month-over-Month change (amount + %)
  - Prior month total ({prev_label})
  - Historical average (if available)
  - Largest single transaction
  - Total transaction count
  - Projected annual run-rate (monthly total × 12)
Then add <p><b>CFO Commentary:</b> …</p> — 2–3 sentences assessing the month's spend health, biggest driver, and one forward-looking note.

<h3>2. Category Breakdown</h3>
HTML table: Category | Total Spend | % of Month | Txn Count | MoM Change.
Sort highest to lowest spend. Apply inline style background-color:#FFE0DC on any category where spend is anomalously high.
All spend must appear in one category. End with a bold "Total" footer row.

<h3>3. Top 10 Vendors</h3>
HTML table: Vendor | Category | Monthly Total | Txn Count | Avg Txn | vs Prior Month.
Sorted by Monthly Total descending. Bold each vendor name.
If a vendor is new this month (didn't appear last month), mark it "[NEW]".

<h3>4. Month-over-Month Analysis</h3>
3–4 specific bullet points comparing this month to last.
Call out: categories that grew most, categories that shrank, new vendors, dropped vendors, and any spend spikes.
Be specific with numbers.

<h3>5. Anomalies & Large Transactions</h3>
Table of every transaction over $500: Date | Vendor | Amount | Category | Note.
Flag duplicates (same vendor + amount), unusual one-offs, or anything worth a second look.
If nothing warrants review, write "No anomalies detected."

<h3>6. Cost Savings & Recommendations</h3>
3–4 specific, actionable bullet points.
Name the vendor or category. Quantify opportunity where possible. Include at least one forward-looking budget recommendation for next month.

<h3>7. Full Transaction Log (Top 25 by Amount)</h3>
HTML table: Date | Vendor | Amount | Category.
Apply inline style background-color:#FFF3CC on rows with Amount >= $500.

FORMAT RULES:
- Valid HTML only. No markdown. Use <b> tags.
- Tables must have <thead><tr><th> headers.
- Currency as $X,XXX.XX. Percentages as +X.X% or -X.X%.
- Scannable in under 4 minutes. No filler prose.
"""

    try:
        response = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=6000,
            temperature=0,
            system="You are an elite Fractional CFO. Produce concise, data-dense financial analysis. Every insight must reference actual numbers from the data.",
            messages=[{"role": "user", "content": prompt}],
        )
        report_html = response.content[0].text

        snapshot = {
            "total_spend":        round(curr_total, 2),
            "prev_month_spend":   round(prev_total, 2),
            "transaction_count":  len(curr_df),
            "top_vendors":        {k: round(v, 2) for k, v in top_vendors.head(5).items()},
            "by_source": (
                {k: round(v, 2) for k, v in curr_df.groupby('Source')['Amount'].sum().items()}
                if 'Source' in curr_df.columns else {}
            ),
        }
        return report_html, curr_df, snapshot
    except Exception as e:
        print(f"Error generating LLM report: {e}")
        return "<p>Error generating report.</p>", curr_df, {}


def main():
    print("Starting Monthly Spend Analysis Bot...")

    if not ANTHROPIC_API_KEY:
        print("Error: ANTHROPIC_API_KEY not set.")
        return
    if not REPORT_RECIPIENT:
        print("Error: REPORT_RECIPIENT not set.")
        return

    # Fetch ~65 days to cover current month + full previous month
    brex_df    = fetch_brex_transactions(BREX_API_KEY, days_back=65)
    mercury_df = fetch_mercury_transactions(MERCURY_API_KEY)
    rippling_df = fetch_rippling_expenses(RIPPLING_API_KEY)

    for df, name in [(brex_df, 'Brex'), (mercury_df, 'Mercury'), (rippling_df, 'Rippling')]:
        if not df.empty and 'Source' not in df.columns:
            df['Source'] = name

    unified_df = pd.concat([brex_df, mercury_df, rippling_df], ignore_index=True)

    if unified_df.empty:
        print("No data fetched from any source. Check API keys.")
        return

    # Load history and generate report
    history = load_monthly_history("spend_monthly")
    report_html, curr_df, snapshot = generate_monthly_report(unified_df, history=history)

    # Save snapshot
    month_key = get_month_key()
    if snapshot:
        save_monthly_snapshot("spend_monthly", month_key, snapshot)

    # Build attachments
    curr_start, *_ = get_month_windows()
    month_label = curr_start.strftime('%B %Y')
    date_str    = curr_start.strftime('%Y-%m')

    # Vendor chart
    chart_png    = generate_vendor_chart(curr_df, month_label)
    chart_images = [chart_png] if chart_png else []

    docx_bytes = html_to_docx(
        report_html, f"Monthly Spend Analysis — {month_label}",
        date_str=date_str, chart_images=chart_images,
    )

    curr_csv = io.BytesIO()
    curr_df.to_csv(curr_csv, index=False)

    full_csv = io.BytesIO()
    unified_df.to_csv(full_csv, index=False)

    attachments = [
        (f"monthly_spend_report_{date_str}.docx", docx_bytes),
        (f"spend_{date_str}_transactions.csv",    curr_csv.getvalue()),
        (f"spend_raw_65days.csv",                 full_csv.getvalue()),
    ]

    send_report_email(
        subject=f"Monthly Spend Analysis — {month_label}",
        body_text=f"Your monthly spend report for {month_label} is attached.",
        recipient=REPORT_RECIPIENT,
        attachments=attachments,
    )


if __name__ == "__main__":
    main()
