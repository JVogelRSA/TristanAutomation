import os
import io
from datetime import datetime, timedelta
import pandas as pd
from anthropic import Anthropic
from dotenv import load_dotenv

# Adapters
from adapters.brex import fetch_brex_transactions
from adapters.mercury import fetch_mercury_transactions
from adapters.rippling import fetch_rippling_expenses

# Shared utilities
from utils.email_sender import send_report_email
from utils.docx_generator import html_to_docx
from utils.history import get_week_monday, save_weekly_snapshot, load_history, build_spend_comparison

# Load environment variables from the .env file next to this script
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'), override=True)

# Configuration
REPORT_RECIPIENT = os.getenv("REPORT_RECIPIENT")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

BREX_API_KEY = os.getenv("BREX_API_KEY")
MERCURY_API_KEY = os.getenv("MERCURY_API_KEY")
RIPPLING_API_KEY = os.getenv("RIPPLING_API_KEY")

def format_currency(x):
    return "${:,.2f}".format(x)

def generate_spend_report(df, history=None):
    """
    Uses LLM to analyze the unified spend dataframe for Week-over-Week insights.
    history: list of past weekly snapshots from utils/history.py
    """
    print("Generating Spend Analysis with LLM...")

    # Needs Date column as datetime
    df['Date'] = pd.to_datetime(df['Date'])

    # Snap to the most recent Monday to ensure consistent Monday-to-Monday reporting
    most_recent_monday = get_week_monday()

    end_date = pd.to_datetime(most_recent_monday)
    curr_week_start = end_date - timedelta(days=7)
    prev_week_start = end_date - timedelta(days=14)
    
    curr_df = df[(df['Date'] >= curr_week_start) & (df['Date'] < end_date)]
    prev_df = df[(df['Date'] >= prev_week_start) & (df['Date'] < curr_week_start)]
    
    # Calculate basic stats locally
    curr_total = curr_df['Amount'].sum()
    prev_total = prev_df['Amount'].sum()
    
    trend = "N/A"
    if prev_total > 0:
        pct_change = ((curr_total - prev_total) / prev_total) * 100
        trend = f"{'+' if pct_change > 0 else ''}{pct_change:.1f}% vs Last Week ({format_currency(prev_total)})"
    
    top_vendors = curr_df.groupby('Description')['Amount'].sum().sort_values(ascending=False).head(10)
    
    # Build historical comparison if we have past data
    hist_comparison = build_spend_comparison(history or [], curr_total, most_recent_monday)

    summary_text = f"""
    --- SPEND DATA ({curr_week_start.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}) ---
    Total Spend: {format_currency(curr_total)}
    Trend: {trend}

    Top 10 Vendors (This Period):
    {top_vendors.to_string()}

    Detailed Transaction List (Top 50 by size):
    {curr_df.sort_values(by='Amount', ascending=False).head(50).to_string(index=False)}

    {hist_comparison}
    """
    
    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    
    prompt = f"""
You are an elite Fractional CFO for a hardware/tech company. Analyse the weekly spend data below and produce a Comprehensive Weekly Financial Report for the CEO.

DATA SUMMARY:
{summary_text}

--------------------------------------------------

Output ONLY valid HTML — no markdown, no code fences, no ** bold syntax (use <b> tags), no introductory or closing text.
Start directly with the first <h3> tag.

Most transactions are labelled "Uncategorized" — you MUST infer logical business categories from vendor names
(e.g. Marketing, COGS / Inventory, Software & Subscriptions, Payroll & Benefits, Travel & Entertainment, Office & Facilities, Professional Services, Shipping & Logistics).

REQUIRED SECTIONS:

<h3>1. Executive Spend Snapshot</h3>
Present the following as a compact 2-column summary table (Metric | Value):
  - Total spend this week
  - Week-over-Week change (amount + %)
  - Rolling 4-week average (if historical data available)
  - Largest single transaction
  - Number of transactions
Then add a single <p><b>CFO Insight:</b> …</p> — one bold sentence assessing spend health and flagging the most important issue or opportunity.

<h3>2. Spend by Category</h3>
HTML table: Category | Total Spend | % of Week | Txn Count.
Sort highest to lowest. Apply inline style background-color:#FFE0DC on any category that is unexpectedly high or anomalous.
Every dollar of spend must appear in exactly one category. End with a "Total" footer row.

<h3>3. Top 10 Vendors</h3>
HTML table: Vendor | Category | Total Spend | Txn Count | Avg Txn Size.
Sorted by Total Spend descending. Bold the vendor name in each row.

<h3>4. Anomalies & Items for Review</h3>
List every transaction over $1,000 in a table: Date | Vendor | Amount | Category | Note.
Also flag any apparent duplicates (same vendor + same amount within the week) or unusual spend spikes.
If nothing warrants flagging, write "No anomalies detected this week."

<h3>5. Cost Savings & Optimisation</h3>
2–3 specific, actionable bullet points based on actual vendor patterns in this data.
Name the vendor or category. Quantify the opportunity where possible. No generic advice.

<h3>6. Full Transaction Log (Top 20 by Amount)</h3>
HTML table: Date | Vendor | Amount | Category.
Apply inline style background-color:#FFF3CC on rows with Amount >= $1,000.

FORMAT RULES:
- Valid HTML only. No markdown. Use <b> not **bold**.
- Tables must have <thead><tr><th> headers.
- Currency formatted as $X,XXX.XX throughout.
- Keep every section concise — this report must be scannable in under 3 minutes.
- Do NOT start with any greeting or intro. Do NOT end with a sign-off.
"""
    
    try:
        response = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=4096,
            temperature=0,
            system="You are an elite, highly analytical Fractional CFO. You identify operational inefficiencies, unnecessary subscriptions, and actionable cost-saving opportunities by deeply analyzing transaction context.",
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        report_html = response.content[0].text

        # Build snapshot data for persistence
        snapshot = {
            "total_spend": round(curr_total, 2),
            "prev_week_spend": round(prev_total, 2),
            "transaction_count": len(curr_df),
            "top_vendors": {k: round(v, 2) for k, v in top_vendors.head(5).items()},
            "by_source": {k: round(v, 2) for k, v in curr_df.groupby('Source')['Amount'].sum().items()} if 'Source' in curr_df.columns else {},
        }
        return report_html, curr_df, snapshot
    except Exception as e:
        print(f"Error generating LLM report: {e}")
        import traceback
        traceback.print_exc()
        return f"<p><b>Error generating report:</b> {e}</p>", curr_df, {}

def main():
    print("Starting Spend Analysis Bot...")

    if not ANTHROPIC_API_KEY:
        print("Error: ANTHROPIC_API_KEY not set.")
        return
    if not REPORT_RECIPIENT:
        print("Error: REPORT_RECIPIENT not set.")
        return

    # 1. Fetch Data (30 days to safely cover two full Monday-to-Monday periods regardless of run day)
    brex_df = fetch_brex_transactions(BREX_API_KEY, days_back=30)
    mercury_df = fetch_mercury_transactions(MERCURY_API_KEY)
    rippling_df = fetch_rippling_expenses(RIPPLING_API_KEY)

    # 2. Unify Data
    for df, name in [(brex_df, 'Brex'), (mercury_df, 'Mercury'), (rippling_df, 'Rippling')]:
        if not df.empty and 'Source' not in df.columns:
            df['Source'] = name

    unified_df = pd.concat([brex_df, mercury_df, rippling_df], ignore_index=True)

    if unified_df.empty:
        print("No data fetched from any source. Check API keys.")
        return

    # 3. Load historical data and analyze
    history = load_history("spend")
    week_monday = get_week_monday()
    report_html, curr_df, snapshot = generate_spend_report(unified_df, history=history)

    # Save this week's snapshot for future comparisons
    if snapshot:
        save_weekly_snapshot("spend", week_monday, snapshot)

    # 4. Build attachments
    date_str = week_monday.isoformat()

    # Generate DOCX report
    docx_bytes = html_to_docx(report_html, "Weekly Spend Analysis (Brex)", date_str)

    # CSV data files
    curr_csv_io = io.BytesIO()
    curr_df.to_csv(curr_csv_io, index=False)

    full_csv_io = io.BytesIO()
    unified_df.to_csv(full_csv_io, index=False)

    attachments = [
        (f"weekly_spend_report_{date_str}.docx", docx_bytes),
        ("analyzed_spend_7_days.csv", curr_csv_io.getvalue()),
        ("full_30_days_raw.csv", full_csv_io.getvalue()),
    ]

    # 5. Send
    send_report_email(
        subject=f"Weekly Spend Analysis (Brex) - {date_str}",
        body_text="Your weekly spend report is attached.",
        recipient=REPORT_RECIPIENT,
        attachments=attachments,
    )

if __name__ == "__main__":
    main()
