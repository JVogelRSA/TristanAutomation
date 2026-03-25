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

# Load environment variables
load_dotenv()

# Configuration
REPORT_RECIPIENT = os.getenv("REPORT_RECIPIENT")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

BREX_API_KEY = os.getenv("BREX_API_KEY")
MERCURY_API_KEY = os.getenv("MERCURY_API_KEY")
RIPPLING_API_KEY = os.getenv("RIPPLING_API_KEY")

def format_currency(x):
    return "${:,.2f}".format(x)

def generate_spend_report(df):
    """
    Uses LLM to analyze the unified spend dataframe for Week-over-Week insights.
    """
    print("Generating Spend Analysis with LLM...")
    
    # Needs Date column as datetime
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Snap to the most recent Monday to ensure consistent Monday-to-Monday reporting
    now = datetime.now()
    days_since_monday = now.weekday() # Monday = 0, Sunday = 6
    most_recent_monday = (now - timedelta(days=days_since_monday)).date()
    
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
    
    summary_text = f"""
    --- SPEND DATA ({curr_week_start.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}) ---
    Total Spend: {format_currency(curr_total)}
    Trend: {trend}
    
    Top 10 Vendors (This Period):
    {top_vendors.to_string()}
    
    Detailed Transaction List (Top 50 by size):
    {curr_df.sort_values(by='Amount', ascending=False).head(50).to_string(index=False)}
    """
    
    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    
    prompt = f"""
    Analyze the following weekly spend data for our hardware/tech company.
    
    DATA SUMMARY:
    {summary_text}
    
    --------------------------------------------------
    
    Create a **Comprehensive Weekly Financial Report** for the CEO. 
    *CRITICAL*: Most transactions are labeled "Uncategorized". You MUST use the Vendor name to infer standard logical business Categories (e.g., Marketing, COGS, Software & Tech, Office Supplies, Travel & Entertainment).
    *CRITICAL*: DO NOT include any conversational filler (e.g. "Here is the report", "This report provides..."). Output ONLY the requested HTML structure.
    
    **REQUIRED REPORT STRUCTURE:**
    
    <h3>💰 <b>1. Executive Spend Snapshot (via Brex)</b></h3>
    *   Total Spend this week.
    *   Week-over-Week Trend (Compare this week to last week based on provided stats).
    *   Largest single transaction of the week.
    *   Add a bold, 1-sentence CFO insight summarizing the week's spend health.
    
    <h3>📊 <b>2. Category Breakdown</b></h3>
    *   Provide a clean HTML table summarizing the total spend grouped by your inferred categories.
    *   Sort the table from highest total spend to lowest.
    *   Ensure all transaction spend is accounted for in these buckets.
    
    <h3>🔍 <b>3. Vendor Deep Dive (Top 10)</b></h3>
    *   List the Top 10 Vendors by total spend.
    *   For each, list the number of transactions, average transaction size, and your inferred Category.
    
    <h3>🚨 <b>4. Anomaly Detection & Review</b></h3>
    *   Identify any transactions > $1,000.
    *   Flag any highly unusual spend or apparent duplicate transactions.
    
    <h3>💡 <b>5. Cost Savings & Optimization Opportunities</b></h3>
    *   As a Fractional CFO, identify 1-2 actionable operational improvements, potential subscription consolidations, or areas of spend that look inefficient based on this week's vendor patterns.
    
    <h3>🧾 <b>6. Transaction Log</b></h3>
    *   Provide a table of the top 20 largest transactions (Date, Vendor, Amount, inferred Category).
    
    **Format Requirements:**
    *   Use professional HTML formatting with clean spacing.
    *   Start immediately with the <h3> tags. NO intro text. NO outro text.
    *   Use <table> for data presentation.

    **Length Constraint:** The final report MUST fit on 1-2 printed pages. Be thorough but concise — use compact tables, short bullet points, and avoid verbose prose.
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
        return report_html, curr_df
    except Exception as e:
        print(f"Error generating LLM report: {e}")
        return "<p>Error generating report.</p>", curr_df

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

    # 3. Analyze
    report_html, curr_df = generate_spend_report(unified_df)

    # 4. Build attachments
    date_str = datetime.now().strftime('%Y-%m-%d')

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
