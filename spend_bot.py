import os
import io
import re
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

# Optional: current cash on hand, used for runway calc. Set in .env / GH secrets.
CASH_BALANCE_USD = os.getenv("CASH_BALANCE_USD")


def format_currency(x):
    return "${:,.2f}".format(x)


def _normalize_vendor(desc: str) -> str:
    """
    Collapse a raw transaction description into a stable vendor key so we can
    group recurring charges across weeks (e.g. "AWS *A1B2C3" and "AWS *D4E5F6"
    both become "AWS").
    """
    if not isinstance(desc, str):
        return ''
    # Drop star-suffixed transaction IDs ("VENDOR *abc123") and long alnum tails
    d = re.sub(r'\s*\*[\w-]+$', '', desc.strip())
    # Drop trailing digits / dates
    d = re.sub(r'[\s#]+\d{3,}$', '', d)
    # Collapse whitespace, strip punctuation noise
    d = re.sub(r'\s+', ' ', d).strip().upper()
    # Keep first 3 tokens max — vendor is usually the head of the description
    return ' '.join(d.split(' ')[:3])


def detect_recurring_subscriptions(full_df: pd.DataFrame, min_hits: int = 2) -> pd.DataFrame:
    """
    Identify likely recurring subscriptions in the 30-day transaction window.
    Signals:
      - Same normalized vendor charged ≥ min_hits times
      - Charge amounts within 10% of median (stable pricing)
      - Gap between charges between 25 and 35 days (monthly) OR 6-8 days (weekly)
    Returns a DataFrame: Vendor | Occurrences | Median Amount | Avg Gap (days) | Cadence | Total 30d
    """
    if full_df.empty:
        return pd.DataFrame()
    df = full_df.copy()
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date'])
    df['_vendor_key'] = df['Description'].apply(_normalize_vendor)
    df = df[df['_vendor_key'] != '']

    rows = []
    for vendor, g in df.groupby('_vendor_key'):
        if len(g) < min_hits:
            continue
        g = g.sort_values('Date')
        amounts = g['Amount'].tolist()
        median_amt = float(pd.Series(amounts).median())
        if median_amt <= 0:
            continue
        # Stable pricing check
        spread = max(amounts) - min(amounts)
        if median_amt > 0 and spread / median_amt > 0.3:
            # Amounts vary too much — probably not a subscription
            continue
        gaps = g['Date'].diff().dt.days.dropna().tolist()
        avg_gap = sum(gaps) / len(gaps) if gaps else 0

        if 6 <= avg_gap <= 8:
            cadence = 'weekly'
        elif 13 <= avg_gap <= 16:
            cadence = 'biweekly'
        elif 25 <= avg_gap <= 35:
            cadence = 'monthly'
        elif 85 <= avg_gap <= 95:
            cadence = 'quarterly'
        else:
            # Irregular — include but mark as such
            cadence = 'irregular'

        rows.append({
            'Vendor': vendor.title(),
            'Occurrences': len(g),
            'Median Amount': round(median_amt, 2),
            'Avg Gap (days)': round(avg_gap, 1),
            'Cadence': cadence,
            'Total 30d': round(g['Amount'].sum(), 2),
        })

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values('Total 30d', ascending=False)


def compute_runway(cash_balance: float, history: list, curr_weekly_spend: float) -> dict:
    """
    Compute weeks of cash runway at the rolling 4-week burn average.
    Returns a dict with: cash_balance, weekly_burn, weeks_remaining, runout_date.
    If cash_balance is None/0, returns an empty dict.
    """
    if not cash_balance or cash_balance <= 0:
        return {}
    recent = [h['total_spend'] for h in (history or [])[-4:] if 'total_spend' in h]
    recent.append(curr_weekly_spend)
    burn = sum(recent) / len(recent) if recent else curr_weekly_spend
    if burn <= 0:
        return {}
    weeks = cash_balance / burn
    runout = datetime.now() + timedelta(days=int(round(weeks * 7)))
    return {
        'cash_balance': round(cash_balance, 2),
        'weekly_burn': round(burn, 2),
        'weeks_remaining': round(weeks, 1),
        'runout_date': runout.strftime('%Y-%m-%d'),
    }

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

    # --- Runway (cash-on-hand / rolling burn) -------------------
    try:
        cash_balance = float(CASH_BALANCE_USD) if CASH_BALANCE_USD else 0.0
    except ValueError:
        cash_balance = 0.0
    runway = compute_runway(cash_balance, history, curr_total)
    runway_block = ""
    if runway:
        runway_block = (
            "--- CASH RUNWAY ---\n"
            f"Cash on hand: ${runway['cash_balance']:,.2f}\n"
            f"Rolling weekly burn (last 5 wks incl this one): ${runway['weekly_burn']:,.2f}\n"
            f"Weeks of runway: {runway['weeks_remaining']}\n"
            f"Projected runout at current burn: {runway['runout_date']}\n"
        )
    else:
        runway_block = (
            "--- CASH RUNWAY ---\n"
            "CASH_BALANCE_USD not configured — set it in .env or GitHub secrets "
            "to enable runway calculation.\n"
        )

    # --- Recurring-subscription detector ------------------------
    # Use the full 30-day window (df), not just the current 7 days
    subs_df = detect_recurring_subscriptions(df)
    if not subs_df.empty:
        subs_block = (
            "--- RECURRING SUBSCRIPTIONS DETECTED (30-day window) ---\n"
            f"{subs_df.to_string(index=False)}\n"
        )
    else:
        subs_block = "--- RECURRING SUBSCRIPTIONS DETECTED ---\nNone detected in last 30 days.\n"

    summary_text = f"""
    --- SPEND DATA ({curr_week_start.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}) ---
    Total Spend: {format_currency(curr_total)}
    Trend: {trend}

    Top 10 Vendors (This Period):
    {top_vendors.to_string()}

    Detailed Transaction List (Top 50 by size):
    {curr_df.sort_values(by='Amount', ascending=False).head(50).to_string(index=False)}

    {hist_comparison}

    {runway_block}

    {subs_block}
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
  - Cash runway (weeks) — use the CASH RUNWAY block verbatim if present; otherwise write "Not configured — set CASH_BALANCE_USD"
Then add a single <p><b>CFO Insight:</b> …</p> — one bold sentence assessing spend health and flagging the most important issue or opportunity.

<h3>2. Cash Runway</h3>
If the CASH RUNWAY block above has numbers, render them as a table:
  Cash on hand | Rolling weekly burn | Weeks remaining | Projected runout date.
Then a single sentence: "At current burn the company runs out of cash on {{runout}}, which is {{weeks}} weeks away."
Apply inline style background-color:#FFE0DC if weeks remaining &lt; 26, #FFF3CC if 26–52, #D5F5E3 if &gt; 52.
If runway is not configured, write "Runway not available — set CASH_BALANCE_USD in environment."

<h3>3. Recurring Subscriptions</h3>
Use the RECURRING SUBSCRIPTIONS DETECTED block verbatim — these are deterministic, not your inference.
Render an HTML table: Vendor | Cadence | Median Amount | Occurrences | Total 30d.
Sort by Total 30d descending.
Below the table, in one or two bullet points flag any subscription that looks redundant, unusually large, or is likely to be unused given the business context.
If the block says "None detected", write "No recurring subscription patterns detected in the last 30 days."

<h3>4. Spend by Category</h3>
HTML table: Category | Total Spend | % of Week | Txn Count.
Sort highest to lowest. Apply inline style background-color:#FFE0DC on any category that is unexpectedly high or anomalous.
Every dollar of spend must appear in exactly one category. End with a "Total" footer row.

<h3>5. Top 10 Vendors</h3>
HTML table: Vendor | Category | Total Spend | Txn Count | Avg Txn Size.
Sorted by Total Spend descending. Bold the vendor name in each row.

<h3>6. Anomalies & Items for Review</h3>
List every transaction over $1,000 in a table: Date | Vendor | Amount | Category | Note.
Also flag any apparent duplicates (same vendor + same amount within the week) or unusual spend spikes.
If nothing warrants flagging, write "No anomalies detected this week."

<h3>7. Cost Savings & Optimisation</h3>
2–3 specific, actionable bullet points based on actual vendor patterns in this data.
Name the vendor or category. Quantify the opportunity where possible. No generic advice.
Prefer picking from the Recurring Subscriptions table when relevant.

<h3>8. Full Transaction Log (Top 20 by Amount)</h3>
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
            "runway": runway,  # dict with cash_balance, weekly_burn, weeks_remaining, runout_date (or {})
            "subscriptions": subs_df.to_dict(orient='records') if not subs_df.empty else [],
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
