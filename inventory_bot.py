import os
import io
import sys
from datetime import datetime, timedelta
import pandas as pd
from imap_tools import MailBox, AND
from openai import OpenAI
from dotenv import load_dotenv

# Shared utilities
from utils.email_sender import send_report_email
from utils.docx_generator import html_to_docx
from utils.history import get_week_monday, save_weekly_snapshot, load_history, build_inventory_comparison

# Load environment variables from the .env file next to this script
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

# Configuration
IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")
IMAP_USERNAME = os.getenv("IMAP_USERNAME")
IMAP_PASSWORD = os.getenv("IMAP_PASSWORD")

REPORT_RECIPIENT = os.getenv("REPORT_RECIPIENT")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EMAIL_SUBJECT_KEYWORD = os.getenv("EMAIL_SUBJECT_KEYWORD", "Inventory")
EMAIL_SENDER = os.getenv("EMAIL_SENDER")

# --- SKU MAPPINGS (Full Daylight catalog) ---
SKU_MAP = {
    # Paid Units
    '1': 'Daylight DC-1',
    '300': 'Daylight DC-1 Amber Sunday Bundle',
    '301': 'Amber Sunday Bundle (2025)',
    '302': 'Keyboard Case Bundle',
    '303': 'Winter Solstice Bundle',
    '400': 'Daylight Kids Bundle (Ages 3-7)',
    '401': 'Daylight Kids Bundle (Ages 8-14)',
    '6': 'Daylight DC-1 POS',
    '6-k': 'Kids Daylight DC-1 POS',
    '7': 'Daylight Kids',
    '100': 'Daylight DC-1 Amber Sunday Bundle',
    '200': 'Daylight DC-1 Amber Sunday Bundle',
    # Open Box
    '2': 'Daylight DC-1 RESEND',
    '4.1.0': 'Daylight DC-1 1.0',
    '4.1.1BQ': 'Daylight DC-1 1.1 BQ',
    '4.1.1DP': 'Daylight DC-1 1.1DP',
    '4.1.1KBQ': 'Kids Daylight DC-1 1.1 BQ',
    '4.1.1KDP': 'Kids Daylight DC-1 1.1DP',
    '4.1.9': 'Daylight DC-1 1.9',
    '4.2.0': 'Daylight DC-1 2.0',
    '4.2.1': 'Daylight DC-1 2.1',
    '4.2.1K': 'Kids Daylight DC-1 2.1',
    '4.2.9': 'Daylight DC-1 2.9',
    '4.3.0': 'Daylight DC-1 3.0',
    '4.3.1': 'Daylight DC-1 3.1',
    '4.3.1K': 'Kids Daylight DC-1 3.1',
    '4.3.9': 'Daylight DC-1 3.9',
    '4.4.1': 'Daylight DC-1 4.1',
    '4.4.1K': 'Kids Daylight DC-1 4.1',
    '4.4.9': 'Daylight DC-1 4.9',
    '5': 'Daylight DC-1 [DOCTOR TEST]',
    '5-k': 'Daylight DC-1 [KIDS TEST]',
    # Gift
    '3': 'Daylight DC-1 GIFT',
    '3-7': 'Daylight Kids For Friends',
    '3-400': 'Daylight Kids Bundle (Ages 3-7) For Friends',
    '3-401': 'Daylight Kids Bundle (Ages 8-14) For Friends',
    # Gift Accessories
    '3-22': 'Daylight Sling for Friends',
    '3-28': 'Daylight Comfy Sleeve for Friends',
    '3-29': 'LAMY Pen For Friends',
    '3-30': 'Kids Stylus For Friends',
    '3-31': 'Daylight Kids Case For Friends',
    '3-34': 'Daylight Stand For Friends',
    '3-34-1': 'Daylight Stand (SAMDI) For Friends',
    '3-35': 'Daylight Keyboard For Friends',
    '3-36': 'Small Incandescent Light Bulb For Friends (T45)',
    '3-37': 'Large Incandescent Light Bulb For Friends (ST64)',
    '3-303-Accessories': 'Winter Solstice Accessories For Friends',
    # Accessories
    '21': 'Daylight Sling + Black Stylus',
    '22': 'Daylight Sling for Friends',
    '22.5': 'Daylight Comfy Sleeve for Friends',
    '23': 'Daylight Sling',
    '25': 'Daylight Folio',
    '26': 'Comfy Knitted Case',
    '28': 'Daylight Comfy Sleeve',
    '29': 'LAMY Pen',
    '30': 'Kids Stylus',
    '31': 'Daylight Kids Case',
    '32': 'Daylight Keyboard Case',
    '34': 'Daylight Stand',
    '34-1': 'Daylight Stand (SAMDI)',
    '35': 'Daylight Keyboard',
    '36': 'Small Incandescent Light Bulb (T45 6 pack)',
    '37': 'Large Incandescent Light Bulb (ST64 4 pack)',
    '38': 'Kids Night Light',
    '40': 'Wooden Light Fixture',
    '90': 'Brown Hat with Daylight Logo',
    '91': 'Brown Hat with Sun',
    '92': 'Tan Hat with Orange Sun',
    '301-INT': 'Amber Sunday Accessories (International)',
    '301-USA': 'Amber Sunday Accessories (USA / Canada)',
    '303-Accessories': 'Winter Solstice Accessories',
    # Deposit
    '5000': 'Daylight DC-1 Pre-Order Deposit',
}

TOP_PRIORITY_SKUS = ['1', '401', '400', '28', '29', '31', '36', '37']

EXCLUDED_SKUS = [
    '3-36', '3-37', '34-', '34-1', '35-', '36-', '37-', '4-3-1', '7'
]

# Default lead time (weeks) from PO → stock-on-hand for reorder decisions.
# Override per-SKU below; anything without an override uses DEFAULT_LEAD_TIME_WEEKS.
DEFAULT_LEAD_TIME_WEEKS = 10
LEAD_TIME_WEEKS = {
    '1': 12,      # DC-1 – long factory lead
    '6': 12,
    '6-k': 12,
    '400': 12,
    '401': 12,
    '28': 6,      # Sleeves / accessories
    '29': 4,
    '31': 6,
    '36': 6,
    '37': 6,
    '303': 14,    # Seasonal bundle
}


def _lead_time(sku: str) -> int:
    return LEAD_TIME_WEEKS.get(sku, DEFAULT_LEAD_TIME_WEEKS)


def _clean_csv_description(desc: str) -> str:
    """
    DCL's CSV Description column often contains name + sub-description concatenated
    (e.g. "Daylight DC-1 Daylight Computer", "Amber Sunday Bundle (2025) SKU: 1 + 34...").
    Strip the noisy tail so we get just the product name.
    """
    if not desc:
        return ''
    d = desc.strip()
    # Cut at "SKU:" (bundle composition) / "Bundle:" / a period followed by a cap
    for sep in [' SKU:', ' Bundle:', '. ', ': ']:
        idx = d.find(sep)
        if idx > 2:
            d = d[:idx].rstrip(' .,-')
            break
    # Collapse whitespace
    d = ' '.join(d.split())
    # De-duplicate obvious "Daylight DC-1 Daylight Computer" tail
    for dup_tail in [' Daylight Computer', ' Daylight Computer Kids Bundle']:
        if d.endswith(dup_tail) and d.count('Daylight') >= 2:
            d = d[: -len(dup_tail)].rstrip()
    return d.strip() or desc.strip()


def _resolve_product_name(sku: str, csv_descriptions: dict) -> str:
    """
    Resolve the display name for a SKU. Priority:
      1. SKU_MAP override (if we've explicitly chosen a prettier name)
      2. CSV Description column (DCL's source of truth)
      3. Bare SKU code (last-resort fallback)
    """
    if sku in SKU_MAP:
        return SKU_MAP[sku]
    if sku in csv_descriptions and csv_descriptions[sku]:
        return csv_descriptions[sku]
    return sku


def _compute_stockout_and_reorder(stock: float, weekly_burn: float, sku: str, today: datetime):
    """
    Given current stock + burn rate, return:
      - stockout_date (date): when stock hits zero at current burn, or None
      - weeks_of_runway (float or inf)
      - reorder_flag (str): one of OVERDUE | THIS WEEK | SOON | OK
    """
    if weekly_burn <= 0 or stock <= 0:
        return None, float('inf'), 'OK'

    weeks_of_runway = stock / weekly_burn
    stockout_date = today + timedelta(days=int(round(weeks_of_runway * 7)))

    lead = _lead_time(sku)
    # Time between "runway" and "lead time" is your reorder buffer
    buffer_weeks = weeks_of_runway - lead
    if buffer_weeks < 0:
        flag = 'OVERDUE'
    elif buffer_weeks < 1:
        flag = 'THIS WEEK'
    elif buffer_weeks < 4:
        flag = 'SOON'
    else:
        flag = 'OK'
    return stockout_date, weeks_of_runway, flag


def _compute_velocity_change(sku: str, curr_burn: float, history: list) -> str:
    """
    Week-over-week burn rate change for a SKU. Returns e.g. '+42%' or '-18%'.
    '—' if no prior data.
    """
    if not history:
        return '—'
    last = history[-1] if isinstance(history, list) else None
    if not last:
        return '—'
    prev = last.get('skus', {}).get(sku, {}).get('burn_rate')
    if prev is None or prev == 0:
        return '—' if prev is None else '+new'
    pct = ((curr_burn - prev) / prev) * 100
    sign = '+' if pct > 0 else ''
    return f"{sign}{pct:.0f}%"

def fetch_latest_emails(limit=4):
    """
    Fetches the latest emails matching the criteria.
    Returns a list of (date, csv_content_as_dataframe) tuples.
    """
    print(f"Connecting to {IMAP_SERVER}...")
    inventory_data = []

    try:
        with MailBox(IMAP_SERVER).login(IMAP_USERNAME, IMAP_PASSWORD) as mailbox:
            # Build search criteria
            criteria = AND(subject=EMAIL_SUBJECT_KEYWORD)
            if EMAIL_SENDER:
                criteria = AND(subject=EMAIL_SUBJECT_KEYWORD, from_=EMAIL_SENDER)
            
            print(f"Searching for emails with subject '{EMAIL_SUBJECT_KEYWORD}'...")
            # Fetch emails, reverse chronological
            for msg in mailbox.fetch(criteria, limit=10, reverse=True):
                if len(inventory_data) >= limit:
                    break
                
                # Check for CSV attachments
                for att in msg.attachments:
                    if att.filename.lower().endswith('.csv'):
                        print(f"Found CSV in email from {msg.date}: {att.filename}")
                        try:
                            # Read CSV content into pandas DataFrame
                            csv_content = io.BytesIO(att.payload)
                            df = pd.read_csv(csv_content)
                            inventory_data.append((msg.date, df))
                        except Exception as e:
                            print(f"Error reading CSV: {e}")
                        break # Only take the first CSV found in the email
            
    except Exception as e:
        print(f"Error fetching emails: {e}")
        return []

    # Sort by date ascending (oldest first) so we can calculate timeline
    inventory_data.sort(key=lambda x: x[0])
    return inventory_data

def generate_llm_report(dfs_data, history=None):
    """
    Analyzes historical data across multiple weeks to calculate average 'burn rate'
    and estimates stock runway, applying human-readable SKU Mappings.
    history: list of past weekly snapshots from utils/history.py
    """
    print("Analyzing comprehensive data and generating report...")
    
    if len(dfs_data) < 2:
        # Callers unpack three values — returning a bare string here used to
        # crash them with "too many values to unpack".
        print("Not enough history to calculate burn rate.")
        return (
            "<p>Error: Not enough historical CSVs found to calculate average burn rate.</p>",
            pd.DataFrame(),
            {},
        )

    # Use the latest DF for the baseline schema
    curr_date, curr_df = dfs_data[-1]

    # Attempt to identify the 'Item Name', 'Quantity', and 'Description' columns
    item_col = next((c for c in curr_df.columns if 'item' in c.lower() or 'sku' in c.lower()), curr_df.columns[0])
    qty_col = next((c for c in curr_df.columns if 'hand' in c.lower() or 'qty' in c.lower() or 'available' in c.lower()), curr_df.columns[-1])
    desc_col = next((c for c in curr_df.columns if 'descrip' in c.lower() or 'name' in c.lower() or 'product' in c.lower()), None)

    print(f"Using columns: Item='{item_col}', Qty='{qty_col}', Desc='{desc_col}'")

    # Build a SKU → description lookup from the CSV's Description column.
    # This is the ground truth maintained by DCL — we use it as the primary
    # source of product names. SKU_MAP (below) is applied only as an override
    # when we want a prettier display name than DCL's raw description.
    csv_descriptions: dict[str, str] = {}
    if desc_col is not None:
        for _, row in curr_df.iterrows():
            sku = str(row[item_col]).strip()
            desc = str(row[desc_col]).strip() if pd.notna(row[desc_col]) else ''
            if sku and desc and sku not in csv_descriptions:
                csv_descriptions[sku] = _clean_csv_description(desc)

    summary_data = []

    # Create lookups for all historical weeks to track changes accurately
    hist_lookups = []
    for date, df in dfs_data:
        df[item_col] = df[item_col].astype(str).str.strip()
        df[qty_col] = pd.to_numeric(df[qty_col], errors='coerce').fillna(0)
        hist_lookups.append(dict(zip(df[item_col], df[qty_col])))
        
    for index, row in curr_df.iterrows():
        sku = str(row[item_col]).strip()
        
        # Skip blacklisted SKUs completely
        if sku in EXCLUDED_SKUS:
            continue
            
        curr_qty = pd.to_numeric(row[qty_col], errors='coerce')
        if pd.isna(curr_qty): curr_qty = 0
        
        # Calculate sequential burn across all available weeks
        total_burn = 0
        weeks_evaluated = len(dfs_data) - 1
        
        for i in range(1, len(dfs_data)):
            prev_qty = hist_lookups[i-1].get(sku, 0)
            this_qty = hist_lookups[i].get(sku, 0)
            drop = prev_qty - this_qty
            if drop > 0:
                total_burn += drop # Only count drops (sales), ignore restocks
                
        avg_weekly_burn = total_burn / weeks_evaluated

        # Deterministic stockout ETA + reorder flag (don't rely on LLM inference)
        today = datetime.now()
        stockout_date, runway, reorder_flag = _compute_stockout_and_reorder(
            curr_qty, avg_weekly_burn, sku, today
        )
        runway_str = f"{runway:.1f} weeks" if runway != float('inf') else "N/A"
        stockout_str = stockout_date.strftime('%Y-%m-%d') if stockout_date else 'N/A'

        # Velocity change vs last week's snapshot
        velocity_str = _compute_velocity_change(sku, avg_weekly_burn, history or [])

        # Mapping Names and Priority — CSV Description is primary, SKU_MAP overrides
        product_name = _resolve_product_name(sku, csv_descriptions)
        is_top = "Yes" if sku in TOP_PRIORITY_SKUS else "No"

        summary_data.append({
            'SKU': sku,
            'Product': product_name,
            'Top 10': is_top,
            'Current Stock': curr_qty,
            'Avg Wkly Burn': round(avg_weekly_burn, 1),
            'Runway (Est)': runway_str,
            'Stockout ETA': stockout_str,
            'Lead Time (wks)': _lead_time(sku),
            'Reorder': reorder_flag,
            'WoW Velocity': velocity_str,
        })
        
    # Inject any top-priority SKUs that were completely missing from the latest CSV.
    # We only bother doing this for SKUs we actively care about (TOP_PRIORITY_SKUS or
    # anything hard-coded in SKU_MAP) — auto-injecting every known SKU bloats the
    # report with zero-stock placeholder rows that don't help the reader.
    processed_skus = {d['SKU'] for d in summary_data}
    must_appear = set(TOP_PRIORITY_SKUS) | set(SKU_MAP.keys())
    for missing_sku in must_appear - processed_skus:
        is_top = "Yes" if missing_sku in TOP_PRIORITY_SKUS else "No"
        summary_data.append({
            'SKU': missing_sku,
            'Product': _resolve_product_name(missing_sku, csv_descriptions),
            'Top 10': is_top,
            'Current Stock': 0,
            'Avg Wkly Burn': 0.0,
            'Runway (Est)': 'N/A',
            'Stockout ETA': 'N/A',
            'Lead Time (wks)': _lead_time(missing_sku),
            'Reorder': 'OK',
            'WoW Velocity': '—',
        })
            
    summary_df = pd.DataFrame(summary_data)

    # Filter to send a meaningful subset to LLM:
    #   - anything actively moving (burn > 0), OR
    #   - anything holding meaningful stock (>= 20 units), OR
    #   - anything we've explicitly named as top priority / mapped
    tracked_skus = set(TOP_PRIORITY_SKUS) | set(SKU_MAP.keys())
    active_items = summary_df[
        (summary_df['Avg Wkly Burn'] > 0)
        | (summary_df['Current Stock'] >= 20)
        | (summary_df['SKU'].isin(tracked_skus))
    ]
    
    # Sort strictly by Burn Rate descending before passing to LLM
    active_items = active_items.sort_values(by='Avg Wkly Burn', ascending=False)
    
    # Extract the exact string dates of the most recent week for the report header
    date_start = dfs_data[-2][0].strftime('%B %d')
    date_end = dfs_data[-1][0].strftime('%B %d')
    
    # Build per-SKU dict for historical comparison and snapshot saving
    current_skus = {}
    for _, row in active_items.iterrows():
        current_skus[row['SKU']] = {
            "product": row['Product'],
            "stock": float(row['Current Stock']),
            "burn_rate": float(row['Avg Wkly Burn']),
        }

    hist_comparison = build_inventory_comparison(history or [], current_skus)

    summary_text = f"""
    Report Period: {date_start} to {date_end}
    Multi-Week Inventory Average ({weeks_evaluated} weeks evaluated):
    {active_items.to_string(index=False)}

    {hist_comparison}
    """
    
    client = OpenAI(api_key=OPENAI_API_KEY)

    prompt = f"""
You are a Senior Supply Chain Analyst at a high-growth hardware company.
Analyze the inventory data below.
 - "Avg Wkly Burn" = average units sold per week over the past month.
 - "Stockout ETA" = deterministic date stock hits zero at current burn (already computed).
 - "Lead Time (wks)" = factory-to-warehouse time for that SKU (already computed).
 - "Reorder" = deterministic flag: OVERDUE / THIS WEEK / SOON / OK based on runway vs lead time.
 - "WoW Velocity" = week-over-week burn rate change (+% / -% / — if no prior data).

You MUST trust the Stockout ETA, Lead Time and Reorder columns — they are pre-computed, not your inference.
Use them directly in the tables below.

DATA SUMMARY:
{summary_text}

--------------------------------------------------

Produce a <b>Detailed Weekly Inventory Deep Dive</b> for the Executive Team.
Output ONLY valid HTML — no markdown, no code fences, no introductory text.
Start directly with the first <h3> tag.

REQUIRED SECTIONS:

<h3>1. Actions Required ({date_start} – {date_end})</h3>
Write 2–4 concise bullet points (<ul><li>…</li></ul>) covering ONLY items with Reorder flag = OVERDUE or THIS WEEK.
For each: name the product, the stockout date, the lead time, and the exact action ("Place PO this week for SKU X — stockout ETA YYYY-MM-DD, lead time N weeks").
If nothing is OVERDUE or THIS WEEK, write a single bullet saying "No immediate reorder actions — next PO window: [earliest SOON item]."

<h3>2. Reorder Priority Queue</h3>
HTML table of every SKU with Reorder flag ≠ OK, sorted so OVERDUE appears first, then THIS WEEK, then SOON.
Columns: SKU | Product | Stock | Wkly Burn | Stockout ETA | Lead Time | Reorder | WoW Velocity.
Apply inline style background-color:#FFE0DC on OVERDUE rows, #FFF3CC on THIS WEEK rows, #FDF6E3 on SOON rows.
If empty, write "No reorder actions required this week."

<h3>3. Top Priority Items Snapshot</h3>
Include only items where "Top 10" = "Yes".
HTML table: SKU | Product | Current Stock | Avg Wkly Burn | Stockout ETA | Reorder | WoW Velocity.
Add background-color:#FFE0DC on any OVERDUE or THIS WEEK row.

<h3>4. Velocity Movers (WoW)</h3>
List up to 5 SKUs with the largest absolute WoW Velocity change (ignore '—' and '+new').
Format: <ul><li><b>Product</b>: burn {{old}} → {{new}} units/wk ({{+/-X%}}) — one-sentence implication</li></ul>.
If fewer than 3 meaningful movers, note it and move on.

<h3>5. Dead Stock & Capital Tied Up</h3>
List up to 5 SKUs with Runway > 52 weeks AND Current Stock > 100 units. For each show: Product, Stock, Burn, Runway.
These are candidates for discount/bundle/liquidation. If none, write "None flagged."

<h3>6. Full Inventory Data Table</h3>
HTML table with ALL items sorted highest burn rate first: SKU | Product | Current Stock | Avg Wkly Burn | Stockout ETA | Reorder.
Apply inline styles: background-color:#FFE0DC on OVERDUE/THIS WEEK rows; background-color:#FFF3CC on SOON rows; background-color:#D5F5E3 on OK rows with burn > 0.

<p><i>Methodology: Avg Weekly Burn is a {weeks_evaluated}-week moving average of actual stock depletion, counting only weeks with positive drawdown. Stockout ETA, Lead Time and Reorder flags are deterministic (not LLM-inferred). Per-SKU lead times can be overridden in inventory_bot.py.</i></p>

FORMAT RULES:
- Valid HTML only. No markdown. No ** bold syntax — use <b> tags.
- Do NOT start with any intro text or sign-off. Start with the first <h3>.
- Tables must have a <thead> with <th> column headers.
- Keep the report tight and scannable — executives will read this in under 2 minutes.
"""
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a direct, no-nonsense inventory analyst."},
                {"role": "user", "content": prompt}
            ]
        )
        snapshot = {"skus": current_skus}
        return response.choices[0].message.content, active_items, snapshot
    except Exception as e:
        print(f"Error generating LLM report: {e}")
        return "<p>Error generating report.</p>", summary_df, {}

def main():
    if not IMAP_USERNAME or not IMAP_PASSWORD or not OPENAI_API_KEY:
        print("Error: Missing environment variables. Please check .env file.")
        return
    if not REPORT_RECIPIENT:
        print("Error: REPORT_RECIPIENT not set.")
        return

    # Fetch last 4 weeks to get a solid moving average
    data = fetch_latest_emails(limit=4)

    if len(data) == 0:
        print("No inventory emails found.")
        return

    print(f"Evaluating data across {len(data)} weeks of history.")

    # Load historical snapshots (excluding this week's own) and generate report
    week_monday = get_week_monday()
    history = load_history("inventory", exclude_week=week_monday)
    report_html, summary_df, snapshot = generate_llm_report(data, history=history)

    print("\n--- REPORT PREVIEW ---\n")
    print(report_html)
    print("\n----------------------\n")

    # Build attachments
    date_str = week_monday.isoformat()

    # Generate DOCX report (no embedded chart — the colour-coded tables
    # in the HTML already communicate the runway status)
    docx_bytes = html_to_docx(report_html, "Weekly Inventory Report", date_str)

    attachments = [(f"weekly_inventory_report_{date_str}.docx", docx_bytes)]

    # CSV: analytical summary
    summary_buffer = io.BytesIO()
    summary_df.to_csv(summary_buffer, index=False)
    attachments.append(("inventory_analytical_summary.csv", summary_buffer.getvalue()))

    # CSV: raw data per week
    for date, df in data:
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False)
        attachments.append((f"inventory_raw_{date.strftime('%Y%m%d')}.csv", csv_buffer.getvalue()))

    sent = send_report_email(
        subject=f"Weekly Inventory Report - {date_str}",
        body_text="Your weekly inventory report is attached.",
        recipient=REPORT_RECIPIENT,
        attachments=attachments,
    )
    if not sent:
        sys.exit(1)
    if snapshot:
        save_weekly_snapshot("inventory", week_monday, snapshot)

if __name__ == "__main__":
    main()
