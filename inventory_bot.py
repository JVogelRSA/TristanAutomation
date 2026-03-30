import os
import io
from datetime import datetime
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
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

# --- SKU MAPPINGS ---
SKU_MAP = {
    '303': 'Winter Solstice',
    '36': 'Small Incandescent Light Bulb (T45 6 pack)',
    '1': 'Daylight DC-1 (Regular)',
    '401': 'Daylight Kids Bundle (Ages 8-14)',
    '400': 'Daylight Kids Bundle (Ages 3-7)',
    '28': 'Daylight Comfy Sleeve',
    '29': 'LAMY Pen',
    '31': 'Daylight Kids Case',
    '37': 'Large Incandescent Light Bulb (ST64 4 pack)',
    '302': 'Keyboard Case Bundle',
    '32': 'Daylight Keyboard Case',
    '35': 'Daylight Keyboard',
    '301': 'Amber Sunday Bundle (2025)',
    '23': 'Daylight Sling',
    '34': 'Daylight Stand',
    '3-36': 'Small Incandescent Light Bulb For Friends (T45)',
    '3-37': 'Large Incandescent Light Bulb For Friends (ST64)'
}

TOP_PRIORITY_SKUS = ['1', '401', '400', '28', '29', '31', '36', '37']

EXCLUDED_SKUS = [
    '3-36', '3-37', '34-', '34-1', '35-', '36-', '37-', '4-3-1', '7'
]

def generate_runway_chart(summary_df: pd.DataFrame) -> bytes | None:
    """
    Render a horizontal bar chart of stock runway by product.
    Colour-coded: red < 4 wks, orange 4–8 wks, green > 8 wks.
    Returns PNG bytes, or None if there is nothing to chart.
    """
    def parse_runway(s):
        try:
            return float(str(s).replace(' weeks', '').strip())
        except (ValueError, TypeError):
            return None

    df = summary_df[summary_df['Avg Wkly Burn'] > 0].copy()
    df['Runway_Float'] = df['Runway (Est)'].apply(parse_runway)
    df = df.dropna(subset=['Runway_Float']).sort_values('Runway_Float')

    if df.empty:
        return None

    DISPLAY_CAP = 30
    df['Runway_Display'] = df['Runway_Float'].clip(upper=DISPLAY_CAP)

    colours = [
        '#C0392B' if r < 4 else '#E67E22' if r < 8 else '#27AE60'
        for r in df['Runway_Float']
    ]

    fig_height = max(3.0, len(df) * 0.48)
    fig, ax = plt.subplots(figsize=(8, fig_height))
    fig.patch.set_facecolor('#FAFAFA')
    ax.set_facecolor('#FAFAFA')

    bars = ax.barh(
        df['Product'], df['Runway_Display'],
        color=colours, edgecolor='white', height=0.58
    )

    for bar, val in zip(bars, df['Runway_Float']):
        label = f"{val:.1f} wks" if val <= DISPLAY_CAP else f"{val:.0f}+ wks"
        ax.text(
            bar.get_width() + 0.3, bar.get_y() + bar.get_height() / 2,
            label, va='center', ha='left', fontsize=8.5, color='#333333'
        )

    ax.axvline(4,  color='#C0392B', linestyle='--', linewidth=1.1, alpha=0.75, label='4-week Critical Zone')
    ax.axvline(8,  color='#E67E22', linestyle='--', linewidth=1.1, alpha=0.75, label='8-week Caution Zone')

    ax.set_xlabel('Estimated Stock Runway (weeks)', fontsize=9, color='#444')
    ax.set_title('Stock Runway by Product', fontsize=12, fontweight='bold',
                 color='#1E3A5F', pad=12)
    ax.legend(fontsize=8, loc='lower right', framealpha=0.7)
    ax.set_xlim(0, max(DISPLAY_CAP + 2, df['Runway_Display'].max() + 3))
    for spine in ('top', 'right'):
        ax.spines[spine].set_visible(False)
    ax.spines['left'].set_color('#CCCCCC')
    ax.spines['bottom'].set_color('#CCCCCC')
    ax.tick_params(axis='both', labelsize=8.5, colors='#444')
    ax.xaxis.grid(True, linestyle=':', linewidth=0.6, alpha=0.6)
    ax.set_axisbelow(True)

    plt.tight_layout(pad=1.2)
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight', facecolor='#FAFAFA')
    plt.close(fig)
    return buf.getvalue()


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
        print("Not enough history to calculate burn rate.")
        return "<p>Error: Not enough historical CSVs found to calculate average burn rate.</p>"

    # Use the latest DF for the baseline schema
    curr_date, curr_df = dfs_data[-1]
    
    # Attempt to identify the 'Item Name' and 'Quantity' columns
    item_col = next((c for c in curr_df.columns if 'item' in c.lower() or 'sku' in c.lower()), curr_df.columns[0])
    qty_col = next((c for c in curr_df.columns if 'hand' in c.lower() or 'qty' in c.lower() or 'available' in c.lower()), curr_df.columns[-1])
    
    print(f"Using columns: Item='{item_col}', Qty='{qty_col}'")

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
        
        runway = (curr_qty / avg_weekly_burn) if avg_weekly_burn > 0 else float('inf')
        runway_str = f"{runway:.1f} weeks" if runway != float('inf') else "N/A"
        
        # Mapping Names and Priority
        product_name = SKU_MAP.get(sku, sku) 
        is_top = "Yes" if sku in TOP_PRIORITY_SKUS else "No"
        
        summary_data.append({
            'SKU': sku,
            'Product': product_name,
            'Top 10': is_top,
            'Current Stock': curr_qty,
            'Avg Wkly Burn': round(avg_weekly_burn, 1),
            'Runway (Est)': runway_str
        })
        
    # Inject any explicitly mapped SKUs that were completely missing from the latest CSV
    processed_skus = [d['SKU'] for d in summary_data]
    for missing_sku, missing_product in SKU_MAP.items():
        if missing_sku not in processed_skus:
            is_top = "Yes" if missing_sku in TOP_PRIORITY_SKUS else "No"
            summary_data.append({
                'SKU': missing_sku,
                'Product': missing_product,
                'Top 10': is_top,
                'Current Stock': 0,
                'Avg Wkly Burn': 0.0,
                'Runway (Est)': 'N/A'
            })
            
    summary_df = pd.DataFrame(summary_data)
    
    # Filter to send a meaningful subset to LLM (all tracked SKUs + anything with burn)
    tracked_skus = list(SKU_MAP.keys())
    active_items = summary_df[(summary_df['Avg Wkly Burn'] > 0) | (summary_df['SKU'].isin(tracked_skus))]
    
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
Analyze the inventory data below. "Avg Wkly Burn" = average units sold per week over the past month.

DATA SUMMARY:
{summary_text}

--------------------------------------------------

Produce a <b>Detailed Weekly Inventory Deep Dive</b> for the Executive Team.
Output ONLY valid HTML — no markdown, no code fences, no introductory text.
Start directly with the first <h3> tag.

REQUIRED SECTIONS:

<h3>1. Actions Required ({date_start} – {date_end})</h3>
Write 2–4 concise bullet points (<ul><li>…</li></ul>) summarising the most important actions the team must take this week.
Be specific: name products, quantities, and urgency. If a Top Priority item has fewer than 6 weeks runway, demand immediate reorder. This section should be scannable in 10 seconds.

<h3>2. Top Priority Items Snapshot</h3>
Include only items where "Top 10" = "Yes".
Render an HTML table with columns: SKU | Product | Current Stock | Avg Wkly Burn | Est. Runway | Status.
In the Status column write "CRITICAL – reorder now" for runway < 4 weeks, "Low – prepare PO" for 4–8 weeks, "Healthy" for > 8 weeks.
Add a <td style="background-color:#FFE0DC;"> inline style on any CRITICAL row's cells.
Below the table, add a short paragraph (<p>) with a plain-English sentence for each critical item explaining the sell-out date and required action.

<h3>3. Burn Rate & Velocity</h3>
Name the top 3 fastest-moving SKUs with their burn rates.
If historical trend data is available, note any SKUs where burn has accelerated or decelerated by more than 15%, and any stock that has dropped sharply vs four weeks ago.
Keep this section to 4–6 bullet points.

<h3>4. Restock Alerts</h3>
List all items (Top Priority or otherwise) in the Red Zone (< 4 weeks runway) and Caution Zone (4–8 weeks runway).
Use a compact 2-column table: Product | Estimated Runway. Apply inline style background-color:#FFE0DC for Red Zone rows, #FFF3CC for Caution Zone rows.
If nothing falls in a zone, write "None at this time."

<h3>5. Full Inventory Data Table</h3>
HTML table with ALL items sorted highest burn rate first: SKU | Product | Current Stock | Avg Wkly Burn | Est. Runway.
Apply inline styles: background-color:#FFE0DC on rows with runway < 4 weeks; background-color:#FFF3CC on rows with runway 4–8 weeks; background-color:#D5F5E3 on rows with runway > 8 weeks and burn > 0.

<p><i>Methodology: Avg Weekly Burn is a {weeks_evaluated}-week moving average of actual stock depletion, counting only weeks with positive drawdown.</i></p>

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

    # Load historical snapshots and generate report
    history = load_history("inventory")
    week_monday = get_week_monday()
    report_html, summary_df, snapshot = generate_llm_report(data, history=history)

    # Save this week's snapshot for future comparisons
    if snapshot:
        save_weekly_snapshot("inventory", week_monday, snapshot)

    print("\n--- REPORT PREVIEW ---\n")
    print(report_html)
    print("\n----------------------\n")

    # Build attachments
    date_str = week_monday.isoformat()

    # Generate runway chart
    chart_png = generate_runway_chart(summary_df)
    chart_images = [chart_png] if chart_png else []

    # Generate DOCX report
    docx_bytes = html_to_docx(report_html, "Weekly Inventory Report", date_str, chart_images=chart_images)

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

    send_report_email(
        subject=f"Weekly Inventory Report - {date_str}",
        body_text="Your weekly inventory report is attached.",
        recipient=REPORT_RECIPIENT,
        attachments=attachments,
    )

if __name__ == "__main__":
    main()
