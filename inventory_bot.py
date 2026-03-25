import os
import io
from datetime import datetime
import pandas as pd
from imap_tools import MailBox, AND
from openai import OpenAI
from dotenv import load_dotenv

# Shared utilities
from utils.email_sender import send_report_email
from utils.docx_generator import html_to_docx

# Load environment variables
load_dotenv()

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

def generate_llm_report(dfs_data):
    """
    Analyzes historical data across multiple weeks to calculate average 'burn rate'
    and estimates stock runway, applying human-readable SKU Mappings.
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
    
    summary_text = f"""
    Report Period: {date_start} to {date_end}
    Multi-Week Inventory Average ({weeks_evaluated} weeks evaluated):
    {active_items.to_string(index=False)}
    """
    
    client = OpenAI(api_key=OPENAI_API_KEY)
    
    prompt = f"""
    You are a Senior Supply Chain Analyst at a high-growth hardware company. 
    Analyze the following inventory data. The "Avg Wkly Burn" represents the calculated average units sold per week over the last month.
    
    DATA SUMMARY:
    {summary_text}
    
    --------------------------------------------------
    
    Create a **Detailed Weekly Inventory Deep Dive** for the Executive Team.
    
    **REQUIRED REPORT STRUCTURE:**
    
    <h3>🌟 <b>1. Top Priority Items Snapshot ({date_start} - {date_end})</b></h3>
    *   Filter for items where "Top 10" is "Yes".
    *   Highlight their Current Stock, Avg Weekly Burn, and Estimated Runway.
    *   **CRITICAL ALERTS**: For any Top 10 item where Runway is < 6 weeks, prominently issue a warning stating exactly when it will sell out and that reordering must happen immediately.
    
    <h3>📅 <b>2. System-Wide Burn Rate & Velocity</b></h3>
    *   Identify the top 3 fastest-moving SKUs across the entire dataset this week.
    *   Highlight highly active SKUs with the largest Average Weekly Burn.
    
    <h3>⏳ <b>3. Critical Stock Runway (Restock Alerts)</b></h3>
    *   **The "4-Week Red Zone"**: List every non-Top-10 item with < 4 weeks of stock left based on current average burn.
    *   **The "6-Week Yellow Zone"**: List items with 4-6 weeks of stock. Advise preparing POs.
    
    <h3>📋 <b>4. All Tracked Items Data Table</b></h3>
    *   Create a clean HTML table showing ALL items passed to you with their Product Name, SKU, Current Stock, Avg Wkly Burn, and Estimated Runway.
    *   Ensure this table is strictly ordered from Highest Avg Wkly Burn to Lowest.
    
    <p><i><b>Methodology Note:</b></i></p>
    *   Add a brief 1-2 sentence explanation at the very bottom stating that the "Avg Weekly Burn" is calculated based on a moving average of actual stock depletion over the last {weeks_evaluated} weeks to ensure accurate trends.
    
    **Format Requirements:**
    *   Use professional HTML formatting. DO NOT include conversational filler like "Here is your report."
    *   Start immediately with the <h3> tags.
    *   Use <table> for lists of data.
    *   Use 🔴 (Red Circle) for critical restock alerts and 🟢 (Green Circle) for safe status.

    **Length Constraint:** Keep the entire report to 1-2 printed pages maximum. Be data-dense and concise — use compact tables and short bullet points.
    """
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a direct, no-nonsense inventory analyst."},
                {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message.content, active_items
    except Exception as e:
        print(f"Error generating LLM report: {e}")
        return "<p>Error generating report.</p>", summary_df

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

    report_html, summary_df = generate_llm_report(data)

    print("\n--- REPORT PREVIEW ---\n")
    print(report_html)
    print("\n----------------------\n")

    # Build attachments
    date_str = datetime.now().strftime('%Y-%m-%d')

    # Generate DOCX report
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

    send_report_email(
        subject=f"Weekly Inventory Report - {date_str}",
        body_text="Your weekly inventory report is attached.",
        recipient=REPORT_RECIPIENT,
        attachments=attachments,
    )

if __name__ == "__main__":
    main()
