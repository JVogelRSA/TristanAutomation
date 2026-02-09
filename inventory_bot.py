import os
import io
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
import pandas as pd
from imap_tools import MailBox, AND
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")
IMAP_USERNAME = os.getenv("IMAP_USERNAME")
IMAP_PASSWORD = os.getenv("IMAP_PASSWORD")

SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
REPORT_RECIPIENT = os.getenv("REPORT_RECIPIENT")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EMAIL_SUBJECT_KEYWORD = os.getenv("EMAIL_SUBJECT_KEYWORD", "Inventory")
EMAIL_SENDER = os.getenv("EMAIL_SENDER")

def fetch_latest_emails(limit=2):
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

    # Sort by date ascending (oldest first) so we can compare last week -> this week
    inventory_data.sort(key=lambda x: x[0])
    return inventory_data

def generate_llm_report(prev_df, curr_df):
    """
    Analyzes the change between two weeks to calculate 'burn rate' (sales velocity)
    and estimates stock runway.
    """
    print("Analyzing data and generating report...")
    
    # Attempt to identify the 'Item Name' and 'Quantity' columns
    # Common DCL headers: 'Item', 'Item Number', 'Description', 'Quantity On Hand', 'Available'
    item_col = next((c for c in curr_df.columns if 'item' in c.lower() or 'sku' in c.lower()), curr_df.columns[0])
    qty_col = next((c for c in curr_df.columns if 'hand' in c.lower() or 'qty' in c.lower() or 'available' in c.lower()), curr_df.columns[-1])
    
    print(f"Using columns: Item='{item_col}', Qty='{qty_col}'")

    # Merge the two weeks
    merged = pd.merge(
        prev_df[[item_col, qty_col]], 
        curr_df[[item_col, qty_col]], 
        on=item_col, 
        suffixes=('_prev', '_curr')
    )

    # Calculate Weekly Burn (Sales)
    # Sales = Last Week - This Week (if positive)
    merged['Weekly_Sales'] = merged[f'{qty_col}_prev'] - merged[f'{qty_col}_curr']
    merged['Weekly_Sales'] = merged['Weekly_Sales'].apply(lambda x: max(0, x)) # Ignore restocks for burn calculation
    
    # Calculate Runway (Weeks Left)
    # Runway = Current Qty / Weekly Sales
    def calculate_runway(row):
        if row['Weekly_Sales'] <= 0:
            return "N/A (No sales)"
        runway = row[f'{qty_col}_curr'] / row['Weekly_Sales']
        return f"{runway:.1f} weeks"

    merged['Runway'] = merged.apply(calculate_runway, axis=1)

    # Prepare data for LLM
    # We'll send a subset: Item, Current Stock, Sales this week, Runway
    summary_df = merged[[item_col, f'{qty_col}_curr', 'Weekly_Sales', 'Runway']]
    summary_df.columns = ['Item', 'Current Stock', 'Sales (This Week)', 'Runway (Est)']
    
    summary_text = f"""
    Inventory Comparison:
    {summary_df.to_string(index=False)}
    """
    
    client = OpenAI(api_key=OPENAI_API_KEY)
    
    prompt = f"""
    You are an inventory and supply chain expert. 
    Analyze the following inventory change data (Previous Week vs Current Week).
    
    {summary_text}
    
    Please write a professional Executive Summary report for my boss.
    Include the following sections:
    1. 📈 **Burn Rate Overview**: Which items are selling fastest?
    2. ⏳ **Stock Runway**: Highlight items that will run out in less than 4 weeks.
    3. 🚨 **Reorder Recommendations**: Give specific "Order Now" recommendations for low-runway items.
    4. 🧊 **Stagnant Items**: Note items with 0 sales this week.
    
    **Format as HTML**. Use <h3> for headers, <ul>/<li> for lists, and <b> for emphasis.
    Use a <table> for the Item Summary.
    Keep it professional, concise, and actionable.
    """
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a helpful inventory assistant."},
                {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"Error generating LLM report: {e}")
        return "<p>Error generating report.</p>"

def send_email_report(report_content, attachments=None):
    """
    Sends the generated report via Email with attachments.
    attachments: list of tuples (filename, content_bytes)
    """
    print(f"Sending email to {REPORT_RECIPIENT}...")
    
    from email.mime.multipart import MIMEMultipart
    from email.mime.application import MIMEApplication

    msg = MIMEMultipart()
    msg['Subject'] = f"Weekly Inventory Report - {datetime.now().strftime('%Y-%m-%d')}"
    msg['From'] = SMTP_USERNAME
    msg['To'] = REPORT_RECIPIENT

    # Attach the HTML body
    msg.attach(MIMEText(report_content, 'html'))

    # Attach CSV files
    if attachments:
        for filename, content in attachments:
            part = MIMEApplication(content, Name=filename)
            part['Content-Disposition'] = f'attachment; filename="{filename}"'
            msg.attach(part)

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
        print("Email sent successfully!")
    except Exception as e:
        print(f"Error sending email: {e}")

def main():
    if not IMAP_USERNAME or not IMAP_PASSWORD or not OPENAI_API_KEY:
        print("Error: Missing environment variables. Please check .env file.")
        return

    data = fetch_latest_emails(limit=2)
    
    csv_attachments = []
    
    if len(data) == 0:
        print("No inventory emails found.")
        return
    elif len(data) == 1:
        print("Only one inventory email found. Generating report based on single week.")
        curr_date, curr_df = data[0]
        # TODO: Handle single week report
        prev_df = curr_df # Hack for now
        
        # Prepare attachment
        csv_buffer = io.BytesIO()
        curr_df.to_csv(csv_buffer, index=False)
        csv_attachments.append((f"inventory_{curr_date.strftime('%Y%m%d')}.csv", csv_buffer.getvalue()))
        
    else:
        print(f"Comparing data from {data[0][0]} and {data[1][0]}")
        prev_date, prev_df = data[0]
        curr_date, curr_df = data[1]
        
        # Prepare attachments
        for date, df in data:
            csv_buffer = io.BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_attachments.append((f"inventory_{date.strftime('%Y%m%d')}.csv", csv_buffer.getvalue()))

    report = generate_llm_report(prev_df, curr_df)
    print("\n--- REPORT PREVIEW ---\n")
    print(report)
    print("\n----------------------\n")
    
    if REPORT_RECIPIENT:
        send_email_report(report, attachments=csv_attachments)
    else:
        print("No recipient configured, skipping email send.")

if __name__ == "__main__":
    main()
