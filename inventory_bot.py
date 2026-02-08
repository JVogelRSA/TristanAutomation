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
    Sends the data to LLM to generate a comparison report.
    """
    print("Generating report with LLM...")
    
    summary_text = f"""
    Here is the inventory data for the last two weeks.
    
    Previous Week Data (Sample):
    {prev_df.to_string(index=False)}
    
    Current Week Data (Sample):
    {curr_df.to_string(index=False)}
    """
    
    client = OpenAI(api_key=OPENAI_API_KEY)
    
    prompt = f"""
    You are an inventory management expert. 
    Analyze the following two datasets (Previous Week vs Current Week).
    The data represents our warehouse stock.
    
    {summary_text}
    
    Please write a concise executive summary email for my boss.
    1. Highlight any significant drops in inventory (sales).
    2. Highlight any low stock items that need reordering.
    3. Point out any stagnant items (no change).
    
    **Format the output as HTML** using <h3> for headers, <ul>/<li> for lists, and <b> for emphasis.
    Do not include valid HTML boilerplate (html/body tags), just the content.
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
