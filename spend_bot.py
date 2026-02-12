import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from datetime import datetime, timedelta
import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv

# Adapters
from adapters.brex import fetch_brex_transactions
from adapters.mercury import fetch_mercury_transactions
from adapters.rippling import fetch_rippling_expenses

# Load environment variables
load_dotenv()

# Configuration
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
REPORT_RECIPIENT = os.getenv("REPORT_RECIPIENT")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

BREX_API_KEY = os.getenv("BREX_API_KEY")
MERCURY_API_KEY = os.getenv("MERCURY_API_KEY")
RIPPLING_API_KEY = os.getenv("RIPPLING_API_KEY")

def format_currency(x):
    return "${:,.2f}".format(x)

def generate_spend_report(df):
    """
    Uses LLM to analyze the unified spend dataframe.
    """
    print("Generating Spend Analysis with LLM...")
    
    # 1. Calculate basic stats locally (save tokens)
    total_spend = df['Amount'].sum()
    top_vendors = df.groupby('Description')['Amount'].sum().sort_values(ascending=False).head(5)
    spend_by_source = df.groupby('Source')['Amount'].sum()
    
    summary_text = f"""
    Total Spend: {format_currency(total_spend)}
    
    Top 5 Vendors:
    {top_vendors.to_string()}
    
    Spend by Source:
    {spend_by_source.to_string()}
    
    Detailed Transaction List (Top 50 by size):
    {df.sort_values(by='Amount', ascending=False).head(50).to_string(index=False)}
    """
    
    client = OpenAI(api_key=OPENAI_API_KEY)
    
    prompt = f"""
    You are a CFO / Financial Analyst.
    Analyze the following spend data for the company.
    
    {summary_text}
    
    Write a Weekly Spend Report email.
    1. **Executive Summary**: Total spend and biggest drivers.
    2. **Anomalies**: Point out any unusually large or suspicious transactions.
    3. **Category Breakdown**: Where is the money going?
    
    **Format as HTML**. Use <h2> for sections, tables for data where appropriate.
    """
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a helpful financial analyst."},
                {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"Error generating LLM report: {e}")
        return "<p>Error generating report.</p>"

def send_email(content, recipient, attachments=None):
    if not recipient:
        print("No recipient specified.")
        return

    msg = MIMEMultipart()
    msg['Subject'] = f"Weekly Spend Analysis - {datetime.now().strftime('%Y-%m-%d')}"
    msg['From'] = SMTP_USERNAME
    msg['To'] = recipient

    msg.attach(MIMEText(content, 'html'))
    
    if attachments:
        for filename, data in attachments:
            part = MIMEApplication(data, Name=filename)
            part['Content-Disposition'] = f'attachment; filename="{filename}"'
            msg.attach(part)

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
        print(f"Email sent to {recipient}!")
    except Exception as e:
        print(f"Error sending email: {e}")

def main():
    print("Starting Spend Analysis Bot...")
    
    # 1. Fetch Data
    brex_df = fetch_brex_transactions(BREX_API_KEY)
    mercury_df = fetch_mercury_transactions(MERCURY_API_KEY)
    rippling_df = fetch_rippling_expenses(RIPPLING_API_KEY)
    
    # 2. Unify Data
    # Ensure all DFs have: Date, Description, Amount, Category, Source
    for df, name in [(brex_df, 'Brex'), (mercury_df, 'Mercury'), (rippling_df, 'Rippling')]:
        if not df.empty and 'Source' not in df.columns:
            df['Source'] = name
            
    unified_df = pd.concat([brex_df, mercury_df, rippling_df], ignore_index=True)
    
    if unified_df.empty:
        print("No data fetched from any source. Check API keys.")
        return
        
    # 3. Analyze
    report_html = generate_spend_report(unified_df)
    
    # 4. Attachments
    csv_io = pd.io.common.BytesIO()
    unified_df.to_csv(csv_io, index=False)
    csv_bytes = csv_io.getvalue()
    
    # 5. Send
    send_email(report_html, REPORT_RECIPIENT, attachments=[("unified_spend.csv", csv_bytes)])

if __name__ == "__main__":
    main()
