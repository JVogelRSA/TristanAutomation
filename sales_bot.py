import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from datetime import datetime, timedelta
import pandas as pd
import snowflake.connector
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
REPORT_RECIPIENT = os.getenv("REPORT_RECIPIENT")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Snowflake Configuration
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

def get_last_monday():
    """Returns the date of the most recent Monday."""
    today = datetime.now().date()
    # Monday is 0 and Sunday is 6
    # If today is Monday(0), we want today. If today is Tuesday(1), we want yesterday.
    days_to_subtract = today.weekday()
    last_monday = today - timedelta(days=days_to_subtract)
    return last_monday.strftime('%Y-%m-%d')

# SQL Query (Injected with dynamic date)
def get_sales_query(target_monday):
    return f"""
    -- ============================================================
    -- DAYLIGHT WEEKLY SALES SUMMARY
    -- ============================================================
    -- Target Monday is provided dynamically: '{target_monday}'

    -- VARIABLES
    SET target_monday = '{target_monday}'::DATE; 
    SET week1_start = $target_monday;
    SET week1_end   = DATEADD('day', 6, $week1_start);
    SET week2_start = DATEADD('day', -7, $week1_start);
    SET week2_end   = DATEADD('day', -1, $week1_start);

    WITH 
    w1 AS (
        SELECT
            SUM(CASE WHEN lineitem_sku IN ('1','6','6-k','100','200','300','301','400','401','7','303') 
                THEN (lineitem_price * lineitem_quantity) ELSE 0 END) AS gross_sales_dc1,
            SUM(CASE WHEN lineitem_sku IN ('7','400','401') THEN (lineitem_price * lineitem_quantity) ELSE 0 END) AS kids_rev,
            SUM(CASE WHEN lineitem_sku IN ('7','400','401') THEN lineitem_quantity ELSE 0 END) AS kids_units,
            SUM(CASE WHEN lineitem_sku IN ('1','6','100','200','300','301','303','400','401','7','302') THEN lineitem_quantity ELSE 0 END) AS gross_units
        FROM DAYLIGHT_SALES.CONNECTORS.SHOPIFY
        WHERE created_at::DATE BETWEEN $week1_start AND $week1_end
    ),
    w2 AS (
        SELECT
            SUM(CASE WHEN lineitem_sku IN ('1','6','6-k','100','200','300','301','400','401','7','303') 
                THEN (lineitem_price * lineitem_quantity) ELSE 0 END) AS gross_sales_dc1,
            SUM(CASE WHEN lineitem_sku IN ('7','400','401') THEN (lineitem_price * lineitem_quantity) ELSE 0 END) AS kids_rev,
            SUM(CASE WHEN lineitem_sku IN ('7','400','401') THEN lineitem_quantity ELSE 0 END) AS kids_units,
            SUM(CASE WHEN lineitem_sku IN ('1','6','100','200','300','301','303','400','401','7','302') THEN lineitem_quantity ELSE 0 END) AS gross_units
        FROM DAYLIGHT_SALES.CONNECTORS.SHOPIFY
        WHERE created_at::DATE BETWEEN $week2_start AND $week2_end
    )

    -- FINAL OUTPUT TABLE
    SELECT 'DATE VERIFICATION' AS metric, 'START DATE' AS week_1, 'END DATE' AS week_2, 'RANGE' AS pct_change
    UNION ALL
    SELECT 'Target Week (W1)', $week1_start::VARCHAR, $week1_end::VARCHAR, '7 Days'
    UNION ALL
    SELECT 'Comp Week (W2)', $week2_start::VARCHAR, $week2_end::VARCHAR, '7 Days'

    UNION ALL SELECT '=============== SALES ===============', '', '', ''

    UNION ALL
    SELECT 
        'Gross Sales DC-1',
        TO_VARCHAR(w1.gross_sales_dc1, '$999,999,999'),
        TO_VARCHAR(w2.gross_sales_dc1, '$999,999,999'),
        TO_VARCHAR(ROUND((w1.gross_sales_dc1 - w2.gross_sales_dc1) / NULLIF(w2.gross_sales_dc1, 0) * 100, 1)) || '%'
    FROM w1, w2

    UNION ALL
    SELECT 
        'Average Daily Sales',
        TO_VARCHAR(w1.gross_sales_dc1 / 7, '$999,999,999'),
        TO_VARCHAR(w2.gross_sales_dc1 / 7, '$999,999,999'),
        TO_VARCHAR(ROUND(((w1.gross_sales_dc1 / 7) - (w2.gross_sales_dc1 / 7)) / NULLIF(w2.gross_sales_dc1 / 7, 0) * 100, 1)) || '%'
    FROM w1, w2

    UNION ALL
    SELECT 
        'Kids Revenue',
        TO_VARCHAR(w1.kids_rev, '$999,999,999'),
        TO_VARCHAR(w2.kids_rev, '$999,999,999'),
        TO_VARCHAR(ROUND((w1.kids_rev - w2.kids_rev) / NULLIF(w2.kids_rev, 0) * 100, 1)) || '%'
    FROM w1, w2

    UNION ALL
    SELECT 
        'Kids % of Total Revenue',
        TO_VARCHAR(ROUND(w1.kids_rev / NULLIF(w1.gross_sales_dc1, 0) * 100, 1)) || '%',
        TO_VARCHAR(ROUND(w2.kids_rev / NULLIF(w2.gross_sales_dc1, 0) * 100, 1)) || '%',
        TO_VARCHAR(ROUND((w1.kids_rev / NULLIF(w1.gross_sales_dc1, 0) * 100) - (w2.kids_rev / NULLIF(w2.gross_sales_dc1, 0) * 100), 1)) || ' pts'
    FROM w1, w2

    UNION ALL SELECT '=============== UNITS ===============', '', '', ''

    UNION ALL
    SELECT 
        'Total Units Sold',
        TO_VARCHAR(w1.gross_units),
        TO_VARCHAR(w2.gross_units),
        TO_VARCHAR(ROUND((w1.gross_units - w2.gross_units) / NULLIF(w2.gross_units, 0) * 100, 1)) || '%'
    FROM w1, w2

    UNION ALL
    SELECT 
        'Kids Units Sold',
        TO_VARCHAR(w1.kids_units),
        TO_VARCHAR(w2.kids_units),
        TO_VARCHAR(ROUND((w1.kids_units - w2.kids_units) / NULLIF(w2.kids_units, 0) * 100, 1)) || '%'
    FROM w1, w2;
    """

def fetch_sales_data():
    """
    Connects to Snowflake and executes the sales query.
    Note: Snowflake python connector executes statements one by one.
    To use variables (SET ...), we need to ensure session state is maintained or execute as a script.
    Usually we can just execute the whole block if using execute_string or splitting.
    Or more simply, we can replace the variables in Python before sending (which we are effectively doing manually, 
    but for the SQL Variables to work we need to execute sequentially).
    """
    if not SNOWFLAKE_USER or not SNOWFLAKE_PASSWORD or not SNOWFLAKE_ACCOUNT:
        print("Snowflake: No credentials provided.")
        return pd.DataFrame()

    print("Connecting to Snowflake...")
    try:
        ctx = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cur = ctx.cursor()
        
        target_monday = get_last_monday()
        print(f"Executing SQL query for week starting: {target_monday}")
        
        # We need to execute the SET commands first, then the SELECT.
        # However, the user provided one large block.
        # Snowflake Connector `execute_string` can handle multiple statements.
        # Alternatively, we can just split by semicolon if simple, but `SET` persists in the session.
        
        sql_script = get_sales_query(target_monday)
        
        # Execute commands sequentially to ensure variables are set
        commands = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip()]
        
        results_df = pd.DataFrame()
        
        for i, cmd in enumerate(commands):
            if not cmd: continue
            
            # print(f"Executing command {i}...")
            cur.execute(cmd)
            
            # If it's the final SELECT, fetch results
            if i == len(commands) - 1:
                results_df = cur.fetch_pandas_all()

        cur.close()
        ctx.close()
        
        print(f"Fetched {len(results_df)} rows.")
        return results_df
        
    except Exception as e:
        print(f"Snowflake Error: {e}")
        return pd.DataFrame()

def generate_sales_report(df):
    """
    Uses LLM to analyze the sales data.
    """
    print("Generating Sales Report with LLM...")
    
    if df.empty:
        return "<p>No sales data found for this week.</p>"
        
    summary_text = df.to_string(index=False)
    
    client = OpenAI(api_key=OPENAI_API_KEY)
    
    prompt = f"""
    You are a VP of Sales.
    Analyze the following weekly sales data report:
    
    {summary_text}
    
    Write a Weekly Sales Update email.
    1. **Revenue Snapshot**: value and WoW trend.
    2. **Deep Dive**: Kids Revenue vs Total.
    3. **Units**: How many units moved?
    4. **Insight**: One key takeaway from this data.
    
    **Format as HTML**. Use <h2> for sections, tables for data.
    Keep it professional and concise.
    """
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a helpful sales analyst."},
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
    msg['Subject'] = f"Weekly Sales Summary - {datetime.now().strftime('%Y-%m-%d')}"
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
    print("Starting Sales Summary Bot...")
    
    df = fetch_sales_data()
    
    if df.empty:
        print("No data or error in query.")
        return
        
    report_html = generate_sales_report(df)
    
    # Attachments
    csv_io = pd.io.common.BytesIO()
    df.to_csv(csv_io, index=False)
    csv_bytes = csv_io.getvalue()
    
    send_email(report_html, REPORT_RECIPIENT, attachments=[("sales_summary.csv", csv_bytes)])

if __name__ == "__main__":
    main()
