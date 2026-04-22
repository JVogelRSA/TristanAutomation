import os
import io
from datetime import datetime, timedelta
import pandas as pd
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from anthropic import Anthropic
from dotenv import load_dotenv

# Shared utilities
from utils.email_sender import send_report_email
from utils.docx_generator import html_to_docx
from utils.history import get_week_monday, save_weekly_snapshot, load_history

# Load environment variables from the .env file next to this script
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'), override=True)

# Configuration
REPORT_RECIPIENT = os.getenv("REPORT_RECIPIENT")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# Snowflake Configuration
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")


def load_private_key(path):
    """Load and parse the RSA private key for Snowflake key-pair auth."""
    with open(path, 'rb') as f:
        private_key = serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )


def connect_snowflake():
    """Connect to Snowflake, preferring key-pair auth over password."""
    connect_args = dict(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        login_timeout=30,
        network_timeout=60,
    )

    if SNOWFLAKE_PRIVATE_KEY_PATH and os.path.exists(SNOWFLAKE_PRIVATE_KEY_PATH):
        print(f"Snowflake: Using key-pair auth ({SNOWFLAKE_PRIVATE_KEY_PATH})")
        connect_args['private_key'] = load_private_key(SNOWFLAKE_PRIVATE_KEY_PATH)
    elif SNOWFLAKE_PASSWORD:
        print("Snowflake: Using password auth")
        connect_args['password'] = SNOWFLAKE_PASSWORD
    else:
        raise ValueError("No Snowflake credentials: set SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD")

    return snowflake.connector.connect(**connect_args)


def get_sales_query(target_monday):
    """Build the weekly sales SQL with proper order-level tax/shipping aggregation."""
    return f"""
    SET target_monday = '{target_monday}'::DATE;
    SET week1_start = $target_monday;
    SET week1_end   = DATEADD('day', 6, $week1_start);
    SET week2_start = DATEADD('day', -7, $week1_start);
    SET week2_end   = DATEADD('day', -1, $week1_start);

    WITH
    -- Week 1: aggregate per order first to correctly sum taxes/shipping
    w1_orders AS (
        SELECT NAME,
            SUM(CASE WHEN lineitem_sku IN ('1','6','6-k','100','200','300','301','400','401','7','303')
                THEN lineitem_price * lineitem_quantity ELSE 0 END) AS line_dc1,
            SUM(CASE WHEN lineitem_sku IN ('1','6','6-k','100','200','300','301','400','401','7','302','303','21','22','23','25','26','28','29','30','31','32','33','34','35','36','37','38','5000')
                THEN lineitem_price * lineitem_quantity ELSE 0 END) AS line_all,
            SUM(CASE WHEN lineitem_sku IN ('7','400','401')
                THEN lineitem_price * lineitem_quantity ELSE 0 END) AS line_kids,
            SUM(CASE WHEN lineitem_sku IN ('7','400','401') THEN lineitem_quantity ELSE 0 END) AS kids_units,
            SUM(CASE WHEN lineitem_sku IN ('1','6','100','200','300','301','303','400','401','7','302')
                THEN lineitem_quantity ELSE 0 END) AS gross_units,
            SUM(CASE WHEN lineitem_sku IN ('1','6','6-k','100','200','300','301','400','401','7','303') AND cancelled_at IS NULL
                THEN lineitem_price * lineitem_quantity ELSE 0 END) AS line_dc1_net,
            SUM(CASE WHEN cancelled_at IS NOT NULL THEN lineitem_quantity ELSE 0 END) AS cancelled_units,
            SUM(discount_amount) AS order_discounts,
            MAX(taxes) AS order_taxes,
            MAX(shipping) AS order_shipping
        FROM DAYLIGHT_SALES.CONNECTORS.SHOPIFY
        WHERE created_at::DATE BETWEEN $week1_start AND $week1_end
        GROUP BY NAME
    ),
    w1 AS (
        SELECT
            SUM(line_dc1) + SUM(order_taxes) + SUM(order_shipping) AS gross_sales_dc1,
            SUM(line_all) + SUM(order_taxes) + SUM(order_shipping) AS gross_sales_all,
            SUM(line_kids) AS kids_rev,
            SUM(kids_units) AS kids_units,
            SUM(gross_units) AS gross_units,
            SUM(cancelled_units) AS cancelled_units,
            COUNT(*) AS order_count,
            SUM(order_discounts) AS discounts,
            SUM(line_dc1_net) - SUM(CASE WHEN cancelled_units = 0 THEN order_discounts ELSE 0 END) AS net_sales_dc1
        FROM w1_orders
    ),

    -- Week 2: same order-level aggregation
    w2_orders AS (
        SELECT NAME,
            SUM(CASE WHEN lineitem_sku IN ('1','6','6-k','100','200','300','301','400','401','7','303')
                THEN lineitem_price * lineitem_quantity ELSE 0 END) AS line_dc1,
            SUM(CASE WHEN lineitem_sku IN ('1','6','6-k','100','200','300','301','400','401','7','302','303','21','22','23','25','26','28','29','30','31','32','33','34','35','36','37','38','5000')
                THEN lineitem_price * lineitem_quantity ELSE 0 END) AS line_all,
            SUM(CASE WHEN lineitem_sku IN ('7','400','401')
                THEN lineitem_price * lineitem_quantity ELSE 0 END) AS line_kids,
            SUM(CASE WHEN lineitem_sku IN ('7','400','401') THEN lineitem_quantity ELSE 0 END) AS kids_units,
            SUM(CASE WHEN lineitem_sku IN ('1','6','100','200','300','301','303','400','401','7','302')
                THEN lineitem_quantity ELSE 0 END) AS gross_units,
            SUM(CASE WHEN lineitem_sku IN ('1','6','6-k','100','200','300','301','400','401','7','303') AND cancelled_at IS NULL
                THEN lineitem_price * lineitem_quantity ELSE 0 END) AS line_dc1_net,
            SUM(CASE WHEN cancelled_at IS NOT NULL THEN lineitem_quantity ELSE 0 END) AS cancelled_units,
            SUM(discount_amount) AS order_discounts,
            MAX(taxes) AS order_taxes,
            MAX(shipping) AS order_shipping
        FROM DAYLIGHT_SALES.CONNECTORS.SHOPIFY
        WHERE created_at::DATE BETWEEN $week2_start AND $week2_end
        GROUP BY NAME
    ),
    w2 AS (
        SELECT
            SUM(line_dc1) + SUM(order_taxes) + SUM(order_shipping) AS gross_sales_dc1,
            SUM(line_all) + SUM(order_taxes) + SUM(order_shipping) AS gross_sales_all,
            SUM(line_kids) AS kids_rev,
            SUM(kids_units) AS kids_units,
            SUM(gross_units) AS gross_units,
            SUM(cancelled_units) AS cancelled_units,
            COUNT(*) AS order_count,
            SUM(order_discounts) AS discounts,
            SUM(line_dc1_net) - SUM(CASE WHEN cancelled_units = 0 THEN order_discounts ELSE 0 END) AS net_sales_dc1
        FROM w2_orders
    )

    SELECT 'Report: ' || $week1_start || ' vs ' || $week2_start AS metric,
           'Week 1 (Target)' AS week_1, 'Week 2 (Comp)' AS week_2, '% Change' AS pct_change

    UNION ALL SELECT '=============== SALES ===============', '', '', ''

    UNION ALL
    SELECT 'Gross Sales DC-1',
        TO_VARCHAR(w1.gross_sales_dc1, '$999,999,999'),
        TO_VARCHAR(w2.gross_sales_dc1, '$999,999,999'),
        TO_VARCHAR(ROUND((w1.gross_sales_dc1 - w2.gross_sales_dc1) / NULLIF(w2.gross_sales_dc1, 0) * 100, 1)) || '%'
    FROM w1, w2

    UNION ALL
    SELECT 'Gross Sales All Products',
        TO_VARCHAR(w1.gross_sales_all, '$999,999,999'),
        TO_VARCHAR(w2.gross_sales_all, '$999,999,999'),
        TO_VARCHAR(ROUND((w1.gross_sales_all - w2.gross_sales_all) / NULLIF(w2.gross_sales_all, 0) * 100, 1)) || '%'
    FROM w1, w2

    UNION ALL
    SELECT 'Average Daily Sales (DC-1)',
        TO_VARCHAR(w1.gross_sales_dc1 / 7, '$999,999,999'),
        TO_VARCHAR(w2.gross_sales_dc1 / 7, '$999,999,999'),
        TO_VARCHAR(ROUND(((w1.gross_sales_dc1 / 7) - (w2.gross_sales_dc1 / 7)) / NULLIF(w2.gross_sales_dc1 / 7, 0) * 100, 1)) || '%'
    FROM w1, w2

    UNION ALL
    SELECT 'Net Sales DC-1 (- canc, disc)',
        TO_VARCHAR(w1.net_sales_dc1, '$999,999,999'),
        TO_VARCHAR(w2.net_sales_dc1, '$999,999,999'),
        TO_VARCHAR(ROUND((w1.net_sales_dc1 - w2.net_sales_dc1) / NULLIF(w2.net_sales_dc1, 0) * 100, 1)) || '%'
    FROM w1, w2

    UNION ALL
    SELECT 'Total Discounts',
        TO_VARCHAR(w1.discounts, '$999,999,999'),
        TO_VARCHAR(w2.discounts, '$999,999,999'),
        TO_VARCHAR(ROUND((w1.discounts - w2.discounts) / NULLIF(w2.discounts, 0) * 100, 1)) || '%'
    FROM w1, w2

    UNION ALL
    SELECT 'Discount Rate %',
        TO_VARCHAR(ROUND(w1.discounts / NULLIF(w1.gross_sales_dc1, 0) * 100, 2)) || '%',
        TO_VARCHAR(ROUND(w2.discounts / NULLIF(w2.gross_sales_dc1, 0) * 100, 2)) || '%',
        TO_VARCHAR(ROUND((w1.discounts / NULLIF(w1.gross_sales_dc1, 0) * 100) - (w2.discounts / NULLIF(w2.gross_sales_dc1, 0) * 100), 2)) || ' pts'
    FROM w1, w2

    UNION ALL SELECT '=============== KIDS ===============', '', '', ''

    UNION ALL
    SELECT 'Kids Revenue',
        TO_VARCHAR(w1.kids_rev, '$999,999,999'),
        TO_VARCHAR(w2.kids_rev, '$999,999,999'),
        TO_VARCHAR(ROUND((w1.kids_rev - w2.kids_rev) / NULLIF(w2.kids_rev, 0) * 100, 1)) || '%'
    FROM w1, w2

    UNION ALL
    SELECT 'Kids Units Sold',
        TO_VARCHAR(w1.kids_units), TO_VARCHAR(w2.kids_units),
        TO_VARCHAR(ROUND((w1.kids_units - w2.kids_units) / NULLIF(w2.kids_units, 0) * 100, 1)) || '%'
    FROM w1, w2

    UNION ALL
    SELECT 'Kids % of Total Revenue',
        TO_VARCHAR(ROUND(w1.kids_rev / NULLIF(w1.gross_sales_dc1, 0) * 100, 1)) || '%',
        TO_VARCHAR(ROUND(w2.kids_rev / NULLIF(w2.gross_sales_dc1, 0) * 100, 1)) || '%',
        TO_VARCHAR(ROUND((w1.kids_rev / NULLIF(w1.gross_sales_dc1, 0) * 100) - (w2.kids_rev / NULLIF(w2.gross_sales_dc1, 0) * 100), 1)) || ' pts'
    FROM w1, w2

    UNION ALL
    SELECT 'Kids % of Total Units',
        TO_VARCHAR(ROUND(w1.kids_units / NULLIF(w1.gross_units, 0) * 100, 1)) || '%',
        TO_VARCHAR(ROUND(w2.kids_units / NULLIF(w2.gross_units, 0) * 100, 1)) || '%',
        TO_VARCHAR(ROUND((w1.kids_units / NULLIF(w1.gross_units, 0) * 100) - (w2.kids_units / NULLIF(w2.gross_units, 0) * 100), 1)) || ' pts'
    FROM w1, w2

    UNION ALL SELECT '=============== UNITS ===============', '', '', ''

    UNION ALL
    SELECT 'Order Count', TO_VARCHAR(w1.order_count), TO_VARCHAR(w2.order_count),
        TO_VARCHAR(ROUND((w1.order_count - w2.order_count) / NULLIF(w2.order_count, 0) * 100, 1)) || '%'
    FROM w1, w2

    UNION ALL
    SELECT 'Total Units Sold', TO_VARCHAR(w1.gross_units), TO_VARCHAR(w2.gross_units),
        TO_VARCHAR(ROUND((w1.gross_units - w2.gross_units) / NULLIF(w2.gross_units, 0) * 100, 1)) || '%'
    FROM w1, w2

    UNION ALL
    SELECT 'Cancelled Units', TO_VARCHAR(w1.cancelled_units), TO_VARCHAR(w2.cancelled_units),
        TO_VARCHAR(ROUND((w1.cancelled_units - w2.cancelled_units) / NULLIF(w2.cancelled_units, 0) * 100, 1)) || '%'
    FROM w1, w2

    UNION ALL SELECT '=============== CALCULATED ===============', '', '', ''

    UNION ALL
    SELECT 'AOV (DC-1)',
        TO_VARCHAR(ROUND(w1.gross_sales_dc1 / NULLIF(w1.order_count, 0), 2), '$999,999'),
        TO_VARCHAR(ROUND(w2.gross_sales_dc1 / NULLIF(w2.order_count, 0), 2), '$999,999'),
        TO_VARCHAR(ROUND(((w1.gross_sales_dc1 / NULLIF(w1.order_count, 0)) - (w2.gross_sales_dc1 / NULLIF(w2.order_count, 0))) / NULLIF(w2.gross_sales_dc1 / NULLIF(w2.order_count, 0), 0) * 100, 1)) || '%'
    FROM w1, w2

    UNION ALL
    SELECT 'Revenue per Unit',
        TO_VARCHAR(ROUND(w1.gross_sales_dc1 / NULLIF(w1.gross_units, 0), 2), '$999,999'),
        TO_VARCHAR(ROUND(w2.gross_sales_dc1 / NULLIF(w2.gross_units, 0), 2), '$999,999'),
        TO_VARCHAR(ROUND(((w1.gross_sales_dc1 / NULLIF(w1.gross_units, 0)) - (w2.gross_sales_dc1 / NULLIF(w2.gross_units, 0))) / NULLIF(w2.gross_sales_dc1 / NULLIF(w2.gross_units, 0), 0) * 100, 1)) || '%'
    FROM w1, w2

    UNION ALL
    SELECT 'Cancellation Rate %',
        TO_VARCHAR(ROUND(w1.cancelled_units / NULLIF(w1.gross_units, 0) * 100, 2)) || '%',
        TO_VARCHAR(ROUND(w2.cancelled_units / NULLIF(w2.gross_units, 0) * 100, 2)) || '%',
        TO_VARCHAR(ROUND((w1.cancelled_units / NULLIF(w1.gross_units, 0) * 100) - (w2.cancelled_units / NULLIF(w2.gross_units, 0) * 100), 2)) || ' pts'
    FROM w1, w2;
    """


def get_daily_breakdown_query(target_monday):
    """Daily gross sales (DC-1) for the target week — for intra-week analysis."""
    return f"""
    WITH daily AS (
        SELECT
            created_at::DATE AS day,
            NAME,
            SUM(CASE WHEN lineitem_sku IN ('1','6','6-k','100','200','300','301','400','401','7','303')
                THEN lineitem_price * lineitem_quantity ELSE 0 END) AS line_dc1,
            MAX(taxes) AS order_taxes,
            MAX(shipping) AS order_shipping
        FROM DAYLIGHT_SALES.CONNECTORS.SHOPIFY
        WHERE created_at::DATE BETWEEN '{target_monday}'::DATE AND DATEADD('day', 6, '{target_monday}'::DATE)
        GROUP BY day, NAME
    )
    SELECT
        TO_CHAR(day, 'YYYY-MM-DD (Dy)') AS day,
        TO_VARCHAR(SUM(line_dc1) + SUM(order_taxes) + SUM(order_shipping), '$999,999,999') AS gross_sales_dc1,
        COUNT(DISTINCT NAME) AS orders
    FROM daily
    GROUP BY day
    ORDER BY day;
    """


def fetch_sales_data():
    """Connect to Snowflake and run both the weekly summary query and the daily breakdown."""
    if not SNOWFLAKE_USER or not SNOWFLAKE_ACCOUNT:
        print("Snowflake: Missing credentials.")
        return pd.DataFrame(), pd.DataFrame()

    print("Connecting to Snowflake...")
    try:
        ctx = connect_snowflake()
        cur = ctx.cursor()

        # When running on Monday, we want LAST week's data (the completed week)
        # not the current week which just started today
        target_monday = (get_week_monday() - timedelta(days=7)).isoformat()
        print(f"Executing SQL for week starting: {target_monday}")

        # --- Weekly summary ----------------------------------
        sql_script = get_sales_query(target_monday)
        commands = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip()]

        results_df = pd.DataFrame()
        for i, cmd in enumerate(commands):
            cur.execute(cmd)
            if i == len(commands) - 1:
                results_df = cur.fetch_pandas_all()
        print(f"Fetched {len(results_df)} weekly summary rows.")

        # --- Daily breakdown ---------------------------------
        cur.execute(get_daily_breakdown_query(target_monday))
        daily_df = cur.fetch_pandas_all()
        print(f"Fetched {len(daily_df)} daily rows.")

        cur.close()
        ctx.close()
        return results_df, daily_df

    except Exception as e:
        print(f"Snowflake Error: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), pd.DataFrame()


def build_sales_comparison(history, current_metrics):
    """Build historical comparison string for the LLM prompt."""
    if not history:
        return ""

    lines = []

    # Rolling 4-week average revenue
    recent = [h for h in history[-4:] if "gross_sales_dc1" in h]
    if recent:
        avg = sum(h["gross_sales_dc1"] for h in recent) / len(recent)
        curr = current_metrics.get("gross_sales_dc1", 0)
        if avg > 0:
            pct = ((curr - avg) / avg) * 100
            lines.append(f"Rolling {len(recent)}-Week Avg Revenue (DC-1): ${avg:,.0f} (this week is {pct:+.1f}% vs avg)")

    # Same week last month (~4 weeks ago)
    if len(history) >= 4 and "gross_sales_dc1" in history[-4]:
        prev = history[-4]["gross_sales_dc1"]
        curr = current_metrics.get("gross_sales_dc1", 0)
        if prev > 0:
            pct = ((curr - prev) / prev) * 100
            lines.append(f"Same Week Last Month: ${prev:,.0f} ({pct:+.1f}% change)")

    # Unit trend
    recent_units = [h["gross_units"] for h in history[-4:] if "gross_units" in h]
    if len(recent_units) >= 3:
        direction = "increasing" if recent_units[-1] > recent_units[0] else "decreasing"
        lines.append(f"4-Week Unit Trend: {direction} ({recent_units[0]} → {recent_units[-1]} units/wk)")

    # Kids revenue trend
    recent_kids = [h["kids_rev"] for h in history[-4:] if "kids_rev" in h]
    if len(recent_kids) >= 2:
        avg_kids = sum(recent_kids) / len(recent_kids)
        curr_kids = current_metrics.get("kids_rev", 0)
        if avg_kids > 0:
            pct = ((curr_kids - avg_kids) / avg_kids) * 100
            lines.append(f"Kids Revenue vs {len(recent_kids)}-wk avg: {pct:+.1f}%")

    if not lines:
        return ""
    return "--- HISTORICAL COMPARISON ---\n" + "\n".join(lines)


def parse_metrics_from_results(df):
    """Extract numeric metrics from the Snowflake result table for snapshot storage."""
    metrics = {}
    for _, row in df.iterrows():
        metric_name = str(row.iloc[0]).strip()
        week1_val = str(row.iloc[1]).strip()

        # Parse dollar values
        clean = week1_val.replace('$', '').replace(',', '').replace('%', '').strip()
        try:
            val = float(clean)
        except (ValueError, TypeError):
            continue

        key_map = {
            'Gross Sales DC-1': 'gross_sales_dc1',
            'Gross Sales All Products': 'gross_sales_all',
            'Net Sales DC-1 (- canc, disc)': 'net_sales_dc1',
            'Kids Revenue': 'kids_rev',
            'Total Units Sold': 'gross_units',
            'Kids Units Sold': 'kids_units',
            'Cancelled Units': 'cancelled_units',
            'Order Count': 'order_count',
            'Total Discounts': 'discounts',
        }
        if metric_name in key_map:
            metrics[key_map[metric_name]] = val

    return metrics


def generate_sales_report(df, daily_df=None, history=None):
    """Use LLM to analyze the sales data and produce an executive report."""
    print("Generating Sales Report with LLM...")

    if df.empty:
        return "<p>No sales data found for this week.</p>", {}

    summary_text = df.to_string(index=False)
    current_metrics = parse_metrics_from_results(df)
    hist_comparison = build_sales_comparison(history or [], current_metrics)

    # Daily breakdown inside the week — deterministic, pass verbatim to LLM
    if daily_df is not None and not daily_df.empty:
        daily_block = (
            "--- DAILY BREAKDOWN (TARGET WEEK) ---\n"
            f"{daily_df.to_string(index=False)}\n"
        )
    else:
        daily_block = ""

    client = Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt = f"""
You are a VP of Sales at Daylight Computer, a hardware company that makes the DC-1 tablet and kids versions.
Analyse the following weekly sales data and produce a Weekly Sales Report for the CEO.

DATA:
{summary_text}

{hist_comparison}

{daily_block}

--------------------------------------------------

Output ONLY valid HTML — no markdown, no code fences, no ** bold syntax (use <b> tags), no intro or closing text.
Start directly with the first <h3> tag.

REQUIRED SECTIONS:

<h3>1. Revenue Snapshot</h3>
Compact 2-column summary table (Metric | Value):
  - Gross Sales DC-1 (this week + WoW % change)
  - Gross Sales All Products (this week + WoW % change)
  - Average Daily Sales (DC-1)
  - Order Count + AOV
  - Total Discounts
  - Rolling 4-week avg (if historical data available)
Then a single <p><b>Sales Insight:</b> …</p> — one bold sentence assessing the week's sales health.

<h3>2. Kids Revenue Deep Dive</h3>
  - Kids revenue this week vs last week (absolute + %)
  - Kids as % of total revenue (this week vs last week)
  - Kids units sold
  - One sentence on whether the kids line is gaining or losing share.

<h3>3. Unit Economics</h3>
  - Total units sold (WoW change)
  - Kids units vs adult units breakdown
  - Average order value trend
  - Discount impact (total discounts as % of gross sales)

<h3>4. Daily Breakdown (What Drove the Week)</h3>
Use the DAILY BREAKDOWN block verbatim — these are deterministic, not your inference.
Render an HTML table: Day | Gross Sales (DC-1) | Orders.
Then a one-sentence call-out naming the day(s) that drove the week (best day + worst day) and whether
the pattern looks like a weekday-skewed week, a weekend-skewed week, or a single-day spike.
If the DAILY BREAKDOWN block is empty, write "Daily data unavailable this week."

<h3>5. Trends & Outlook</h3>
If historical data is provided:
  - Is revenue trending up or down over the last 4 weeks?
  - Are kids sales accelerating or decelerating?
  - Any notable patterns or seasonality signals?
If no historical data, note this is the first week and trends will appear in future reports.

<h3>6. Key Takeaway</h3>
2-3 bullet points: the single most important insight, one risk to watch, one opportunity.

FORMAT RULES:
- Valid HTML only. Use <b> not **bold**.
- Tables must have <thead><tr><th> headers.
- Currency formatted as $X,XXX throughout.
- Keep concise — must fit 1-2 printed pages.
"""

    try:
        response = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=4096,
            temperature=0,
            system="You are a sharp, data-driven VP of Sales. You focus on actionable insights, revenue trends, and growth opportunities. Be direct and specific — no fluff.",
            messages=[{"role": "user", "content": prompt}]
        )
        report_html = response.content[0].text
        return report_html, current_metrics
    except Exception as e:
        print(f"Error generating LLM report: {e}")
        import traceback
        traceback.print_exc()
        return f"<p><b>Error generating report:</b> {e}</p>", current_metrics


def main():
    print("Starting Sales Summary Bot...")

    if not ANTHROPIC_API_KEY:
        print("Error: ANTHROPIC_API_KEY not set.")
        return
    if not REPORT_RECIPIENT:
        print("Error: REPORT_RECIPIENT not set.")
        return

    # 1. Fetch data from Snowflake (weekly summary + daily breakdown)
    df, daily_df = fetch_sales_data()
    if df.empty:
        print("No data or error in query.")
        return

    # 2. Load history and generate report
    history = load_history("sales")
    week_monday = get_week_monday()
    report_html, metrics = generate_sales_report(df, daily_df=daily_df, history=history)

    # 3. Save snapshot for future comparisons
    if metrics:
        save_weekly_snapshot("sales", week_monday, metrics)

    # 4. Build attachments
    date_str = week_monday.isoformat()

    docx_bytes = html_to_docx(report_html, "Weekly Sales Summary", date_str)

    csv_io = io.BytesIO()
    df.to_csv(csv_io, index=False)

    daily_csv_io = io.BytesIO()
    if daily_df is not None and not daily_df.empty:
        daily_df.to_csv(daily_csv_io, index=False)

    attachments = [
        (f"weekly_sales_report_{date_str}.docx", docx_bytes),
        ("sales_summary.csv", csv_io.getvalue()),
    ]
    if daily_csv_io.getvalue():
        attachments.append(("sales_daily_breakdown.csv", daily_csv_io.getvalue()))

    # 5. Send
    send_report_email(
        subject=f"Weekly Sales Summary - {date_str}",
        body_text="Your weekly sales report is attached.",
        recipient=REPORT_RECIPIENT,
        attachments=attachments,
    )


if __name__ == "__main__":
    main()
