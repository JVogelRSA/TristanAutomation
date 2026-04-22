"""
Unified weekly report orchestrator.

Runs all three bots (sales, spend, inventory), builds a single polished HTML
email combining every section + chart + attachment, and ships one email.

This is meant to replace the three separate emails that used to come from
spend_bot.py / sales_bot.py / inventory_bot.py individually. Those scripts
still run standalone if you need to (they each keep their own main()); this
orchestrator reuses their data-gathering and report-generation functions
without calling their solo send_report_email() path.
"""
from __future__ import annotations

import io
import os
from datetime import timedelta

import pandas as pd
from dotenv import load_dotenv

from utils.history import (
    get_week_monday,
    save_weekly_snapshot,
    load_history,
)
from utils.docx_generator import html_to_docx
from utils.unified_email import compose_weekly_email, send_unified_email

# Import each bot's building blocks (reused, not their main())
from spend_bot import (
    generate_spend_report,
    CASH_BALANCE_USD,
)
from adapters.brex import fetch_brex_transactions
from adapters.mercury import fetch_mercury_transactions
from adapters.rippling import fetch_rippling_expenses

from sales_bot import (
    fetch_sales_data,
    generate_sales_report,
    generate_revenue_sparkline,
    parse_metrics_from_results,
)

from inventory_bot import (
    fetch_latest_emails,
    generate_llm_report as generate_inventory_report,
    generate_runway_chart,
)


load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"), override=True)

REPORT_RECIPIENT = os.getenv("REPORT_RECIPIENT")

BREX_API_KEY = os.getenv("BREX_API_KEY")
MERCURY_API_KEY = os.getenv("MERCURY_API_KEY")
RIPPLING_API_KEY = os.getenv("RIPPLING_API_KEY")


# ── Per-bot orchestration steps ────────────────────────────────────────────
def build_spend_section():
    """Run spend bot data pipeline; return dict + attachments list."""
    print("[spend] fetching Brex / Mercury / Rippling …")
    brex_df = fetch_brex_transactions(BREX_API_KEY, days_back=30)
    mercury_df = fetch_mercury_transactions(MERCURY_API_KEY)
    rippling_df = fetch_rippling_expenses(RIPPLING_API_KEY)

    for df, name in [(brex_df, "Brex"), (mercury_df, "Mercury"), (rippling_df, "Rippling")]:
        if not df.empty and "Source" not in df.columns:
            df["Source"] = name

    unified_df = pd.concat([brex_df, mercury_df, rippling_df], ignore_index=True)
    if unified_df.empty:
        print("[spend] no data from any source — skipping spend section.")
        return {
            "html": "<p><i>No spend data returned from any connected source (Brex / Mercury / Rippling).</i></p>",
            "headline": {},
            "attachments": [],
            "attachment_names": [],
            "snapshot": {},
        }

    history = load_history("spend")
    report_html, curr_df, snapshot = generate_spend_report(unified_df, history=history)

    # Headline for KPI strip
    headline = {
        "total_spend": snapshot.get("total_spend"),
        "prev_week_spend": snapshot.get("prev_week_spend"),
        "runway": snapshot.get("runway") or {},
    }

    # Attachments
    week_monday = get_week_monday()
    date_str = week_monday.isoformat()

    docx_bytes = html_to_docx(report_html, "Weekly Spend Analysis", date_str)

    curr_csv = io.BytesIO()
    curr_df.to_csv(curr_csv, index=False)

    full_csv = io.BytesIO()
    unified_df.to_csv(full_csv, index=False)

    attachments = [
        (f"weekly_spend_report_{date_str}.docx", docx_bytes),
        ("analyzed_spend_7_days.csv", curr_csv.getvalue()),
        ("full_30_days_raw.csv", full_csv.getvalue()),
    ]

    return {
        "html": report_html,
        "headline": headline,
        "attachments": attachments,
        "attachment_names": [a[0] for a in attachments],
        "snapshot": snapshot,
    }


def build_sales_section():
    """Run sales bot data pipeline; return dict with html + chart + attachments."""
    print("[sales] querying Snowflake …")
    df, daily_df = fetch_sales_data()
    if df.empty:
        print("[sales] no data — skipping sales section.")
        return {
            "html": "<p><i>No sales data returned from Snowflake this week.</i></p>",
            "headline": {},
            "attachments": [],
            "attachment_names": [],
            "snapshot": {},
            "chart_png": None,
        }

    history = load_history("sales")
    report_html, metrics = generate_sales_report(df, daily_df=daily_df, history=history)

    # Look up previous week's revenue for the KPI pct change
    prev_rev = None
    if history:
        prev = history[-1]
        prev_rev = prev.get("gross_sales_dc1")

    curr_rev = metrics.get("gross_sales_dc1")
    order_count = metrics.get("order_count")
    aov = (curr_rev / order_count) if (curr_rev and order_count) else None

    kids_rev = metrics.get("kids_rev")
    kids_pct_str = "—"
    if kids_rev is not None and curr_rev:
        kids_pct_str = f"{(kids_rev / curr_rev * 100):.1f}%"

    headline = {
        "gross_sales_dc1": curr_rev,
        "prev_gross_sales_dc1": prev_rev,
        "order_count": int(order_count) if order_count else None,
        "aov": aov,
        "kids_rev": kids_rev,
        "kids_pct": kids_pct_str,
    }

    # Chart — 8-week sparkline
    sparkline_png = generate_revenue_sparkline(history, metrics)

    # Attachments
    week_monday = get_week_monday()
    date_str = week_monday.isoformat()

    docx_bytes = html_to_docx(
        report_html, "Weekly Sales Summary", date_str,
        chart_images=[sparkline_png] if sparkline_png else [],
    )

    csv_io = io.BytesIO()
    df.to_csv(csv_io, index=False)

    daily_csv_io = io.BytesIO()
    daily_has = daily_df is not None and not daily_df.empty
    if daily_has:
        daily_df.to_csv(daily_csv_io, index=False)

    attachments = [
        (f"weekly_sales_report_{date_str}.docx", docx_bytes),
        ("sales_summary.csv", csv_io.getvalue()),
    ]
    if daily_has:
        attachments.append(("sales_daily_breakdown.csv", daily_csv_io.getvalue()))

    return {
        "html": report_html,
        "headline": headline,
        "attachments": attachments,
        "attachment_names": [a[0] for a in attachments],
        "snapshot": metrics,
        "chart_png": sparkline_png,
        "chart_cid": "sales_sparkline",
    }


def build_inventory_section():
    """Run inventory bot data pipeline; return dict with html + chart + attachments."""
    print("[inventory] fetching DCL inventory emails …")
    data = fetch_latest_emails(limit=4)
    if len(data) == 0:
        print("[inventory] no emails found — skipping inventory section.")
        return {
            "html": "<p><i>No DCL inventory emails found this week — check IMAP filters.</i></p>",
            "headline": {},
            "attachments": [],
            "attachment_names": [],
            "snapshot": {},
            "chart_png": None,
        }

    history = load_history("inventory")
    report_html, summary_df, snapshot = generate_inventory_report(data, history=history)

    # Headline — count of rows with Reorder in {'OVERDUE','THIS WEEK'}
    critical_count = 0
    critical_products: list[str] = []
    if summary_df is not None and not summary_df.empty and "Reorder" in summary_df.columns:
        crit = summary_df[summary_df["Reorder"].isin(["OVERDUE", "THIS WEEK"])]
        critical_count = len(crit)
        critical_products = crit["Product"].head(3).tolist()

    critical_sub = (
        ", ".join(critical_products)
        if critical_products
        else ("all inventory healthy" if critical_count == 0 else f"{critical_count} SKUs")
    )

    headline = {
        "critical_count": int(critical_count),
        "critical_sub": critical_sub,
    }

    chart_png = generate_runway_chart(summary_df)

    # Attachments
    week_monday = get_week_monday()
    date_str = week_monday.isoformat()

    docx_bytes = html_to_docx(
        report_html, "Weekly Inventory Report", date_str,
        chart_images=[chart_png] if chart_png else [],
    )

    attachments: list[tuple[str, bytes]] = [
        (f"weekly_inventory_report_{date_str}.docx", docx_bytes),
    ]
    summary_csv = io.BytesIO()
    summary_df.to_csv(summary_csv, index=False)
    attachments.append(("inventory_analytical_summary.csv", summary_csv.getvalue()))

    for date, raw_df in data:
        raw_csv = io.BytesIO()
        raw_df.to_csv(raw_csv, index=False)
        attachments.append((f"inventory_raw_{date.strftime('%Y%m%d')}.csv", raw_csv.getvalue()))

    return {
        "html": report_html,
        "headline": headline,
        "attachments": attachments,
        "attachment_names": [a[0] for a in attachments],
        "snapshot": snapshot,
        "chart_png": chart_png,
        "chart_cid": "inventory_runway",
    }


# ── Top-level orchestrator ────────────────────────────────────────────────
def main(test_mode: bool = False) -> None:
    print("=" * 70)
    print(f" Weekly unified report · {get_week_monday().isoformat()}")
    print("=" * 70)

    if not REPORT_RECIPIENT:
        print("Error: REPORT_RECIPIENT not set.")
        return

    week_monday = get_week_monday()
    date_str = week_monday.isoformat()

    # Run each bot section; any single failure shouldn't kill the whole email
    try:
        sales = build_sales_section()
    except Exception as e:
        import traceback; traceback.print_exc()
        sales = {"html": f"<p><b>Sales section failed:</b> {e}</p>", "headline": {}, "attachments": [], "attachment_names": [], "snapshot": {}, "chart_png": None}

    try:
        spend = build_spend_section()
    except Exception as e:
        import traceback; traceback.print_exc()
        spend = {"html": f"<p><b>Spend section failed:</b> {e}</p>", "headline": {}, "attachments": [], "attachment_names": [], "snapshot": {}}

    try:
        inventory = build_inventory_section()
    except Exception as e:
        import traceback; traceback.print_exc()
        inventory = {"html": f"<p><b>Inventory section failed:</b> {e}</p>", "headline": {}, "attachments": [], "attachment_names": [], "snapshot": {}, "chart_png": None}

    # Save snapshots for future runs (only if the section succeeded and has a snapshot)
    if sales.get("snapshot"):
        save_weekly_snapshot("sales", week_monday, sales["snapshot"])
    if spend.get("snapshot"):
        save_weekly_snapshot("spend", week_monday, spend["snapshot"])
    if inventory.get("snapshot"):
        save_weekly_snapshot("inventory", week_monday, inventory["snapshot"])

    # Compose the unified HTML + inline images
    html_body, inline_images = compose_weekly_email(
        week_monday=date_str,
        sales=sales,
        spend=spend,
        inventory=inventory,
    )

    # Gather all file attachments
    file_attachments: list[tuple[str, bytes]] = []
    file_attachments += sales.get("attachments", [])
    file_attachments += spend.get("attachments", [])
    file_attachments += inventory.get("attachments", [])

    # Plaintext fallback (very short — the HTML is the experience)
    plain = (
        f"Daylight Weekly Report — Week of {date_str}\n\n"
        "This email is best viewed in an HTML-capable client. "
        "The full report is also attached as DOCX + CSV files.\n\n"
        f"Attachments ({len(file_attachments)}):\n"
        + "\n".join(f"  - {a[0]}" for a in file_attachments)
    )

    subject_prefix = "[TEST] " if test_mode else ""
    subject = f"{subject_prefix}Daylight Weekly Report — {date_str}"

    send_unified_email(
        subject=subject,
        html_body=html_body,
        plain_fallback=plain,
        recipient=REPORT_RECIPIENT,
        file_attachments=file_attachments,
        inline_images=inline_images,
    )


if __name__ == "__main__":
    import sys
    main(test_mode="--test" in sys.argv)
