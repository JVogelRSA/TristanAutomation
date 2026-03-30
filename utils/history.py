import json
import os
from datetime import datetime, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"


def get_month_key():
    """Returns 'YYYY-MM' string for the current calendar month."""
    return datetime.now().strftime('%Y-%m')


def save_monthly_snapshot(bot_name, month_key, data):
    """
    Save a monthly snapshot as JSON.
    month_key: 'YYYY-MM' string (e.g. '2026-03')
    data: dict of metrics to persist
    """
    path = DATA_DIR / bot_name
    path.mkdir(parents=True, exist_ok=True)
    filepath = path / f"month_{month_key}.json"
    payload = {"month": month_key, "saved_at": datetime.now().isoformat(), **data}
    with open(filepath, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    print(f"Saved monthly snapshot: {filepath.name}")


def load_monthly_history(bot_name, max_months=12):
    """
    Load all saved monthly snapshots, sorted oldest-first.
    Returns list of dicts. Caps at max_months most recent.
    """
    bot_path = DATA_DIR / bot_name
    bot_path.mkdir(parents=True, exist_ok=True)
    files = sorted(bot_path.glob("month_*.json"))
    snapshots = []
    for f in files:
        try:
            with open(f) as fh:
                snapshots.append(json.load(fh))
        except (json.JSONDecodeError, IOError):
            continue
    return snapshots[-max_months:]


def build_monthly_spend_comparison(history, curr_total):
    """Build a historical comparison string for the monthly spend LLM prompt."""
    if not history:
        return ""
    lines = []
    recent = [h for h in history[-3:] if "total_spend" in h]
    if recent:
        avg = sum(h["total_spend"] for h in recent) / len(recent)
        pct = ((curr_total - avg) / avg * 100) if avg > 0 else 0
        lines.append(f"3-Month Average Spend: ${avg:,.2f} (this month is {pct:+.1f}% vs avg)")
    if len(recent) >= 2:
        direction = "increasing" if recent[-1]["total_spend"] < curr_total else "decreasing"
        lines.append(f"Spend trend over last {len(recent)+1} months: {direction}")
        for h in recent:
            lines.append(f"  {h['month']}: ${h['total_spend']:,.2f} ({h.get('transaction_count', '?')} txns)")
    if not lines:
        return ""
    return "--- MONTHLY HISTORICAL COMPARISON ---\n" + "\n".join(lines)


def build_monthly_inventory_comparison(history, current_skus):
    """Build historical comparison for the monthly inventory LLM prompt."""
    if not history:
        return ""
    lines = []
    if history:
        prev = history[-1]
        changes = []
        for sku, curr in current_skus.items():
            prev_sku = prev.get("skus", {}).get(sku)
            if prev_sku and "monthly_burn" in prev_sku and curr.get("monthly_burn", 0) > 0:
                prev_burn = prev_sku["monthly_burn"]
                curr_burn = curr["monthly_burn"]
                if prev_burn > 0:
                    pct = ((curr_burn - prev_burn) / prev_burn) * 100
                    if abs(pct) > 10:
                        direction = "up" if pct > 0 else "down"
                        changes.append(f"{curr.get('product', sku)}: {direction} {abs(pct):.0f}% MoM ({prev_burn:.1f} → {curr_burn:.1f} units/mo)")
        if changes:
            lines.append(f"Month-over-Month Burn Changes (vs {prev.get('month', '?')}):\n  " + "\n  ".join(changes[:6]))
    if not lines:
        return ""
    return "--- MONTH-OVER-MONTH TRENDS ---\n" + "\n".join(lines)


def get_week_monday():
    """Returns the most recent Monday as a date object. Consistent across all bots."""
    today = datetime.now().date()
    days_since_monday = today.weekday()  # Monday = 0
    return today - timedelta(days=days_since_monday)


def _bot_dir(bot_name):
    path = DATA_DIR / bot_name
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_weekly_snapshot(bot_name, week_monday, data):
    """
    Save a weekly snapshot as JSON.
    week_monday: date object (the Monday that starts the reporting week)
    data: dict of metrics to persist
    """
    filepath = _bot_dir(bot_name) / f"week_{week_monday.isoformat()}.json"
    payload = {
        "week_monday": week_monday.isoformat(),
        "saved_at": datetime.now().isoformat(),
        **data,
    }
    with open(filepath, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    print(f"Saved snapshot: {filepath.name}")


def load_history(bot_name, max_weeks=12):
    """
    Load all saved weekly snapshots, sorted oldest-first.
    Returns list of dicts. Caps at max_weeks most recent.
    """
    bot_path = _bot_dir(bot_name)
    files = sorted(bot_path.glob("week_*.json"))
    snapshots = []
    for f in files:
        try:
            with open(f) as fh:
                snapshots.append(json.load(fh))
        except (json.JSONDecodeError, IOError):
            continue
    return snapshots[-max_weeks:]


def build_spend_comparison(history, curr_total, curr_week_monday):
    """
    Given spend history and this week's total, build a comparison summary string
    for the LLM prompt. Only includes comparisons where data exists.
    """
    if not history:
        return ""

    lines = []

    # Rolling 4-week average
    recent = [h for h in history[-4:] if "total_spend" in h]
    if recent:
        avg = sum(h["total_spend"] for h in recent) / len(recent)
        pct = ((curr_total - avg) / avg * 100) if avg > 0 else 0
        lines.append(f"Rolling {len(recent)}-Week Avg Spend: ${avg:,.2f} (this week is {pct:+.1f}% vs avg)")

    # Same week last month (~4 weeks ago)
    target = (curr_week_monday - timedelta(weeks=4)).isoformat()
    match = next((h for h in history if h["week_monday"] == target), None)
    if match and "total_spend" in match:
        prev = match["total_spend"]
        pct = ((curr_total - prev) / prev * 100) if prev > 0 else 0
        lines.append(f"Same Week Last Month: ${prev:,.2f} ({pct:+.1f}% change)")

    # Spend trend (last 4 weeks)
    recent_totals = [h["total_spend"] for h in history[-4:] if "total_spend" in h]
    if len(recent_totals) >= 3:
        trend_dir = "increasing" if recent_totals[-1] > recent_totals[0] else "decreasing"
        lines.append(f"4-Week Trend: {trend_dir} (${recent_totals[0]:,.0f} → ${recent_totals[-1]:,.0f})")

    # Top vendor consistency (who keeps showing up)
    if len(history) >= 2:
        all_top = {}
        for h in history[-4:]:
            for vendor, amt in h.get("top_vendors", {}).items():
                all_top[vendor] = all_top.get(vendor, 0) + 1
        recurring = {v: c for v, c in all_top.items() if c >= 2}
        if recurring:
            top3 = sorted(recurring.items(), key=lambda x: -x[1])[:3]
            lines.append(f"Recurring Top Vendors (last 4 weeks): {', '.join(f'{v} ({c}wk)' for v, c in top3)}")

    if not lines:
        return ""
    return "--- HISTORICAL COMPARISON ---\n" + "\n".join(lines)


def build_inventory_comparison(history, current_skus):
    """
    Given inventory history and this week's SKU data, build a comparison summary
    for the LLM prompt. current_skus: dict of {sku: {stock, burn_rate}}
    """
    if not history:
        return ""

    lines = []

    # Burn rate trend per top SKU (is it accelerating?)
    if len(history) >= 2:
        burn_trends = []
        for sku, curr_data in current_skus.items():
            past_burns = []
            for h in history[-4:]:
                sku_hist = h.get("skus", {}).get(sku)
                if sku_hist and "burn_rate" in sku_hist:
                    past_burns.append(sku_hist["burn_rate"])
            if past_burns and curr_data.get("burn_rate", 0) > 0:
                avg_past = sum(past_burns) / len(past_burns)
                curr_burn = curr_data["burn_rate"]
                if avg_past > 0:
                    pct = ((curr_burn - avg_past) / avg_past) * 100
                    if abs(pct) > 15:  # Only flag meaningful changes
                        direction = "accelerating" if pct > 0 else "decelerating"
                        burn_trends.append(f"{curr_data.get('product', sku)}: burn {direction} ({pct:+.0f}% vs {len(past_burns)}-wk avg)")
        if burn_trends:
            lines.append("Burn Rate Trends:\n  " + "\n  ".join(burn_trends[:5]))

    # Stock level comparison vs 4 weeks ago
    if history:
        old = history[-4] if len(history) >= 4 else history[0]
        stock_changes = []
        for sku, curr_data in current_skus.items():
            old_sku = old.get("skus", {}).get(sku)
            if old_sku and "stock" in old_sku:
                old_stock = old_sku["stock"]
                new_stock = curr_data.get("stock", 0)
                if old_stock > 0:
                    pct = ((new_stock - old_stock) / old_stock) * 100
                    if pct < -40:
                        stock_changes.append(f"{curr_data.get('product', sku)}: {old_stock} → {new_stock} ({pct:+.0f}%)")
        if stock_changes:
            lines.append(f"Significant Stock Drops (vs {old.get('week_monday', '?')}):\n  " + "\n  ".join(stock_changes[:5]))

    if not lines:
        return ""
    return "--- HISTORICAL TRENDS ---\n" + "\n".join(lines)
