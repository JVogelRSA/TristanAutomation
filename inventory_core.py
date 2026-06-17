"""
Shared inventory logic for the Zeni (external) and Anjan (internal) monthly
reports. Both reports MUST agree on unit counts and valuation — Zeni's
accountant and Anjan are on the same email thread — so the SKU definitions,
cost/retail basis, and the warehouse+office combine live here, in ONE place.

Valuation basis (confirmed by Zeni / Amol Taxali, 2026-06-17):
  New + Open-box valued at cost (and shown at retail); Warranty units are a
  count-only memo (carried at NRV, not full cost).

The two reports differ only in (a) which DCL snapshot they read — Zeni uses the
month-end snapshot, Anjan the latest — and (b) presentation. The category math
and money are identical because they both call combine() here.
"""
import os
import re
from datetime import datetime

import pandas as pd

# What counts as a finished, fully-assembled device.
#   SKU 1 = DC-1 adult, 6 / 6-k = POS (usually 0), 7 = Kids DC-1 bundle.
#   Open-box / graded returns = SKU 4-x-x (all DC-1) and the RESEND SKU 2.
DC1_SKUS = {"1", "6", "6-k"}
KIDS_SKUS = {"7"}


def _envfloat(name, default):
    """float() tolerant of unset/empty env (GitHub passes undefined secrets as '')."""
    v = (os.getenv(name) or "").strip()
    try:
        return float(v) if v else float(default)
    except ValueError:
        return float(default)


# Per-unit money. Cost = production cost ($425 both models, Jesse 2026-06-16,
# Kids being a bundle carries the same per-unit cost). Retail = list price.
DC1_COST = _envfloat("DC1_COST_USD", 425)
KIDS_COST = _envfloat("KIDS_COST_USD", 425)
DC1_RETAIL = _envfloat("DC1_VALUE_USD", 729)
KIDS_RETAIL = _envfloat("KIDS_VALUE_USD", 799)


def date_from_filename(filename, fallback):
    """DCL files embed the report date (tz-proof), e.g. 'Items Status-2026-06-01_0000.csv'."""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", filename or "")
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            pass
    return fallback


def norm_items(series):
    """Normalize the Item # column to clean strings.

    Critical: a single blank/NaN Item # cell makes pandas type the whole column
    float64, and a plain .astype(str) turns SKU '1' into '1.0' - which matches
    none of our SKU sets and silently zeroes every count. Strip a trailing '.0'.
    """
    return series.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)


def warehouse_breakdown(df):
    """Finished-goods counts from a DCL 'Items Status' snapshot dataframe."""
    items = norm_items(df["Item #"])
    qty = pd.to_numeric(df["Q On Hand"], errors="coerce").fillna(0)
    return {
        "new_dc1": int(qty[items.isin(DC1_SKUS)].sum()),
        "new_kids": int(qty[items.isin(KIDS_SKUS)].sum()),
        "ob_dc1": int(qty[items.str.startswith("4-") | (items == "2")].sum()),  # all DC-1
        "ob_kids": 0,
    }


def empty_office():
    z = {"total": 0, "std": 0, "kids": 0}
    return {"new": dict(z), "warranty": dict(z), "openbox": dict(z), "total": dict(z),
            "grades": {}, "queue": {}, "accessories": {}}


def combine(wh, office):
    """Merge warehouse breakdown + office summary into the canonical numbers
    both reports render from. `wh` keys: new_dc1/new_kids/ob_dc1/ob_kids (the
    Zeni report may pass month-end-adjusted new counts). `office` is an
    office_inventory.office_summary() dict (or empty_office())."""
    new_dc1 = wh["new_dc1"] + office["new"]["std"]
    new_kids = wh["new_kids"] + office["new"]["kids"]
    ob_dc1 = wh["ob_dc1"] + office["openbox"]["std"]
    ob_kids = wh["ob_kids"] + office["openbox"]["kids"]
    wr_dc1 = office["warranty"]["std"]      # warehouse warranty folds into open-box
    wr_kids = office["warranty"]["kids"]

    new_cost = new_dc1 * DC1_COST + new_kids * KIDS_COST
    new_ret = new_dc1 * DC1_RETAIL + new_kids * KIDS_RETAIL
    ob_cost = ob_dc1 * DC1_COST + ob_kids * KIDS_COST
    ob_ret = ob_dc1 * DC1_RETAIL + ob_kids * KIDS_RETAIL

    sellable_dc1, sellable_kids = new_dc1 + ob_dc1, new_kids + ob_kids
    sellable_units = sellable_dc1 + sellable_kids
    warranty_units = wr_dc1 + wr_kids

    return {
        "new": (new_dc1, new_kids), "openbox": (ob_dc1, ob_kids), "warranty": (wr_dc1, wr_kids),
        "new_cost": new_cost, "new_ret": new_ret, "ob_cost": ob_cost, "ob_ret": ob_ret,
        "sellable": (sellable_dc1, sellable_kids), "sellable_units": sellable_units,
        "sellable_cost": new_cost + ob_cost, "sellable_ret": new_ret + ob_ret,
        "warranty_units": warranty_units,
        "census_units": sellable_units + warranty_units,
        "wh_units": wh["new_dc1"] + wh["new_kids"] + wh["ob_dc1"] + wh["ob_kids"],
        # Derive office_units from the SAME components that build census (not the
        # sheet's separate "Total DC-1s" cell, which could drift), so the
        # headline identity census == wh_units + office_units always holds.
        "office_units": (office["new"]["total"] + office["openbox"]["total"]
                         + office["warranty"]["total"]),
    }
