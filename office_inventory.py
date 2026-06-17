"""
Reads office-held inventory from the "Daylight IMS" Google Sheet, Report tab,
via the daylight-claude service account. Returns a structured summary that the
internal (Anjan) report merges with the DCL warehouse snapshot.

The office holds returns/warranty/open-box and a small amount of new stock that
the DCL warehouse snapshot does NOT see. DCL = warehouse, this sheet = office.

Credential resolution (first that works wins):
  1. GOOGLE_CREDENTIALS_JSON  - raw service-account JSON string (CI / GitHub secret)
  2. GOOGLE_CREDENTIALS_PATH  - path to a service-account json file
  3. ./credentials/google_credentials.json  - local, gitignored

Parsing is by ROW LABEL (not fixed cell refs) so it survives the sheet author
inserting/moving rows. If creds are missing or the read fails, office_summary()
returns None and the caller falls back to a warehouse-only report.
"""
import os
import re
import json

SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "16qhcqQvxQYZauGc8s-NTcaG2Vy3qKijV5wrx_H-jq38")
REPORT_TAB = os.getenv("OFFICE_REPORT_TAB", "Report")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def _credentials():
    from google.oauth2 import service_account
    raw = (os.getenv("GOOGLE_CREDENTIALS_JSON") or "").strip()
    if raw:
        return service_account.Credentials.from_service_account_info(
            json.loads(raw), scopes=SCOPES)
    here = os.path.dirname(os.path.abspath(__file__))
    for p in (os.getenv("GOOGLE_CREDENTIALS_PATH"),
              os.path.join(here, "credentials", "google_credentials.json")):
        if p and os.path.exists(p):
            return service_account.Credentials.from_service_account_file(p, scopes=SCOPES)
    return None


def _num(x):
    """Parse a sheet cell to int; blank / non-numeric -> 0."""
    s = re.sub(r"[^0-9.\-]", "", str(x if x is not None else ""))
    if s in ("", "-", ".", "-."):
        return 0
    try:
        return int(round(float(s)))
    except ValueError:
        return 0


def office_summary():
    """Return a dict of office inventory, or None if unavailable."""
    creds = _credentials()
    if creds is None:
        print("[office] no service-account credentials found - skipping office data.")
        return None
    try:
        from googleapiclient.discovery import build
        svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
        vals = (svc.spreadsheets().values()
                .get(spreadsheetId=SHEET_ID, range=f"{REPORT_TAB}!A1:L40")
                .execute().get("values", []))
    except Exception as e:                       # network / auth / API error
        print(f"[office] sheet read failed: {e}")
        return None
    if not vals:
        return None

    grid = [row + [""] * (12 - len(row)) for row in vals]   # pad for safe indexing

    def cat(label):
        for row in grid:
            if str(row[0]).strip().lower() == label.lower():
                return {"total": _num(row[1]), "std": _num(row[2]), "kids": _num(row[3])}
        return {"total": 0, "std": 0, "kids": 0}

    # Open-box grade split (cols K/L)
    grades = {}
    for row in grid:
        k = str(row[10]).strip()
        if k.lower().startswith("creak grade"):
            grades[k.replace("Creak Grade", "").strip()] = _num(row[11])

    # Warranty issue queue (cols F/G): issue label + count, skip headers/zeros
    queue = {}
    for row in grid:
        f = str(row[5]).strip()
        if not f or f.lower() in ("warranty", "all", "total", "standard", "kids"):
            continue
        n = _num(row[6])
        if n > 0:
            queue[f] = n

    # Office accessories: rows between the "Accessories" and "Test Units" headers
    accessories, in_acc = {}, False
    for row in grid:
        a = str(row[0]).strip()
        if a.lower() == "accessories":
            in_acc = True
            continue
        if a.lower().startswith("test unit"):
            in_acc = False
        if in_acc and a:
            accessories[a] = _num(row[1])

    return {
        "new": cat("New Units"),
        "warranty": cat("Warranty"),
        "openbox": cat("Open Box"),
        "total": cat("Total DC-1s"),
        "grades": grades,
        "queue": queue,
        "accessories": accessories,
    }


if __name__ == "__main__":
    import pprint
    pprint.pprint(office_summary())
