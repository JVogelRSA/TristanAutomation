import os
import requests
import pandas as pd
from datetime import datetime, timedelta

MERCURY_API_URL = "https://api.mercury.com/api/v1"

def fetch_mercury_transactions(api_key, days_back=30):
    """
    Fetches Mercury transactions for the last N days.
    """
    if not api_key:
        print("Mercury: No API Key provided.")
        return pd.DataFrame()

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # Mercury uses 'start_at' (ISO timestamp)
    start_date = (datetime.now() - timedelta(days=days_back)).isoformat()
    
    url = f"{MERCURY_API_URL}/transactions"
    params = {
        "start": start_date,
        "limit": 500
    }
    
    print(f"Mercury: Fetching transactions since {start_date}...")
    
    all_txns = []
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"Mercury Error {response.status_code}: {response.text}")
            return pd.DataFrame()
            
        data = response.json()
        all_txns = data.get('transactions', [])
        
    except Exception as e:
        print(f"Mercury Exception: {e}")
        return pd.DataFrame()

    if not all_txns:
        return pd.DataFrame(columns=["Date", "Description", "Amount", "Category", "Source"])

    # Normalization
    normalized_data = []
    for txn in all_txns:
        # Extract fields
        # Mercury date is 'postedAt' or 'createdAt'
        date_str = txn.get('postedAt') or txn.get('createdAt')
        date = date_str.split('T')[0] if date_str else ""
        
        desc = txn.get('bankDescription') or txn.get('note') or "Unknown"
        amount = float(txn.get('amount', 0))
        
        # Mercury kind: 'externalTransfer', 'check', etc.
        kind = txn.get('kind', 'Unknown')
        
        # We only want Money Out (negative amounts in Mercury are outflows)
        if amount >= 0:
            continue # Skip deposits
            
        # Convert to positive for reporting
        spend_amount = abs(amount)
            
        normalized_data.append({
            "Date": date,
            "Description": desc,
            "Amount": spend_amount,
            "Category": kind, # Mercury doesn't have deep categorization, use Kind
            "Source": "Mercury"
        })
        
    return pd.DataFrame(normalized_data)
