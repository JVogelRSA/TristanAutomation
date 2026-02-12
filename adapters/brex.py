import os
import requests
import pandas as pd
from datetime import datetime, timedelta

BREX_API_URL = "https://platform.brexapis.com/v2"

def fetch_brex_transactions(api_key, days_back=30):
    """
    Fetches Brex transactions (Card) for the last N days.
    """
    if not api_key:
        print("Brex: No API Key provided.")
        return pd.DataFrame()

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # Date filtering
    # Brex uses 'posted_at_start' in ISO format
    start_date = (datetime.now() - timedelta(days=days_back)).isoformat()
    
    url = f"{BREX_API_URL}/transactions/card/primary"
    params = {
        "posted_at_start": start_date,
        "limit": 100
    }
    
    print(f"Brex: Fetching transactions since {start_date}...")
    
    all_txns = []
    
    try:
        while url:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code != 200:
                print(f"Brex Error {response.status_code}: {response.text}")
                break
                
            data = response.json()
            items = data.get('items', [])
            all_txns.extend(items)
            
            # Pagination
            next_cursor = data.get('next_cursor')
            if next_cursor:
                params['cursor'] = next_cursor
                # Remove posted_at_start for subsequent pages if strictly cursor-based?
                # Brex docs usually say keep filters.
            else:
                break
    except Exception as e:
        print(f"Brex Exception: {e}")
        return pd.DataFrame()

    if not all_txns:
        return pd.DataFrame(columns=["Date", "Description", "Amount", "Category", "Source"])

    # Normalization
    normalized_data = []
    for txn in all_txns:
        # Extract fields
        date = txn.get('posted_at_date') # YYYY-MM-DD
        desc = txn.get('description')
        amount = txn.get('amount', {}).get('amount')
        currency = txn.get('amount', {}).get('currency')
        
        # Determine category (Brex provides 'merchant' object with mcc or category)
        category = "Uncategorized"
        if txn.get('merchant'):
            category = txn.get('merchant').get('mcc_description') or category
            
        # Filter for only Money Out (positive amount in Brex is usually spend, but verify)
        # Brex: positive = spend, negative = refund/payment
        try:
            amt_val = float(amount)
        except:
            amt_val = 0.0
            
        normalized_data.append({
            "Date": date,
            "Description": desc,
            "Amount": amt_val, 
            "Category": category,
            "Source": "Brex"
        })
        
    return pd.DataFrame(normalized_data)
