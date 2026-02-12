import os
import requests
import pandas as pd
from datetime import datetime, timedelta

RIPPLING_API_URL = "https://api.rippling.com/platform/api"

def fetch_rippling_expenses(api_key, days_back=30):
    """
    Fetches Rippling Employee Expenses (Reimbursements).
    """
    if not api_key:
        print("Rippling: No API Key provided.")
        return pd.DataFrame()

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # Rippling API allows SQL-like queries (RQL) often, or standard endpoints
    # Trying the 'expenses' endpoint which is common in their partner integrations
    url = f"{RIPPLING_API_URL}/expenses" # This is a guess, needs verification
    
    print(f"Rippling: Fetching expenses...")
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 404:
            print("Rippling: '/expenses' endpoint not found. Please verify correct endpoint in API docs.")
            return pd.DataFrame()
        elif response.status_code != 200:
            print(f"Rippling Error {response.status_code}: {response.text}")
            return pd.DataFrame()
            
        # If successful (rare without correct endpoint knowledge), process here
        print("Rippling connection successful (mock).")
        
    except Exception as e:
        print(f"Rippling Exception: {e}")
    
    # Return empty for now until endpoint is confirmed
    return pd.DataFrame(columns=["Date", "Description", "Amount", "Category", "Source"])
