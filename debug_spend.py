import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()
from adapters.brex import fetch_brex_transactions

def format_currency(x):
    return "${:,.2f}".format(x)

df = fetch_brex_transactions(os.getenv("BREX_API_KEY"), days_back=30)
df['Date'] = pd.to_datetime(df['Date'])

now = datetime.now()
days_since_monday = now.weekday()
most_recent_monday = (now - timedelta(days=days_since_monday)).date()

end_date = pd.to_datetime(most_recent_monday)
curr_week_start = end_date - timedelta(days=7)
prev_week_start = end_date - timedelta(days=14)

curr_df = df[(df['Date'] >= curr_week_start) & (df['Date'] < end_date)]
print(curr_df)
