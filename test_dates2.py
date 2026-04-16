import pandas as pd
from datetime import datetime, timedelta

def format_currency(x):
    return "${:,.2f}".format(x)

data = {'Date': ['2026-03-18', '2026-03-17', '2026-03-16', '2026-03-15', '2026-03-09'], 'Amount': [100, 19.99, 10, 20, 30]}
df = pd.DataFrame(data)

df['Date'] = pd.to_datetime(df['Date'])

now = datetime.now()
days_since_monday = now.weekday() # Monday = 0, Sunday = 6
most_recent_monday = (now - timedelta(days=days_since_monday)).date()

end_date = pd.to_datetime(most_recent_monday)
curr_week_start = end_date - timedelta(days=7)
prev_week_start = end_date - timedelta(days=14)

print(f"Now: {now}, most_recent_monday: {most_recent_monday}")
print(f"End Date: {end_date}")
print(f"Curr Week Start: {curr_week_start}")
print(f"Prev Week Start: {prev_week_start}")

curr_df = df[(df['Date'] >= curr_week_start) & (df['Date'] < end_date)]
print("\ncurr_df:")
print(curr_df)
