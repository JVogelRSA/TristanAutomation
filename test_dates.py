import pandas as pd
from datetime import datetime, timedelta

now = datetime(2026, 3, 18)
days_since_monday = now.weekday()
most_recent_monday = (now - timedelta(days=days_since_monday)).date()
end_date = pd.to_datetime(most_recent_monday)
curr_week_start = end_date - timedelta(days=7)

print("now:", now)
print("most_recent_monday:", most_recent_monday)
print("end_date:", end_date)
print("curr_week_start:", curr_week_start)

data = {'Date': ['2026-03-17', '2026-03-14'], 'Amount': [19.99, 2.99]}
df = pd.DataFrame(data)
df['Date'] = pd.to_datetime(df['Date'])
curr_df = df[(df['Date'] >= curr_week_start) & (df['Date'] < end_date)]
print(curr_df)
