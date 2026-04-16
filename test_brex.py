import os
import pandas as pd
from dotenv import load_dotenv
from adapters.brex import fetch_brex_transactions

load_dotenv()
df = fetch_brex_transactions(os.getenv("BREX_API_KEY"), days_back=30)
print(df.head(10))
if 'Amount' in df.columns:
    print("\nTypes of 'Amount' column:")
    print(df['Amount'].apply(type).value_counts())
    print("\nTotal Spend computed:", df['Amount'].sum())
