import os
import pandas as pd
from dotenv import load_dotenv
from adapters.brex import fetch_brex_transactions

load_dotenv()
df = fetch_brex_transactions(os.getenv("BREX_API_KEY"), days_back=30)
print(df[['Date', 'Description', 'Amount']].head(10))
