import os
import io
import pandas as pd
from imap_tools import MailBox, AND
from dotenv import load_dotenv

load_dotenv()
IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")
IMAP_USERNAME = os.getenv("IMAP_USERNAME")
IMAP_PASSWORD = os.getenv("IMAP_PASSWORD")

with MailBox(IMAP_SERVER).login(IMAP_USERNAME, IMAP_PASSWORD) as mailbox:
    for msg in mailbox.fetch(AND(subject="Inventory"), limit=1, reverse=True):
        for att in msg.attachments:
            if att.filename.lower().endswith('.csv'):
                df = pd.read_csv(io.BytesIO(att.payload))
                item_col = next((c for c in df.columns if 'item' in c.lower() or 'sku' in c.lower()), df.columns[0])
                skus_in_csv = df[item_col].astype(str).str.strip().tolist()
                
                print("Missing SKUs Check:")
                tracked = ['303', '36', '1', '401', '400', '28', '29', '31', '37', '302', '32', '35', '301']
                for t in tracked:
                    if t not in skus_in_csv:
                        print(f"MISSING FROM CSV: {t}")
                    else:
                        print(f"FOUND IN CSV: {t}")
                break
