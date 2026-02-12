import os
import sys
from dotenv import load_dotenv
from adapters.brex import fetch_brex_transactions
from adapters.mercury import fetch_mercury_transactions
from adapters.rippling import fetch_rippling_expenses

# Load environment variables
load_dotenv()

def main():
    print("--- Testing API Connections ---")
    
    # 1. Test Brex
    print("\n[1] Testing Brex...")
    brex_key = os.getenv("BREX_API_KEY")
    if brex_key:
        df = fetch_brex_transactions(brex_key, days_back=5)
        print(f"   Success! Fetched {len(df)} transactions.")
        if not df.empty:
            print(df.head())
    else:
        print("   SKIPPED (No Key)")

    # 2. Test Mercury
    print("\n[2] Testing Mercury...")
    mercury_key = os.getenv("MERCURY_API_KEY")
    if mercury_key:
        df = fetch_mercury_transactions(mercury_key, days_back=5)
        print(f"   Success! Fetched {len(df)} transactions.")
        if not df.empty:
            print(df.head())
    else:
        print("   SKIPPED (No Key)")

    # 3. Test Rippling
    print("\n[3] Testing Rippling...")
    rippling_key = os.getenv("RIPPLING_API_KEY")
    if rippling_key:
        df = fetch_rippling_expenses(rippling_key, days_back=5)
        print(f"   Result: Fetched {len(df)} records.")
    else:
        print("   SKIPPED (No Key)")

    # 4. Test Snowflake
    print("\n[4] Testing Snowflake...")
    if os.getenv("SNOWFLAKE_USER"):
        try:
            import snowflake.connector
            ctx = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA")
            )
            print("   Success! Connected to Snowflake.")
            ctx.close()
        except Exception as e:
            print(f"   Failed: {e}")
    else:
        print("   SKIPPED (No Credentials)")

    print("\n------------------------------")
    print("Done. If you see 'Success', your keys are working!")

if __name__ == "__main__":
    main()
