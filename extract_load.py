import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load env
load_dotenv()
DB_URL = os.getenv("NEON_DB_URL")
FINGRID_API_KEY = os.getenv("FINGRID_API_KEY")

# DB connect
engine = create_engine(DB_URL)

def load_prices_to_bronze():
    print("Gathering prices data (Pörssisähkö)...")
    url = "https://api.porssisahko.net/v1/latest-prices.json"
    
    # Use of retry
    try:
        data_json = fetch_with_retry(url, headers={})
        data = data_json.get('prices', [])
        
        if data:
            df = pd.DataFrame(data)
            df.rename(columns={'startDate': 'start_date', 'endDate': 'end_date'}, inplace=True)
            df.to_sql('raw_spot_prices', engine, schema='bronze', if_exists='append', index=False)
            print(f"✅ Done {len(df)} price rows.")
    except Exception as e:
        print(f"❌ Failed after retries: {e}")

def load_wind_to_bronze():
    print("Gathering wind data (Fingrid)...")
    url = "https://data.fingrid.fi/api/datasets/75/data"
    headers = {"x-api-key": FINGRID_API_KEY}
    
    # Last 3 days
    now = datetime.utcnow()
    start = now - timedelta(days=3)
    
    params = {
        "startTime": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "endTime": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "format": "json"
    }
    
  
def fetch_with_retry(url, headers, retries=3):
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if i == retries - 1: raise e
            print(f"Error, next try... {i+1}")
            time.sleep(5) # Wait 5 seconds

if __name__ == "__main__":
    print("🚀 Extract & Load...")
    load_prices_to_bronze()
    load_wind_to_bronze()
    print("🎉 Done!")
