import os
import requests
import pandas as pd
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
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json().get('prices', [])
        df = pd.DataFrame(data)
        
        # Rename
        df.rename(columns={'startDate': 'start_date', 'endDate': 'end_date'}, inplace=True)
        
        # Load to Neon DB
        df.to_sql('raw_spot_prices', engine, schema='bronze', if_exists='append', index=False)
        print(f"✅ Done {len(df)} price rows.")
    else:
        print("❌ Error while gathering.")

def load_wind_to_bronze():
    print("Gathering wind data (Fingrid)...")
    url = "https://data.fingrid.fi/api/datasets/75/data"
    headers = {"x-api-key": FINGRID_API_KEY}
    
    # Data (fixed)
    params = {
        "startTime": "2024-05-01T00:00:00Z",
        "endTime": "2024-05-02T00:00:00Z",
        "format": "json"
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json().get('data', [])
        df = pd.DataFrame(data)
        
        # Rename
        df.rename(columns={'startTime': 'start_time', 'endTime': 'end_time', 'datasetId': 'dataset_id'}, inplace=True)
        
        # Load to Neon DB
        df.to_sql('raw_wind_generation', engine, schema='bronze', if_exists='append', index=False)
        print(f"✅ Loaded {len(df)} wind rows.")
    else:
        print(f"❌ Error Fingrid API: {response.text}")

if __name__ == "__main__":
    print("🚀 Extract & Load...")
    load_prices_to_bronze()
    load_wind_to_bronze()
    print("🎉 Done!")