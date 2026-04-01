import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load env
load_dotenv()
DB_URL = os.getenv("NEON_DB_URL")
FINGRID_API_KEY = os.getenv("FINGRID_API_KEY")

# DB connect
engine = create_engine(DB_URL)

def fetch_with_retry(url, headers, retries=3):
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if i == retries - 1: raise e
            print(f"Error, next try... {i+1}")
            time.sleep(5)

# --- UPSERT ---
def upsert_to_db(df, table_name, schema, unique_key, engine):
    """Загружает данные во временную таблицу и делает безопасный UPSERT в основную"""
    temp_table = f"{table_name}_temp"
    
    # 1. Upload dataframe in to the table
    df.to_sql(temp_table, engine, schema=schema, if_exists='replace', index=False)
    
    # 2. SQL to paste
    columns = ', '.join([f'"{col}"' for col in df.columns])
    
    upsert_query = f"""
        INSERT INTO {schema}.{table_name} ({columns})
        SELECT {columns} FROM {schema}.{temp_table}
        ON CONFLICT ({unique_key}) DO NOTHING;
    """
    
    # 3. Transaction
    with engine.begin() as conn:
        conn.execute(text(upsert_query))
        conn.execute(text(f"DROP TABLE {schema}.{temp_table}"))


def load_prices_to_bronze():
    print("Gathering prices data (Pörssisähkö)...")
    url = "https://api.porssisahko.net/v1/latest-prices.json"
    
    try:
        data_json = fetch_with_retry(url, headers={})
        data = data_json.get('prices', [])
        
        if data:
            df = pd.DataFrame(data)
            df.rename(columns={'startDate': 'start_date', 'endDate': 'end_date'}, inplace=True)
            
            upsert_to_db(df, 'raw_spot_prices', 'bronze', 'start_date', engine)
            print(f"✅ Processed {len(df)} price rows (new rows added, duplicates ignored).")
            
    except Exception as e:
        print(f"❌ Failed after retries: {e}")


def load_wind_to_bronze():
    print("Gathering wind data (Fingrid)...")
    url = "https://data.fingrid.fi/api/datasets/75/data"
    headers = {"x-api-key": FINGRID_API_KEY}
    
    now = datetime.utcnow()
    start = now - timedelta(days=3)
    
    params = {
        "startTime": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "endTime": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "format": "json",  
        "pageSize": 5000
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        raw_data = response.json()
        
        if isinstance(raw_data, dict):
            data = raw_data.get('data', [])
        else:
            data = raw_data 
            
        print(f"DEBUG: Received {len(data)} items from Fingrid")
        
        if len(data) > 0:
            df = pd.DataFrame(data)
            df.rename(columns={'startTime': 'start_time', 'endTime': 'end_time', 'datasetId': 'dataset_id'}, inplace=True)
            
            # New function
            upsert_to_db(df, 'raw_wind_generation', 'bronze', 'start_time', engine)
            print(f"✅ Processed {len(df)} wind rows (new rows added, duplicates ignored).")
        else:
            print("⚠️ Warning: Fingrid returned empty list.")
            
    except Exception as e:
        print(f"❌ Fingrid error: {e}")

if __name__ == "__main__":
    print("🚀 Extract & Load...")
    load_prices_to_bronze()
    load_wind_to_bronze()
    print("🎉 Done!")
