import osimport os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Config
load_dotenv()
DB_URL = os.getenv("NEON_DB_URL")
FINGRID_API_KEY = os.getenv("FINGRID_API_KEY")

engine = create_engine(DB_URL)

def fetch_with_retry(url, headers, params=None, retries=3):
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 429:
                print(f"Rate limited (429). Sleeping 5s... (Attempt {i+1})")
                time.sleep(5)
                continue
                
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            if i == retries - 1: raise e
            print(f"Request failed, retrying... {i+1}")
            time.sleep(5)

def upsert_to_db(df, table_name, schema, unique_key, engine):
    temp_table = f"{table_name}_temp"
    
    df.to_sql(temp_table, engine, schema=schema, if_exists='replace', index=False)
    
    columns = ', '.join([f'"{col}"' for col in df.columns])
    
    upsert_query = f"""
        INSERT INTO {schema}.{table_name} ({columns})
        SELECT {columns} FROM {schema}.{temp_table}
        ON CONFLICT ({unique_key}) DO NOTHING;
    """
    
    with engine.begin() as conn:
        conn.execute(text(upsert_query))
        conn.execute(text(f"DROP TABLE {schema}.{temp_table}"))

def load_prices_to_bronze():
    print("Fetching Pörssisähkö prices...")
    url = "https://api.porssisahko.net/v1/latest-prices.json"
    
    try:
        data_json = fetch_with_retry(url, headers={})
        data = data_json.get('prices', [])
        
        if data:
            df = pd.DataFrame(data)
            df.rename(columns={'startDate': 'start_date', 'endDate': 'end_date'}, inplace=True)
            
            df['start_date'] = pd.to_datetime(df['start_date'])
            df['end_date'] = pd.to_datetime(df['end_date'])

            upsert_to_db(df, 'raw_spot_prices', 'bronze', 'start_date', engine)
            print("✅ OK: Prices processed.")

    except Exception as e:
        print(f"❌ FAIL: Prices error: {e}")
        raise e

def load_generation_3min_to_bronze():
    print("Fetching Fingrid 3-min generation...")
    datasets = [181, 188, 191, 192]
    headers = {"x-api-key": FINGRID_API_KEY}
    
    now = datetime.utcnow()
    start = now - timedelta(days=3)
    
    params = {
        "startTime": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "endTime": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "format": "json",  
        "pageSize": 10000
    }
    
    for dataset_id in datasets:
        print(f" -> Dataset {dataset_id}")
        url = f"https://data.fingrid.fi/api/datasets/{dataset_id}/data"
        
        try:
            raw_data = fetch_with_retry(url, headers=headers, params=params)
            if not raw_data:
                continue

            data = raw_data.get('data', []) if isinstance(raw_data, dict) else raw_data
            
            if len(data) > 0:
                df = pd.DataFrame(data)
                
                df.rename(columns={
                    'startTime': 'start_time',
                    'endTime': 'end_time',
                    'datasetId': 'dataset_id',
                    'value': 'value'
                }, inplace=True)
                
                df = df[['dataset_id', 'start_time', 'end_time', 'value']]
                
                df['start_time'] = pd.to_datetime(df['start_time'])
                df['end_time'] = pd.to_datetime(df['end_time'])
                
                upsert_to_db(df, 'raw_generation_3min', 'bronze', 'start_time, dataset_id', engine)
                print(f"✅    OK: {len(df)} rows inserted.")
            else:
                print(f"❌    WARN: Empty data.")
            
            time.sleep(2)
            
        except Exception as e:
            print(f"❌    FAIL: {e}")

if __name__ == "__main__":
    print("--- Pipeline Started ---")
    load_prices_to_bronze()
    load_generation_3min_to_bronze()
    print("--- Pipeline Finished ---")
