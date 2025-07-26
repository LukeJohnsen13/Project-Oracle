import os
import requests
import pandas as pd
from datetime import datetime
from dune_client.client import DuneClient
from dotenv import load_dotenv
load_dotenv()

def fetch_glassnode(metric='active_addresses', asset='BTC'):
    url = f'https://api.glassnode.com/v1/metrics/addresses/active_count?a={asset}&api_key={os.getenv("GLASSNODE_API_KEY")}'
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.json()
        df = pd.DataFrame(data)
        df['t'] = pd.to_datetime(df['t'], unit='s', utc=True)
        return df.rename(columns={'t': 'timestamp', 'v': 'value'})
    return None

def fetch_dune_fallback(query_id=123456):  # Replace with your Dune query ID for active addresses
    dune = DuneClient(os.getenv('DUNE_API_KEY'))
    results = dune.refresh_into_dataframe(query_id)
    return results  # Assume timestamp column exists

if __name__ == '__main__':
    df = fetch_glassnode() or fetch_dune_fallback()
    out_dir = 'data/raw/onchain'
    os.makedirs(out_dir, exist_ok=True)
    today = datetime.utcnow().strftime('%Y-%m-%d')
    df.to_parquet(f'{out_dir}/btc_active_addresses_{today}.parquet')