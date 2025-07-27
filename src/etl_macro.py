import os
import yfinance as yf
from fredapi import Fred
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

def fetch_macro():
    """
    Fetches and merges macroeconomic data including the US Dollar Index (DXY) and S&P 500 index prices.

    Returns:
        pd.DataFrame: A DataFrame containing merged data for the DXY and S&P 500 indices, indexed by timestamp.

    Notes:
        - The DXY data is retrieved from the FRED API using the 'DTWEXBGS' series.
        - The S&P 500 data is downloaded using yfinance for the past year.
        - The function expects the FRED API key to be available in the environment variable 'FRED_API_KEY'.
        - The returned DataFrame contains columns for the dollar index ('dxy') and S&P 500 prices, merged on timestamp.
    """
    fred = Fred(api_key=os.getenv('FRED_API_KEY'))
    dxy = fred.get_series('DTWEXBGS')  # Dollar index
    sp500 = yf.download('^GSPC', start=(datetime.utcnow() - timedelta(days=365)).strftime('%Y-%m-%d'))
    dxy_df = pd.DataFrame(dxy, columns=['dxy']).reset_index(name='timestamp')
    return pd.merge(dxy_df, sp500.reset_index().rename(columns={'Date': 'timestamp'}), on='timestamp', how='outer')

if __name__ == '__main__':
    df = fetch_macro()
    out_dir = 'data/raw/macro'
    os.makedirs(out_dir, exist_ok=True)
    today = datetime.utcnow().strftime('%Y-%m-%d')
    df.to_parquet(f'{out_dir}/macro_{today}.parquet')