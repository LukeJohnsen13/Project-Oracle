import os
import ccxt
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

# Experiment with the time frame later during training

def fetch_ohlcv(pair='BTC/USDT', timeframe='1h', limit=1000):
    """
    Fetches OHLCV (Open, High, Low, Close, Volume) data for a specified trading pair and timeframe from Binance.

    Args:
        pair (str): Trading pair symbol (e.g., 'BTC/USDT'). Defaults to 'BTC/USDT'.
        timeframe (str): Timeframe for each OHLCV bar (e.g., '1h', '5m'). Defaults to '1h'.
        limit (int): Maximum number of OHLCV bars to fetch. Defaults to 1000.

    Returns:
        pandas.DataFrame: DataFrame containing OHLCV data with columns ['timestamp', 'open', 'high', 'low', 'close', 'volume'].
            The 'timestamp' column is in UTC datetime format.

    Raises:
        ccxt.BaseError: If there is an issue fetching data from Binance.
        KeyError: If API keys are not found in environment variables.
    """
    exchange = ccxt.binance({
        'apiKey': os.getenv('BINANCE_API_KEY'),     # Add API key for Binance
        'secret': os.getenv('BINANCE_API_SECRET'),   # Add API secret for Binance
    })

    # The since function pulls from the past 7 days only might need to edit in training
    since = int((datetime.utcnow() - timedelta(days=7)).timestamp() * 1000)
    bars = exchange.fetch_ohlcv(pair, timeframe, since=since, limit=limit)
    df = pd.DataFrame(bars, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    return df

if __name__ == '__main__':
    df = fetch_ohlcv()
    out_dir = 'data/raw/ohlcv'
    os.makedirs(out_dir, exist_ok=True)
    today = datetime.utcnow().strftime('%Y-%m-%d')
    df.to_parquet(f'{out_dir}/btc_usdt_{today}.parquet')  # just changes the datatype to parquet from df
    print(f'Saved {len(df)} rows')