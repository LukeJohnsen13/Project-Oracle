import os
import ccxt
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import tenacity  # For retrying on rate limit errors

# Load API keys from .env
load_dotenv()

# Accounts for three failed connections
@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),  # Retry 3 times
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),  # Exponential backoff
    retry=tenacity.retry_if_exception_type(ccxt.RateLimitExceeded)  # Retry on rate limit
)
def fetch_ohlcv(pair='BTC/USDT', timeframe='1h', limit=1000):
    # Initialize Binance exchange with API key and secret
    exchange = ccxt.binance({
        'apiKey': os.getenv('BINANCE_API_KEY'),
        'secret': os.getenv('BINANCE_API_SECRET'),
        'enableRateLimit': True,  # Respect Binance rate limits
    })
    
    # Calculate timestamp for last 7 days
    since = int((datetime.utcnow() - timedelta(days=7)).timestamp() * 1000)
    
    try:
        # Fetch OHLCV data
        bars = exchange.fetch_ohlcv(pair, timeframe, since=since, limit=limit)
        df = pd.DataFrame(bars, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        return df
    except ccxt.AuthenticationError as e:
        print(f"Authentication error: {e}. Check BINANCE_API_KEY and BINANCE_API_SECRET in .env")
        return None
    except ccxt.NetworkError as e:
        print(f"Network error: {e}")
        raise  # Will trigger retry if rate limit

if __name__ == '__main__':
    # Fetch data
    df = fetch_ohlcv()
    if df is not None:
        # Save to Parquet
        out_dir = 'data/raw/ohlcv'
        os.makedirs(out_dir, exist_ok=True)
        today = datetime.utcnow().strftime('%Y-%m-%d')
        df.to_parquet(f'{out_dir}/btc_usdt_{today}.parquet')
        print(f'Saved {len(df)} rows to {out_dir}/btc_usdt_{today}.parquet')
    else:
        print("Failed to fetch OHLCV data")