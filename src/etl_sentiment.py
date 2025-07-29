import os
import tweepy
import praw
from newsapi import NewsApiClient
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

coin_query = 'bitcoin'      #Currently all queries are set to bitcoin, should be parameterized later

def fetch_x_tweets(query=coin_query, limit=100):
    """
    Fetches recent tweets matching a given query from X (formerly Twitter) using the Tweepy client.

    Args:
        query (str): The search query string to filter tweets. Defaults to 'bitcoin'.
        limit (int): The maximum number of tweets to retrieve. Defaults to 100.

    Returns:
        pd.DataFrame: A DataFrame containing the timestamp and text of each tweet. Returns an empty DataFrame if no tweets are found.

    Notes:
        - Requires the environment variable 'X_BEARER_TOKEN' to be set with a valid bearer token.
        - Fetches tweets from the last 24 hours.
    """
    client = tweepy.Client(bearer_token=os.getenv('X_BEARER_TOKEN'))
    start_time = (datetime.utcnow() - timedelta(days=1)).isoformat()
    tweets = client.search_recent_tweets(query=query, start_time=start_time, max_results=limit,
                                         tweet_fields=['created_at', 'text'])
    if tweets.data:
        rows = [{'timestamp': t.created_at, 'text': t.text} for t in tweets.data]
        return pd.DataFrame(rows)
    return pd.DataFrame()

def fetch_reddit_posts(subreddit=coin_query, limit=100):
    reddit = praw.Reddit(client_id=os.getenv('REDDIT_CLIENT_ID'),
                         client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
                         user_agent=os.getenv('REDDIT_USER_AGENT'))
    posts = reddit.subreddit(subreddit).new(limit=limit)
    rows = [{'timestamp': datetime.fromtimestamp(p.created_utc, tz=datetime.timezone.utc),
             'text': p.title + ' ' + p.selftext} for p in posts]
    return pd.DataFrame(rows)

def fetch_news(query=coin_query, limit=100):
    newsapi = NewsApiClient(api_key=os.getenv('NEWSAPI_KEY'))
    articles = newsapi.get_everything(q=query, language='en', page_size=limit)['articles']
    rows = [{'timestamp': pd.to_datetime(a['publishedAt'], utc=True),
             'text': a['title'] + ' ' + a['description']} for a in articles]
    return pd.DataFrame(rows)

if __name__ == '__main__':
    df_x = fetch_x_tweets()
    df_reddit = fetch_reddit_posts()
    df_news = fetch_news()
    df = pd.concat([df_x, df_reddit, df_news], ignore_index=True)
    out_dir = 'data/raw/sentiment'
    os.makedirs(out_dir, exist_ok=True)
    today = datetime.utcnow().strftime('%Y-%m-%d')
    df.to_parquet(f'{out_dir}/crypto_sentiment_{today}.parquet')