import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs 

def run_twitter_etl():
    # Twitter API v2 credentials - Replace with your Bearer Token
    bearer_token = "AAAAAAAAAAAAAAAAAAAAAGh83wEAAAAAilCl0lK8blExtGXyP8krqbZJ%2FaE%3DmZ4qC98f96wDzdGWwd87DVE8NJGscMMXPn79KSZRtJmAWG9UTE"
    
    # Initialize Twitter API v2 client
    client = tweepy.Client(bearer_token=bearer_token)
    
    # Get user ID first
    user = client.get_user(username="elonmusk")
    if not user.data:
        print("User not found or access denied")
        return pd.DataFrame()  # Return empty DataFrame if user not found
    
    user_id = user.data.id
    
    # Get tweets using v2 endpoint (max 100 per request for essential access)
    tweets = client.get_users_tweets(
        id=user_id,
        max_results=100,  # Maximum for essential access
        exclude=['retweets'],  # Equivalent to include_rts=False
        tweet_fields=['created_at', 'text', 'public_metrics', 'author_id']
    )
    
    list = []
    if tweets.data:
        for tweet in tweets.data:
            refined_tweet = {
                "user": "elonmusk",  # Using the screen name directly
                'text': tweet.text,
                'favorite_count': tweet.public_metrics['like_count'],  # Note: v2 uses 'like_count' instead of 'favorite_count'
                'retweet_count': tweet.public_metrics['retweet_count'],
                'created_at': tweet.created_at
            }
            list.append(refined_tweet)
    
    df = pd.DataFrame(list)
    df.to_csv('refined_tweets.csv')
    return df

# Run the ETL process
df = run_twitter_etl()
print(df.head())