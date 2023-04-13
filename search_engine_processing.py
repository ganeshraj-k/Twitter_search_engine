import pandas as pd
import psycopg2
from pymongo import MongoClient
from db_connections import get_mongo_engine, create_engine_postgres

tweets_collection = get_mongo_engine()
conn = create_engine_postgres(
    host='your_host',
    database='your_database_name',
    user='your_username',
    password='your_password',
    port='your_port'
)

def query(sql_query, conn):
 	df = pd.read_sql_query(sql_query, conn)
 	return df

def convert_tweets_df(tweets):


def get_tweet_info_by_hashtag(hashtag):
	query = { "hashtags": { "$in": hashtags } }
    tweets = tweets_collection.find(query,{'tweet_str': 1, 'created_at': 1, 'retweet_count': 1, 'user_id_str': 1})
    df_tweets = pd.DataFrame(list(tweets))

    df_users = query("""SELECT id_str, name FROM users 
		                WHERE
		                id_str = ({','.join(df_tweets.user_id_str.unique())}) """, conn)

	df_final = df_tweets.merge(df_users, on = 'user_id_str') 
	return df_final

def get_tweet_info_by_user(user_name):
	df = query(f"""SELECT id_str FROM users 
		            WHERE name =  {user_name}""", conn)

	tweets = tweets_collection.find({"user_id_str": : df.iloc[0,0]} , 'created_at': 1, 'retweet_count': 1, 'user_id_str': 1)
	df_tweets = pd.DataFrame(list(tweets))

	df_final = df.merge(df_tweets, on = 'user_id_str')

def get_tweet_info_by_tweet(tweet_str, tweet_id):
	if tweet_str:
		tweets = tweets_collection.find({"text": {"$regex": tweet_str}},{'created_at': 1, 'retweet_count': 1, 'user_id_str': 1})
	if tweet_id:
		tweets = tweets_collection.find({"oc_tweet_id": tweet_id},{'created_at': 1, 'retweet_count': 1, 'user_id_str': 1, 'hashtag': 1})

	df_tweets = pd.DataFrame(list(tweets))

	df_users = query("""SELECT id_str, name FROM users 
		                WHERE
		                id_str = ({','.join(df_tweets.user_id_str.unique())}) """, conn)

	df_final = df_tweets.merge(df_users, on = 'user_id_str') 
	return df_final

## Cache Retrieval
## def retrieve_from_cache(): 

def main():
	## Retrieve string from search
	#tweet_str = get()
	if tweet_str:
		get_tweet_info_by_tweet(tweet_str = tweet_str)
	elif tweet_id:
		get_tweet_info_by_tweet(tweet_id = tweet_id)
	elif user_name:
		get_tweet_info_by_user(user_name)
	elif hashtag:
		get_tweet_info_by_hashtag




