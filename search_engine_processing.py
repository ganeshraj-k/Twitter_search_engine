import pandas as pd
import psycopg2
from pymongo import MongoClient
from db_connections import get_mongo_engine, create_engine_snowflake
import nltk
import string
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

tweets_collection = get_mongo_engine()
conn = create_engine_snowflake()

def query(sql_query, conn):
 	df = pd.read_sql_query(sql_query, conn)
 	return df

#def convert_tweets_df(tweets):


def get_info_by_hashtag(hashtag):
	try:
		query = { "hashtags": { "$in": hashtags } }
	    tweets = tweets_collection.find(query,{'tweet_str': 1, 'created_at': 1, 'retweet_count': 1, 'user_id_str': 1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)
	    df_tweets = pd.DataFrame(list(tweets))

	    df_users = query(f"""SELECT id_str, name FROM users 
			                WHERE id_str = ({','.join(df_tweets.user_id_str.unique())}) """, conn)

		df_final = df_tweets.merge(df_users, on = 'user_id_str') 
		df_final = df_final.drop('user_id_str', axis=1)
		return df_final.to_json(orient='records')
	except Exception as e:
		print(f"Retrieval of Tweet from hashtags failed : {e}")

def get_info_by_user(user_name):
	try:
		df = query(f"""SELECT id_str FROM users 
							WHERE name =  {user_name}
			         """, conn)

		tweets = tweet_collection.find({"user_id_str": : df.iloc[0,0]} , 'created_at': 1, 'retweet_count': 1, 'user_id_str': 1, '_id': 0).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)
		df_tweets = pd.DataFrame(list(tweets))
		df_final = df.merge(df_tweets, on = 'user_id_str')
		df_final = df_final.drop('user_id_str', axis=1)
		return df_final.to_json(orient='records')
	except Exception as e:
		print(f"Retrieval of Tweet from username failed : {e}")


def get_info_by_tweet(tweet_str = None, tweet_id = None):
	try:
		if tweet_str:
			words = word_tokenize(tweet_str)
			non_stop_words = sorted([word for word in words if not (word.lower() in stopwords.words() or word in string.punctuation)])
			query = {
			    "$and": [
			         {'$text': {'$search': ' '.join(non_stop_words)}},
			         {"oc_tweet_id": ""}
			    ]
			}
			tweets = tweet_collection.find(query, {'created_at': 1, 'retweet_count': 1, 'user_id_str': 1, 'oc_tweet_id': 1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)
		if tweet_id:
			tweets = tweets_collection.find({"oc_tweet_id": tweet_id},{'created_at': 1,'text':1, 'retweet_count': 1, 'user_id_str': 1, 'hashtag': 1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)

		df_tweets = pd.DataFrame(list(tweets))

		df_users = query("""SELECT id_str, name FROM users 
			                WHERE
			                 id_str = ({','.join(df_tweets.user_id_str.unique())}) """, conn)

		df_final = df_tweets.merge(df_users, on = 'user_id_str') 
		del df_final['user_id_str']
		if 'oc_tweet_id' in df_final.columns():
			del df_final['oc_tweet_id']
		return df_final.to_json(orient='records')
	except Exception as e:
		print(f"Retrieval of Tweet Failed : {e}")


def main():
	## Retrieve string from search
	#tweet_str = get()
	if tweet_str.startswith('@'):
		get_info_by_user()
	elif tweet_str.startswith('#'):
		get_info_by_hashtag(search_input)
	elif tweet_id:
		get_info_by_tweet(tweet_id = search_input)
	else:
		get_info_by_tweet(tweet_str = search_input)