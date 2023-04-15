import pandas as pd
import psycopg2
from pymongo import MongoClient
from db_connections import get_mongo_engine, create_engine_postgres
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

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


def get_info_by_hashtag(hashtag):
	try:
		query = { "hashtags": { "$in": hashtags } }
	    tweets = tweets_collection.find(query,{'tweet_str': 1, 'created_at': 1, 'retweet_count': 1, 'user_id_str': 1}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)
	    df_tweets = pd.DataFrame(list(tweets))

	    df_users = query("""SELECT id_str, name FROM users 
			                WHERE
			                id_str = ({','.join(df_tweets.user_id_str.unique())}) """, conn)

		df_final = df_tweets.merge(df_users, on = 'user_id_str') 
		return df_final
	except Exception as e:
		print(f"Retrieval of Tweet from hashtags failed : {e}")


def get_info_by_user(user_name):
	try:
		df = query(f"""SELECT id_str FROM users 
			            WHERE name =  {user_name}""", conn)

		tweets = tweets_collection.find({"user_id_str": : df.iloc[0,0]} , 'created_at': 1, 'retweet_count': 1, 'user_id_str': 1).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)
		df_tweets = pd.DataFrame(list(tweets))
		df_final = df.merge(df_tweets, on = 'user_id_str')
		return df_final
	except Exception as e:
		print(f"Retrieval of Tweet from username failed : {e}")


def get_info_by_tweet(tweet_str, tweet_id):
	try:
		if tweet_str:
			words = word_tokenize(tweet_str)
			non_stop_words = sorted([word for word in words if not word.lower() in stopwords.words()])
			query = {
			    "$and": [
			        {"$and": [{"text": {"$regex": ".*" + word + ".*"}} for word in non_stop_words]},
			        {"oc_tweet_id": ""}
			    ]
			}
			tweets = collection.find(query, {'created_at': 1, 'retweet_count': 1, 'user_id_str': 1, 'oc_tweet_id': 1}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)
		if tweet_id:
			tweets = tweets_collection.find({"oc_tweet_id": tweet_id},{'created_at': 1, 'retweet_count': 1, 'user_id_str': 1, 'hashtag': 1}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)

		df_tweets = pd.DataFrame(list(tweets))

		df_users = query("""SELECT id_str, name FROM users 
			                WHERE
			                id_str = ({','.join(df_tweets.user_id_str.unique())}) """, conn)

		df_final = df_tweets.merge(df_users, on = 'user_id_str') 
		return df_final
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





