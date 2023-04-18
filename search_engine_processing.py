import pandas as pd
import psycopg2
import pymongo
from pymongo import MongoClient
from db_connections import get_mongo_engine, create_engine_postgres
import nltk
import string
import json
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

tweet_collection = get_mongo_engine()
conn = create_engine_postgres()

def query(sql_query, conn):
 	df = pd.read_sql_query(sql_query, conn)
 	return df

def fuzzy_matching(search_string):
	result = tweet_collection.aggregate([
        {
            "$search" :{           	               
                "index" : "language_search1",	                
                "compound": {
                "should": [
                    {
					"text": {
					"path": "text",
					"query": search_string,
					"synonyms": "mapping"
					}
				},
				{
				    "text": {
					"path": "text",
					"query": search_string,
					"fuzzy" : {"maxEdits" : 2}
				    }
	      		}
                ]

         	}               
	        }                 
	    },
	    {
        "$match": {
            "oc_tweet_id": ""
                 }
        },
		{
		    "$limit": 10
		},     	        
		{
			"$project": {
			"_id": 0,
			"text" : 1,
			"created_at" : 1,
			"retweet_count" : 1,
			"user_id_str" : 1,
			"user_name" : 1,
			"id_str" : 1,
			"hashtags" : 1,
			"score": {
			    "$meta": "searchScore"
			   } 
			}
		}         
	])
	return result

#def convert_tweets_df(tweets):
def get_info_by_hashtag(hashtags):
	try:
		hashtags = hashtags.split('#')
		query = { "hashtags": { "$in": [hashtags] } }
		tweets = tweet_collection.find(query,{'tweet_str': 1, 'created_at': 1, 'retweet_count': 1, 'user_id_str': 1, 'user_name':1, 'id_str':1, 'hashtags':1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)
		df_final = pd.DataFrame(list(tweets))

		#df_users = query(f"""SELECT id_str, name FROM users 
		#	                WHERE id_str = ({','.join(df_tweets.user_id_str.unique())}) """, conn)

		#df_final = df_tweets.merge(df_users, on = 'user_id_str') 
		#df_final = df_final.drop('user_id_str', axis=1)
		return json.loads(df_final.to_json(orient='records', date_format='iso'))
	except Exception as e:
		print(f"Retrieval of Tweet from hashtags failed : {e}")

def get_info_by_user(user_name):
	try:
		df_users = query(f"""SELECT id_str as user_id_str, name, screen_name, verified, 
		                            followers_count, friends_count, favourites_count, statuses_count,
		                            protected
		                            FROM users 
							WHERE (name LIKE '%{user_name}%') OR (screen_name LIKE '%{user_name}%')
							""", conn)
		#tweets = tweet_collection.find({"user_id_str": df.iloc[0,0]},{'created_at': 1, 'retweet_count': 1, 'user_id_str': 1,  'id_str':1, 'text':1, 'followers_count':1,'friends_count':1, 'hashtags':1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)
		print(df_users)
		#df_final = df.merge(df_tweets, on = 'user_id_str')
		return json.loads(df_users.to_json(orient='records', date_format='iso'))
	except Exception as e:
		print(f"Retrieval of Tweet from username failed : {e}")


def get_info_by_tweet(tweet_str = None, oc_tweet_id = None, tweet_id = None):
	try:
		if tweet_str:
			#words = word_tokenize(tweet_str)
			#non_stop_words = sorted([word for word in words if not (word.lower() in stopwords.words() or word in string.punctuation)])
			#query = {
			#	"$and": [
			#	    {'$text': {'$search': ' '.join(non_stop_words)}},
			#		{"oc_tweet_id": ""}
			#	]
			#}
			#tweets = tweet_collection.find(query, {'created_at': 1, 'retweet_count': 1, 'user_id_str': 1, 'user_name': 1, 'id_str':1, 'text':1, 'hashtags':1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)
		    tweets = fuzzy_matching(tweet_str)
		if oc_tweet_id:
			tweets = tweets_collection.find({"oc_tweet_id": oc_tweet_id},{'created_at': 1,'text':1, 'retweet_count': 1, 'user_id_str': 1, 'user_name': 1, 'hashtags': 1, 'text':1, 'id_str':1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)
		if tweet_id:
			tweets = tweets_collection.find({"id_str": tweet_id},{'created_at': 1,'text':1, 'retweet_count': 1, 'user_id_str': 1, 'user_name': 1, 'hashtags': 1, 'text':1, 'id_str':1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)	
		
		df_final = pd.DataFrame(list(tweets))
		print(df_final)

		#df_users = query("""SELECT id_str, name FROM users 
			             #  WHERE
			              #   id_str = ({','.join(df_tweets.user_id_str.unique())}) """, conn)

		#df_final = df_tweets.merge(df_users, on = 'user_id_str') 
		return json.loads(df_final.to_json(orient='records', date_format='iso'))
	except Exception as e:
		print(f"Retrieval of Tweet Failed : {e}")