import pandas as pd
import psycopg2
import pymongo
from pymongo import MongoClient
from db_connections import get_mongo_engine, create_engine_postgres
import python_cache_demo
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
def get_info_by_hashtag(hashtags, comma_separated = False):
    try:
        cached_result = python_cache_demo.Search_Cache(hashtags)
        if cached_result[0] == []:
            if comma_separated:
                hashtags_list = hashtags.split('#')
            else:
                hashtags_list = hashtags.split('#')
            query = { "hashtags": { "$in": [hashtags_list] } }
            tweets = tweet_collection.find(query,{'tweet_str': 1, 'created_at': 1, 'retweet_count': 1, 'user_id_str': 1, 'user_name':1, 'tweet':1, 'id_str':1, 'hashtags':1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)
            df_final = pd.DataFrame(list(tweets))
            hashtag_output =  json.loads(df_final.to_json(orient='records', date_format='iso'))
            if cached_result[1]==0:
                python_cache_demo.Write_Cache(hashtags, hashtag_output)
            return hashtag_output
        else:
            return cached_result[0]

        #df_users = query(f"""SELECT id_str, name FROM users 
        #	                WHERE id_str = ({','.join(df_tweets.user_id_str.unique())}) """, conn)

        #df_final = df_tweets.merge(df_users, on = 'user_id_str') 
        #df_final = df_final.drop('user_id_str', axis=1)
    except Exception as e:
        print(f"Retrieval of Tweet from hashtags failed : {e}")

def get_info_by_user(user_name = None , user_id = None):
	try:
		if user_name:
			cached_result = python_cache_demo.Search_Cache(user_name)
			if cached_result[0] == []:
				df_users = query(f"""SELECT id_str as user_id_str, name, screen_name, verified, 
				                    followers_count, friends_count, favourites_count, statuses_count,
				                    protected
				                FROM users 
				                WHERE (name LIKE '%{user_name}%') OR (screen_name LIKE '%{user_name}%')
				             """, conn)
				users_output = json.loads(df_users.to_json(orient='records', date_format='iso'))
				if cached_result[1]==0:
					python_cache_demo.Write_Cache(user_name, users_output)
				return users_output
			else:
				return cached_result[0]
		elif user_id:
			tweets = tweet_collection.find({'user_id_str':user_id},{'created_at': 1, 'retweet_count': 1, 'user_id_str': 1,  'id_str':1, 'text':1, 'followers_count':1,'friends_count':1, 'hashtags':1, 'user_name':1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)
			df_final = pd.DataFrame(list(tweets))
			return json.loads(df_final.to_json(orient='records', date_format='iso'))

        #tweets = tweet_collection.find({"user_id_str": df.iloc[0,0]},{'created_at': 1, 'retweet_count': 1, 'user_id_str': 1,  'id_str':1, 'text':1, 'followers_count':1,'friends_count':1, 'hashtags':1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)
        #df_final = df.merge(df_tweets, on = 'user_id_str')
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
            cached_result = python_cache_demo.Search_Cache(tweet_str)
            if cached_result[0] == []:
                tweets = fuzzy_matching(tweet_str)
                df_final = pd.DataFrame(list(tweets))
                tweet_output = json.loads(df_final.to_json(orient='records', date_format='iso'))
                if cached_result[1]==0:
                    python_cache_demo.Write_Cache(tweet_str, tweet_output)
                return tweet_output
            else:
                return cached_result[0]

        if oc_tweet_id:
            tweets = tweet_collection.find({"oc_tweet_id": oc_tweet_id},{'created_at': 1,'text':1, 'retweet_count': 1, 'user_id_str': 1, 'user_name': 1, 'hashtags': 1, 'text':1, 'id_str':1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)		    

        if tweet_id:
            tweets = tweet_collection.find({"id_str": tweet_id},{'created_at': 1,'text':1, 'retweet_count': 1, 'user_id_str': 1, 'user_name': 1, 'hashtags': 1, 'text':1, 'id_str':1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)

        df_final = pd.DataFrame(list(tweets))
        #print(df_final)

        #df_users = query("""SELECT id_str, name FROM users 
                         #  WHERE
                          #   id_str = ({','.join(df_tweets.user_id_str.unique())}) """, conn)

        #df_final = df_tweets.merge(df_users, on = 'user_id_str') 
        return json.loads(df_final.to_json(orient='records', date_format='iso'))
    except Exception as e:
        print(f"Retrieval of Tweet Failed : {e}")


def get_top_10_details(top10):
	try:
		if top10 == 'tweets':
			cached_result = python_cache_demo.Search_Cache('top_10_tweets')
			if cached_result[0] == []:
				tweets = tweet_collection.find({}, {'created_at': 1,'text':1, 'retweet_count': 1, 'user_id_str': 1, 'user_name': 1, 'hashtags': 1, 'text':1, 'id_str':1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)   
				df_final = pd.DataFrame(list(tweets))
				tweet_output = json.loads(df_final.to_json(orient='records', date_format='iso'))
				if cached_result[1]==0:
					python_cache_demo.Write_Cache('top_10_tweets', tweet_output)
				return tweet_output
			else:
				return cached_result[0]
		elif top10 == 'users':
			cached_result = python_cache_demo.Search_Cache('top_10_users')
			if cached_result[0] == []:
				df_final = query(f"""SELECT id_str as user_id_str, name, screen_name, verified, 
										followers_count, friends_count, favourites_count, statuses_count,
										protected
										FROM users 
										ORDER BY followers_count DESC 
										LIMIT 10
									""", conn)
				user_output = json.loads(df_final.to_json(orient='records', date_format='iso'))
				if cached_result[1]==0:
					python_cache_demo.Write_Cache('top_10_users', user_output)
				return user_output
			else:
				return cached_result[0]
		else:
			pass
	except Exception as e:
		print(f"Retrieval of 10 ten {top10} failed")














