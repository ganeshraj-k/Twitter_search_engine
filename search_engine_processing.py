import pandas as pd
import psycopg2
import pymongo
from pymongo import MongoClient
from db_connections import get_mongo_engine, create_engine_postgres
import python_cache
import nltk
import string
import json
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

#Get connections for datastores
tweet_collection = get_mongo_engine()
conn = create_engine_postgres()

#Function to read the query
def query(sql_query, conn):
    df = pd.read_sql_query(sql_query, conn)
    return df

#Function to implement fuzzy matching and sort by score
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

#Function to get results of hashtag
def get_info_by_hashtag(hashtags, fromClick = False, toDate=None, fromDate=None):
    '''
    Input : hashtag name, toDate : to filter by start date, fromDate: to filter by end date
    result: list of Tweets having hashtags
    '''
    try:
        cached_result = python_cache.Search_Cache(hashtags)
        if cached_result[0] == []:
            print('NOT FROM CACHE')
            if fromClick:
                hashtags_list = [hashtags]
            else:
                hashtags_list = hashtags.split('#')
            query = { "hashtags": { "$in": [hashtags_list] } }
            tweets = tweet_collection.find(query,{'tweet_str': 1, 'created_at': 1, 'retweet_count': 1, 'user_id_str': 1, 'user_name':1, 'text':1, 'id_str':1, 'hashtags':1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)
            df_final = pd.DataFrame(list(tweets))
            if toDate:
                	df_final = df_final.loc[(df_final['created_at'] >= toDate) & (df_final['created_at'] <= fromDate)]
            hashtag_output =  json.loads(df_final.to_json(orient='records', date_format='iso'))
            if cached_result[1]==0:
                python_cache.Write_Cache(hashtags, hashtag_output)
            return hashtag_output
        else:
            print('FROM CACHE')
            return cached_result[0]
    except Exception as e:
        print(f"Retrieval of Tweet from hashtags failed : {e}")
        
#Function to get information of user
def get_info_by_user(user_name = None , user_id = None):
    '''
    Input : user name if clicked on username in tweets, userid if searched with @
    result: information of the user
    '''
    try:
        if user_name:
            cached_result = python_cache.Search_Cache(user_name)
            if cached_result[0] == []:
                print('NOT FROM CACHE')
                df_users = query(f"""SELECT id_str as user_id_str, name, screen_name, verified, 
                                    followers_count, friends_count, favourites_count, statuses_count,
                                    protected
                                FROM users 
                                WHERE (name LIKE '{user_name}') OR (screen_name LIKE '{user_name}')
                             """, conn)
                users_output = json.loads(df_users.to_json(orient='records', date_format='iso'))
                if cached_result[1]==0:
                    python_cache.Write_Cache(user_name, users_output)
                return users_output
            else:
                print('FROM CACHE')
                return cached_result[0]
        elif user_id:
            tweets = tweet_collection.find({'user_id_str':user_id},{'created_at': 1, 'retweet_count': 1, 'user_id_str': 1,  'id_str':1, 'text':1, 'followers_count':1,'friends_count':1, 'hashtags':1, 'user_name':1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)
            df_final = pd.DataFrame(list(tweets))
            return json.loads(df_final.to_json(orient='records', date_format='iso'))
    except Exception as e:
        print(f"Retrieval of Tweet from username failed : {e}")

#Function to get the tweets based on tweetid or search string or dates
def get_info_by_tweet(tweet_str = None, oc_tweet_id = None, tweet_id = None, toDate=None, fromDate=None):
    '''
    Input : search string, original tweet id if searched for retweets,
    toDate : to filter by start date, fromDate: to filter by end date
    result: list of Tweets having matching the search string
    '''
    try:
        if tweet_str:
            cached_result = python_cache.Search_Cache(tweet_str)
            if cached_result[0] == []:
                print('NOT FROM CACHE')
                tweets = fuzzy_matching(tweet_str)
                df_final = pd.DataFrame(list(tweets))
                if toDate:
                	df_final = df_final.loc[(df_final['created_at'] >= toDate) & (df_final['created_at'] <= fromDate)]
                tweet_output = json.loads(df_final.to_json(orient='records', date_format='iso'))
                if cached_result[1]==0:
                    python_cache.Write_Cache(tweet_str, tweet_output)
                return tweet_output
            else:
                print('FROM CACHE')
                return cached_result[0]

        if oc_tweet_id:
            tweets = tweet_collection.find({"oc_tweet_id": oc_tweet_id},{'created_at': 1,'text':1, 'retweet_count': 1, 'user_id_str': 1, 'user_name': 1, 'hashtags': 1, 'text':1, 'id_str':1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)    

        if tweet_id:
            tweets = tweet_collection.find({"id_str": tweet_id},{'created_at': 1,'text':1, 'retweet_count': 1, 'user_id_str': 1, 'user_name': 1, 'hashtags': 1, 'text':1, 'id_str':1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)

        df_final = pd.DataFrame(list(tweets))
        return json.loads(df_final.to_json(orient='records', date_format='iso'))
    except Exception as e:
        print(f"Retrieval of Tweet Failed : {e}")


#Function to get top 10 users or top 10 tweets
def get_top_10_details(top10):
    '''
    Input : tweets or user
    result: list of Tweets having matching the search string
    '''
    try:
        if top10 == 'tweets':
            cached_result = python_cache.Search_Cache('top_10_tweets')
            if cached_result[0] == []:
                tweets = tweet_collection.find({}, {'created_at': 1,'text':1, 'retweet_count': 1, 'user_id_str': 1, 'user_name': 1, 'hashtags': 1, 'text':1, 'id_str':1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)   
                df_final = pd.DataFrame(list(tweets))
                tweet_output = json.loads(df_final.to_json(orient='records', date_format='iso'))
                if cached_result[1]==0:
                    python_cache.Write_Cache('top_10_tweets', tweet_output)
                return tweet_output
            else:
                return cached_result[0]
        elif top10 == 'users':
            cached_result = python_cache.Search_Cache('top_10_users')
            if cached_result[0] == []:
                df_final = query(f"""SELECT id_str as user_id_str, name, screen_name, verified, created_at
                                        followers_count, friends_count, favourites_count, statuses_count,
                                        protected
                                        FROM users 
                                        ORDER BY followers_count DESC 
                                        LIMIT 10
                                    """, conn)
                user_output = json.loads(df_final.to_json(orient='records', date_format='iso'))
                if cached_result[1]==0:
                    python_cache.Write_Cache('top_10_users', user_output)
                return user_output
            else:
                return cached_result[0]
        else:
            pass
    except Exception as e:
        print(f"Retrieval of 10 ten {top10} failed")














