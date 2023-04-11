import pandas as pd
import psycopg2
from pymongo import MongoClient

def get_mongo_engine():
	url = "mongodb+srv://grmongodb:Mongodb321@clustertwitter0.qx1igmo.mongodb.net/?retryWrites=true&w=majority"
	db_name = "twitterdatabase"
	collection_name = "collection001"
	cluster = MongoClient(url)
	db = cluster[db_name]
	collection = db[collection_name]
	return collection

def create_engine_postgres(host, database, user, password, port):
	conn = psycopg2.connect(
    host="your_host",
    database="your_database_name",
    user="your_username",
    password="your_password",
    port="your_port"
  )
  return conn


def query(sql_query, conn):
 	df = pd.read_sql_query(sql_query, conn)
 	return df

def get_tweet_info_by_hashtag(hashtag, conn):


def get_tweet_info_by_user(user_name, conn):
 	df = query(f"""SELECT id_str FROM users 
 		            WHERE name =  {user_name}""", conn)

 	tweets = collection.find({"user_id_str": : df.iloc[0,0]})


def get_tweet_info_by_tweet(tweet_str, conn):
 	tweets = collection.find({"text": {"$regex": tweet_str}})
 	df_tweets = pd.DataFrame(list(tweets))

 	df_users = query("""SELECT id_str, name FROM users 
 		                 WHERE
 		                  id_str = ({','.join(df_tweets.user_id_str.unique())}) """, conn)
   
    df_final = df_tweets.merge(df_users, on = 'user_id_str') 

def get_retweet_info(tweet_id, conn):
	retweets = collection.find({"oc_tweet_id": tweet_id})
	df_retweets = pd.DataFrame(list(tweets))

 	df _users= query("""SELECT id_str as user_id_str, name FROM users 
 		                WHERE 		             
 		                  id_str = ({','.join(f_tweets.user_id_str.unique())}) """, conn)

    df_final = df_retweets.merge(df_users, on = 'user_id_str')



def main():
 	conn = create_engine()
 	coll = create_mongo_engine()
 	## Retrieve string from search
 	tweet_str = get()
 	get_tweet_information(tweet_str, conn, coll)

