import pymongo
from pymongo import MongoClient
import psycopg2
import os
import json

def get_mongo_engine():
    try:
        url = "mongodb+srv://grmongodb:Mongodb321@clustertwitter0.qx1igmo.mongodb.net/?retryWrites=true&w=majority"
        db_name = "twitterdatabase"
        collection_name = "completedata"
        cluster = MongoClient(url)
        db = cluster[db_name]
        collection = db[collection_name]
        return collection
    except Exception as e:
        print(f'Unable to retrieve the connection : {e}')

def create_engine_postgres():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="twitter_db",
            user="postgres",
            password="root",
            port=5433
        )
        return conn
    except Exception as e:
        print(f'Unable to retrieve Postgres Connection :{e}')

def fetch_cache():
    try:    
        print("Fetching Cache")
        cached_data={}
        if(os.path.isfile("CacheFile.json")):
            with open("CacheFile.json","r") as cache_file:
                cached_data = json.load(cache_file)['cached_queries']
        return cached_data
    except Exception as e:
        print(f'Unable to fetch from Cache : {e}')