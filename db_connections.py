import pymongo
from pymongo import MongoClient
import psycopg2
import os
import json

def get_mongo_engine():
    try:
        url = "mongodb+srv://grmongodb:Mongodb321@clustertwitter0.qx1igmo.mongodb.net/?retryWrites=true&w=majority"
        db_name = "twitterdatabase"
        collection_name = "collection001"
        cluster = MongoClient(url)
        db = cluster[db_name]
        collection = db[collection_name]
        return collection
    except Exception as e:
        print(f'Unable to retrieve the connection : {e}')

def create_engine_postgres():
    conn = psycopg2.connect(
        host="localhost",
        database="TwitterDatabase",
        user="postgres",
        password="India@2194",
        port=5432
    )
    return conn

def fetch_cache():
    print("Fetching Cache")
    cached_data=[]
    if(os.path.isfile("CacheFile.json")):
        with open("CacheFile.json","r") as cache_file:
            cached_data = json.load(cache_file)['cached_queries']
    return cached_data