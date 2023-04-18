import pymongo
from pymongo import MongoClient
import psycopg2

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