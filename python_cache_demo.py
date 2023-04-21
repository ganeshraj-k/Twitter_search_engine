
from thefuzz import process, fuzz
import threading
from db_connections import fetch_cache
import json
import pandas as pd 
import os
import time

global cached_data
cached_data = fetch_cache()

# Update the cache file on RAM with latest score
def update_cache():
    global cached_data
    print('Saving Data to File')
    cached_data_write={"cached_queries":cached_data}
    json_string = json.dumps(cached_data_write)
    with open("CacheFile.json","w") as cache_file:
        cache_file.write(json_string);

class StoreCache(threading.Thread):
    def __init__(self, seconds):
        super().__init__()
        self.delay = seconds
        self.is_done = False

    def done(self):
        self.is_done = True

    def run(self):
        while not self.is_done:
            time.sleep(self.delay)
            update_cache()

t = StoreCache(60)
t.start()

def get_top_cache_data():
    global cached_data
    cached_queries=[]
    potential_cached_queries=[]
    if(bool(cached_data)):
        counter = 0
        for key in cached_data:
            cached_queries.append(key) if counter<5 else potential_cached_queries.append(key)
            counter+=1
    return cached_queries,potential_cached_queries

def Search_Cache(search_string):
    global cached_data
    search_result=[[],0]
    if(bool(cached_data)):
        cached_queries, potential_cached_queries = get_top_cache_data()
        if(bool(cached_queries)):
            match_query = process.extractOne(search_string, cached_queries , scorer = fuzz.token_set_ratio)
            if(((search_string == 'top_10_tweets' and match_query[1]>74) | (search_string == 'top_10_users' and match_query[1]>64))):
                
                cached_data[search_string]['counter']+=1
                search_result[0] = cached_data[search_string]['result']
                #Calling refresh
                cached_data = refresh()
                search_result[1]=1
                return search_result
            elif(search_string != 'top_10_tweets' and search_string != 'top_10_users'):
                if(match_query[1]>50):
                    cached_data[match_query[0]]['counter']+=1
                    search_result[0] = cached_data[match_query[0]]['result']
                    #Calling refresh
                    cached_data = refresh()
                    search_result[1]=1
                    return search_result
                elif(potential_cached_queries):
                    match_query = process.extractOne(search_string, potential_cached_queries)
                    if(match_query[1]>50): ## potential error
                        cached_data[match_query[0]]['counter']+=1
                        #Calling refresh
                        cached_data = refresh()
                        search_result[1]=1
                        return search_result
    print("The search Result : ",search_result)
    return search_result

def Write_Cache(search_string,search_result):
    global cached_data
    if(len(cached_data)<20):
        cached_data[search_string] = {
            "counter":1,
            "result":search_result
        }
        cached_data = refresh()
            
# Refresh the dictionary with sorted count
def refresh():
    global cached_data
    dict_cache_small = {}
    len_small = len(cached_data)
    dict_big = dict(sorted(cached_data.items() , key = lambda x : x[1]['counter'], reverse = True))
    keys = list(dict_big.keys())
    for i in range(0,len_small ):
        dict_cache_small[keys[i]] = dict_big[keys[i]]
    return dict_cache_small
       




