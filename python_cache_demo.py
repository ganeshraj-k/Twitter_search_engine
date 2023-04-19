
from thefuzz import process, fuzz
from db_connections import fetch_cache
import json
import pandas as pd 
import os

cached_data = fetch_cache()

def get_top_cache_data(cached_data):
    cached_queries=[]
    potential_cached_queries=[]
    if(bool(cached_data)):
        counter=0
        for key in cached_data:
            cached_queries.append(key) if counter<5 else potential_cached_queries.append(key)
            counter+=1
    return cached_queries,potential_cached_queries

def Search_Cache(search_string):
    global cached_data
    search_result=[]
    if(bool(cached_data)):
        cached_queries, potential_cached_queries = get_top_cache_data(cached_data)
        if(bool(cached_queries)):
            match_query = process.extractOne(search_string, cached_queries , scorer = fuzz.token_set_ratio)
            if(match_query[1]>50):
                cached_data[match_query[0]]['counter']+=1
                search_result = cached_data[match_query[0]]['result']
                #Calling refresh
                cached_data = refresh(cached_data)
                update_cache(cached_data)
                return search_result
            elif(potential_cached_queries):
                match_query = process.extractOne(search_string, potential_cached_queries)
                if(match_query[1]>50): ## potential error
                    cached_data[match_query[0]]['counter']+=1
                    #Calling refresh
                    cached_data = refresh(cached_data)
                    update_cache(cached_data)
                    return search_result
        return search_result

def Write_Cache(search_string,search_result):
    global cached_data
    if(len(cached_data)<20):
        cached_data[search_string] = {
            "counter":1,
            "result":search_result
        }
        cached_data = refresh(cached_data)
        update_cache(cached_data)
            
# Refresh the dictionary with sorted count
def refresh(cached_data):
    dict_cache_small = {}
    len_small = len(cached_data)
    dict_big = dict(sorted(cached_data.items() , key = lambda x : x[1]['counter'], reverse = True))
    keys = list(dict_big.keys())
    for i in range(0,len_small ):
        
        dict_cache_small[keys[i]] = dict_big[keys[i]]
    return dict_cache_small


# Update the cache file on RAM with latest score
def update_cache(cached_data):
    cached_data_write={"cached_queries":cached_data}
    json_string = json.dumps(cached_data_write)
    with open("CacheFile.json","w") as cache_file:
        cache_file.write(json_string);
       




