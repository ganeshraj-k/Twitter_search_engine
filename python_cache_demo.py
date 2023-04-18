#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install thefuzz')


# In[131]:


from thefuzz import process, fuzz


# In[156]:


import json
import pandas as pd 
import os

def fetch_cache():
    cached_data=[]
    if(os.path.isfile("CacheFile.json")):
        with open("CacheFile.json","r") as cache_file:
            cached_data = json.load(cache_file)['cached_queries']
    return cached_data

def get_top_cache_data(cached_data):
    cached_queries=[]
    potential_cached_queries=[]
    if(bool(cached_data)):
        counter=0
        for key in cached_data:
            cached_queries.append(key) if counter<5 else potential_cached_queries.append(key)
            counter+=1
    return cached_queries,potential_cached_queries


# In[157]:


def Search_Cache(search_string):
    cached_data = fetch_cache()
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
                if(match_query[1]>50):
                    cached_data[match_query[0]]['counter']+=1
                    #Calling refresh
                    search_result=[]
                    cached_data = refresh(cached_data)
                    update_cache(cached_data)
                    return search_result
        return search_result;

def Write_Cache(search_string,search_result):
    cached_data = fetch_cache()
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
       


# In[164]:


search_string = "top billboard artists"
result = [
                {
                    "created_at": "2020-04-12 18:27:32",
                    "id_str": "1249403795643043840",
                    "text": "This Monday I\u2019m Dj\u2019ing a vinyl set on Diggers Factory to gather money for Corona Virus  research. A lot of artists\u2026 https://t.co/9D5A5841sS",
                    "retweet_count": 0,
                    "user_id_str": "2983288664",
                    "user_name": "John Tejada",
                    "hashtags": [],
                    "score": 5.943684101104736
                },
                {
                    "created_at": "2020-04-12 18:27:43",
                    "id_str": "1249403843009368064",
                    "text": "RT @himdaughter: Be worried, be very worried.\nCorona not a disease for the underprivileged anywhere in the world.\nCalifornia Tops Two Milli\u2026",
                    "retweet_count": 0,
                    "user_id_str": "90804011",
                    "user_name": "Dr Soumitra Pathare",
                    "hashtags": [],
                    "score": 5.823451995849609
                },
                {
                    "created_at": "2020-04-12 18:08:42",
                    "id_str": "1249399056272707591",
                    "text": "Be worried, be very worried.\nCorona not a disease for the underprivileged anywhere in the world.\nCalifornia Tops Tw\u2026 https://t.co/sDmvkqDG2o",
                    "retweet_count": 1,
                    "user_id_str": "551910958",
                    "user_name": "Shailja",
                    "hashtags": [],
                    "score": 5.823451995849609
                },
                {
                    "created_at": "2020-04-12 18:28:08",
                    "id_str": "1249403945660715010",
                    "text": "RT @mollycrabapple: This little boy looks like he\u2019s ten tops, and these big officers, some with no masks, are traumatizing him\n\nDe Blasio s\u2026",
                    "retweet_count": 0,
                    "user_id_str": "56831437",
                    "user_name": "Brandon Davis",
                    "hashtags": [],
                    "score": 5.750097274780273
                },
                {
                    "created_at": "2020-04-12 14:48:48",
                    "id_str": "1249348749043867653",
                    "text": "This little boy looks like he\u2019s ten tops, and these big officers, some with no masks, are traumatizing him\n\nDe Blas\u2026 https://t.co/69bQYvMxDM",
                    "retweet_count": 209,
                    "user_id_str": "15644999",
                    "user_name": "Molly Crabapple\ud83c\uddf5\ud83c\uddf7",
                    "hashtags": [],
                    "score": 5.750097274780273
                },
                {
                    "created_at": "2020-04-12 18:29:31",
                    "id_str": "1249404294291390464",
                    "text": "RT @Mckbenna: The fact that they were already in such a severe lockdown, we can't even compare. And having Corona on top of that and then T\u2026",
                    "retweet_count": 0,
                    "user_id_str": "740121769646907393",
                    "user_name": "hadiya butt",
                    "hashtags": [],
                    "score": 5.671999931335449
                },
                {
                    "created_at": "2020-04-12 18:27:05",
                    "id_str": "1249403682262863872",
                    "text": "The fact that they were already in such a severe lockdown, we can't even compare. And having Corona on top of that\u2026 https://t.co/PkYOhuvwEw",
                    "retweet_count": 1,
                    "user_id_str": "1085761150313213952",
                    "user_name": "Ashi",
                    "hashtags": [],
                    "score": 5.671999931335449
                },
                {
                    "created_at": "2020-04-12 18:27:33",
                    "id_str": "1249403799984394250",
                    "text": "RT @hemirdesai: How will u do shooting pro #CoronaVirus when 50 or less will be allowed. U can cast @taapsee n secular artists as they thin\u2026",
                    "retweet_count": 0,
                    "user_id_str": "1156634363393708033",
                    "user_name": "#Dev Oza | \u0926\u0947\u0935 |",
                    "hashtags": [
                        "CoronaVirus"
                    ],
                    "score": 5.53331184387207
                },
                {
                    "created_at": "2020-04-11 13:40:32",
                    "id_str": "1248969182026190849",
                    "text": "How will u do shooting pro #CoronaVirus when 50 or less will be allowed. U can cast @taapsee n secular artists as t\u2026 https://t.co/lwAwhyNO4P",
                    "retweet_count": 45,
                    "user_id_str": "165641975",
                    "user_name": "Hemir Desai",
                    "hashtags": [
                        "CoronaVirus"
                    ],
                    "score": 5.408830165863037
                },
                {
                    "created_at": "2020-04-12 18:28:11",
                    "id_str": "1249403960315858948",
                    "text": "Then please tell me again the UK is not racist.",
                    "retweet_count": 0,
                    "user_id_str": "1686687122",
                    "user_name": "Beatrice \ud83c\udf39",
                    "hashtags": [],
                    "score": 2.2354636192321777
                }
            ]
        


# In[172]:


#search_string = "Joseph for president"
#search_string ="global warming"
#search_string ="caSes on the rise"
search_string = "billboard artists"


# In[173]:


search_string


# In[175]:


search_result=Search_Cache(search_string)
print(search_result)

if(search_result==None):
    Write_Cache(search_string,result)


# In[184]:


import pandas as pd
import re
from string import punctuation
from nltk.tokenize import word_tokenize, sent_tokenize
import contractions
from nltk.corpus import stopwords
from pymongo import MongoClient
import json
import pymongo


# In[255]:


url = "mongodb+srv://grmongodb:Mongodb321@clustertwitter0.qx1igmo.mongodb.net/?retryWrites=true&w=majority"
db_name = "twitterdatabase"
collection_name = "collection001"

cluster = MongoClient(url)
db = cluster[db_name]
collection = db[collection_name]


# In[256]:


collection.name


# In[203]:


cluster.close()


# In[230]:


stopwords = stopwords.words('english')


# In[133]:


punctuation


# In[130]:


import nltk
nltk.download("stopwords")
nltk.download("punkt")


# In[2]:


s = input()


# In[3]:


print(s)


# In[136]:


def preprocess_tokenize(text):
    text = text.lower()  # Lowercase text
    text = contractions.fix(text)
    
    text = re.sub(f"[{re.escape(punctuation)}]", " ", text)  # Remove punctuation
    
    text = contractions.fix(text)
    text = " ".join(text.split())  # Remove extra spaces, tabs, and new lines
    tokens =  word_tokenize(text)
    return [token for token in tokens if token not in stopwords and token not in punctuation]


# In[138]:


preprocess_tokenize("your brain's not working")


# In[25]:


cl = preprocess_tokenize("this is america-19 this //")


# In[27]:


cl2 = ['this', 'is', 'america', '19', 'this' , 'donald', 'glover']


# In[35]:


def perc_match(l1, l2):
    nm = set(l1).intersection(l2)
    return len(nm)/len(l2)


# In[43]:


cache_main_dict = {
    
    "'this', 'is', 'america', '19', 'this' , 'donald', 'glover'" : {'score' : 2 , 'out' : ''} 
 }


# In[47]:


dic["'this', 'is', 'america', '19', 'this' , 'donald', 'glover'"]['score']


# In[ ]:


# step 1 fill the dictionary
# step 2 make a function to increment and retrieve 
# step 


# In[55]:


file = "cache_text_input.txt"


# In[139]:


in_list  = []
file_read = open(file, 'r', encoding = 'utf-8')
lines = file_read.readlines()
sent_tokenize(lines[0])
for l in lines:
    if len(l) <= 2:
        print("skip")
        continue
    sentences = sent_tokenize(l)
    for sen in sentences:
        in_list.append(preprocess_tokenize(str (sen)) )


# In[164]:


in_list


# In[161]:


dict_cache = {}
for item in in_list:
    dict_cache[str(item)] = { 'score' : 1 , 'output' : item }
    


# In[169]:


search_str = "pie is on top of my list"


# In[253]:



def search_retrieve(s):
    tokens = preprocess_tokenize(s)

    for key in list(dict_cache.keys()):
        if perc_match( tokens, eval(key)) > 0.4:
            dict_cache[key]['score'] = dict_cache[key]['score'] + 1
            print(dict_cache[key]['output'] ,dict_cache[key]['score'] )
    cache_main = refresh()
            
            


# In[181]:


sorted(dict_cache.items() , key = lambda x : x[1]['score'], reverse = True)


# In[221]:



def refresh():
    dict_cache_small = {}
    len_small = 50
    dict_big = dict(sorted(dict_cache.items() , key = lambda x : x[1]['score'], reverse = True))
    keys = list(dict_big.keys())
    for i in range(0,len_small ):
        
        dict_cache_small[keys[i]] = dict_big[keys[i]]
    return dict_cache_small
    


# In[223]:


cache_main = refresh()


# In[244]:


search_string  = input()


# In[250]:


search_retrieve(search_string)


# In[254]:


cache_main


# In[ ]:





# In[300]:


def get_info_by_tweet(tweet_str = None):
    try:
        if tweet_str:
            words = word_tokenize(tweet_str)
            non_stop_words = sorted([word for word in words if not (word.lower() in stopwords or word in punctuation)])
            query = {
                "$and": [
                     {'$text': {'$search': ' '.join(non_stop_words)}},
                     {"oc_tweet_id": ""}
                ]
            }
            
            tweets = collection.find(query, {'created_at': 1, 'retweet_count': 1, 'user_id_str': 1, 'oc_tweet_id': 1, '_id': 0}).sort([('retweet_count', pymongo.DESCENDING)]).limit(10)
            return tweets
    except Exception as e:
        print(e)
        
            


# In[308]:


tweets = get_info_by_tweet("thankful for staff risking their life for maintaining the telecom service")


# In[310]:


for t in tweets:
    print(t)

