#!/usr/bin/env python
# coding: utf-8

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

