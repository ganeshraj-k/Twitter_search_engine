#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json


# In[26]:


file_name = "corona-out-3" # read input file
f  = open(file_name , 'r')    
lines = f.readlines()

tweets = {}
tweets['tweets'] = []

for line in lines:
    if line[0] == '{':
        tweets['tweets'].append(json.loads(line))  # load tweets from input file


# In[25]:


file2 = "completedata.json"    # final json file
fileread = open(file2, 'w')
for tweet in tweets['tweets']:
    fileread.write(json.dumps(tweet))    #writing to the json file
    fileread.write("\n"


# In[21]:




