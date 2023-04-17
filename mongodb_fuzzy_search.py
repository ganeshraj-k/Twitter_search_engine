def fuzzy_matching2(search_string):
    result = collection.aggregate([
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
            
            
            
            
            
    }
      ,
        
        
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