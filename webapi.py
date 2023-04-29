from flask import Flask, request,render_template
from search_engine_processing import get_info_by_tweet, get_info_by_hashtag, get_info_by_user, get_top_10_details
import time

app=Flask(__name__)

#Route to get info by hashtag/tweets/user based on search string
@app.route('/search',methods=['POST'])
def search():
    userdata=""
    data = ""
    toDate=""
    fromDate=""
    search_str = request.form['searchstring']
    toDate = request.form['toDate']
    fromDate = request.form['fromDate']
    if search_str.startswith('@'):
        search_str = search_str[1:]
        start_time = time.time()  # record the start time
        userdata = get_info_by_user(search_str)
        end_time = time.time() 
        elapsed_time = end_time - start_time
        userdata = [{'elapsed_time':elapsed_time},userdata]
    # record the end time
    elif search_str.startswith('#'):
        start_time = time.time()  #
        data = get_info_by_hashtag(search_str[1:], toDate=toDate,fromDate=fromDate)
        end_time = time.time()  # record the end time
        elapsed_time = end_time - start_time\
        data = [{'elapsed_time':elapsed_time},data]
    else:
        start_time = time.time()  #
        data = get_info_by_tweet(tweet_str = search_str, toDate=toDate,fromDate=fromDate)
        end_time = time.time()  # record the end time
        elapsed_time = end_time - start_time
        data = [{'elapsed_time':elapsed_time},data]
    return render_template('mypage.html',data=data,userdata=userdata)

#Route to get info by user when clicked on username
@app.route('/user',methods=['GET'])
def getuser():
    start_time = time.time()  #
    uname = request.args.get('user')
    userdata = get_info_by_user(user_name=uname)
    end_time = time.time()  # record the end time
    elapsed_time = end_time - start_time
    finalData = [{'elapsed_time':elapsed_time},userdata]
    return render_template('mypage.html',userdata=finalData,data="")

#Route to get tweets made by user when tweet counts
@app.route('/userTweet', methods=["GET"])
def getTweetsByUser():
    uid = request.args.get('userid')
    tweetdata = get_info_by_user(user_id = uid)
    return render_template('mypage.html',userdata="",data=tweetdata)

#Route to get retweets of the tweet
@app.route('/retweets', methods=["GET"])
def getretweetsbyTweetID():
    octweetid = request.args.get('tweetid')
    tweetdata = get_info_by_tweet(oc_tweet_id=octweetid)
    return render_template('mypage.html',userdata="",data=tweetdata)

#Route to get info by hashtag when clicked on hashtag
@app.route('/hashtag',methods=['GET'])
def getTweetsbyHashtag():
    hashtag = request.args.get('hashtag')
    tweetdata = get_info_by_tweet(hashtag,True)
    return render_template('mypage.html',userdata="",data=tweetdata)

#Route to get top10 user/tweets
@app.route('/topDetails',methods=['GET'])
def getTopDetails():
    data=""
    reqType = request.args.get('reqtype')
    start_time = time.time()  #
    data = get_top_10_details(reqType)
    end_time = time.time()  # record the end time
    elapsed_time = end_time - start_time
    finalData = [{'elapsed_time':elapsed_time},data]
    #data.append({'elapsed_time':elapsed_time})
    if reqType=="tweets":
        return render_template('mypage.html',userdata="",data=finalData)
    elif reqType=="users":
        return render_template('mypage.html',userdata=finalData,data="")
    
#Route to get landing screen
@app.route('/')
def load():
    return render_template('mypage.html',userdata="",data="")

if __name__ == '__main__':
    app.run(debug=True,host ='localhost',port = 5001)