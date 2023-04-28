from flask import Flask, request,render_template
from search_engine_processing import get_info_by_tweet, get_info_by_hashtag, get_info_by_user, get_top_10_details
import time

app=Flask(__name__)

@app.route('/search',methods=['POST'])
def search():
    userdata=""
    data = ""
    search_str = request.form['searchstring']
    if search_str.startswith('@'):
        search_str = search_str[1:]
        start_time = time.time()  # record the start time
        userdata = get_info_by_user(search_str)
        end_time = time.time() 
        elapsed_time = end_time - start_time
        userdata.append({'elapsed_time':elapsed_time})
    # record the end time
    elif search_str.startswith('#'):
        start_time = time.time()  #
        data = get_info_by_hashtag(search_str[1:])
        end_time = time.time()  # record the end time
        elapsed_time = end_time - start_time
        data.append({'elapsed_time':elapsed_time})
    else:
        start_time = time.time()  #
        data = get_info_by_tweet(tweet_str = search_str)
        end_time = time.time()  # record the end time
        elapsed_time = end_time - start_time
        data.append({'elapsed_time':elapsed_time})
    return render_template('mypage.html',data=data,userdata=userdata)

@app.route('/user',methods=['GET'])
def getuser():
    start_time = time.time()  #
    uname = request.args.get('user')
    userdata = get_info_by_user(user_name=uname)
    end_time = time.time()  # record the end time
    elapsed_time = end_time - start_time
    userdata.append({'elapsed_time':elapsed_time})
    return render_template('mypage.html',userdata=userdata,data="")

@app.route('/userTweet', methods=["GET"])
def getTweetsByUser():
    uid = request.args.get('userid')
    tweetdata = get_info_by_user(user_id = uid)
    return render_template('mypage.html',userdata="",data=tweetdata)

@app.route('/retweets', methods=["GET"])
def getretweetsbyTweetID():
    octweetid = request.args.get('tweetid')
    tweetdata = get_info_by_tweet(oc_tweet_id=octweetid)
    return render_template('mypage.html',userdata="",data=tweetdata)

@app.route('/hashtag',methods=['GET'])
def getTweetsbyHashtag():
    hashtag = request.args.get('hashtag')
    tweetdata = get_info_by_tweet(hashtag,True)
    return render_template('mypage.html',userdata="",data=tweetdata)

@app.route('/topDetails',methods=['GET'])
def getTopDetails():
    data=""
    reqType = request.args.get('reqtype')
    start_time = time.time()  #
    data = get_top_10_details(reqType)
    end_time = time.time()  # record the end time
    elapsed_time = end_time - start_time
    data.append({'elapsed_time':elapsed_time})
    if reqType=="tweets":
        return render_template('mypage.html',userdata="",data=data)
    elif reqType=="users":
        return render_template('mypage.html',userdata=data,data="")

@app.route('/')
def load():
    return render_template('mypage.html',userdata="",data="")

if __name__ == '__main__':
    app.run(debug=True,host ='localhost',port = 5001)