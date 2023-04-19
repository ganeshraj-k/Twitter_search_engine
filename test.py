from flask import Flask, request,render_template
from search_engine_processing import get_info_by_tweet, get_info_by_hashtag, get_info_by_user

app=Flask(__name__)

@app.route('/search',methods=['POST'])
def search():
	userdata=""
	search_str = request.form['searchstring']
	if search_str.startswith('@'):
		search_str = search_str[1:]
		userdata = get_info_by_user(search_str)
	elif search_str.startswith('#'):
		data = get_info_by_hashtag(search_str[1:])
	else:
		data = get_info_by_tweet(tweet_str = search_str)
	return render_template('mypage.html',data=data,userdata=userdata)

@app.route('/user',methods=['GET'])
def getuser():
    uname = request.args.get('user')
    userdata = get_info_by_user(uname)
    return render_template('mypage.html',userdata=userdata,data="")

@app.route('/userTweet', methods=["GET"])
def getTweetsByUser():
    uname = request.args.get('userid')
    tweetdata = get_info_by_user(uname)
    return render_template('mypage.html',userdata="",data=tweetdata)

@app.route('/retweets', methods=["GET"])
def getretweetsbyTweetID():
	octweetid = request.args.get('tweetid')
	tweetdata = get_info_by_tweet(oc_tweet_id=octweetid)
	return render_template('mypage.html',userdata="",data=tweetdata)

@app.route('/')
def load():
        return render_template('mypage.html',userdata="",data="")

if __name__ == '__main__':
    app.run(debug=True,host ='localhost',port = 5001)