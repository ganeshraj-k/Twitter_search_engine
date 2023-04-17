from flask import Flask, request,render_template
from search_engine_processing import get_info_by_tweet, get_info_by_hashtag, get_info_by_user

app=Flask(__name__)

@app.route('/search',methods=['POST'])
def search():
	search_str = request.form['searchstring']
	if search_str.startswith('@'):
		search_str = search_str[1:]
		data = get_info_by_user(search_str)
	elif search_str.startswith('#'):
		data = get_info_by_hashtag(search_str[1:])
	else:
		data = get_info_by_tweet(tweet_str = search_str)
	print(data)
	return render_template('mypage.html',data=data,userdata="")

@app.route('/user',methods=['GET'])
def getuser():
    uname = request.args.get('user')
    userdata = get_info_by_user(uname)
    print(userdata)
    return render_template('mypage.html',userdata=userdata,data="")

@app.route('/')
def load():
        return render_template('mypage.html',userdata="",data="")

if __name__ == '__main__':
    app.run(debug=True,host ='localhost',port = 5001)