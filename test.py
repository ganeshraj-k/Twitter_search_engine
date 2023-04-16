from flask import Flask, request,render_template
from SearchApp import PrintNames,GetUserDetails

app=Flask(__name__)

@app.route('/search',methods=['POST'])
def search():
    search_str = request.form['searchstring']
    data = PrintNames(search_str)
    return render_template('mypage.html',data=data,userdata="")

@app.route('/user',methods=['GET'])
def getuser():
    uname = request.args.get('user')
    userdata = GetUserDetails(uname)
    print(userdata)
    return render_template('mypage.html',userdata=userdata,data="")

@app.route('/')
def load():
        return render_template('mypage.html',userdata="",data="")

if __name__ == '__main__':
    app.run(debug=True,host ='localhost',port = 5001)