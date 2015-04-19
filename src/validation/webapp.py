from flask import Flask, url_for, request
from flask import render_template
import json

# Whether app is deployed locally or
# on a shared web host.
deployment = "local"
#deployment = "web"


# Global variable that determines which
# data set the samples are selected from.
experiment = "experiment1"

# Where the usernames and passwords are stored
# as a dictionary in json format.
url_credentials = "./static/credentials.json"
url_user_data = "./static/user_data.json"
url_results = "./results/"
url_data = "./data/"

if deployment == "local":
    app = Flask(__name__)
elif deployment == "web":
    app = Flask(__name__ , static_url_path = "/home1/naviddia/public_html/FEC/validation/static")
else:
    raise("Error: must specify deployment mode.")




def get_static_url(filename):
    return  url_for('static', filename=filename)



@app.route('/submit/', methods=['POST'])
def submit():
    """
    Receive a data submission request and save 
    the data to file.
    """
    if request.method != "POST":
        return "Error: data must be submitted using the POST method."

    try:
        result = json.loads(request.form['post'])
    except KeyError:
        print "Error: No 'post' data received."     

    # Both submit and goback requests must include username and pageno
    try:
        pageno = result['pageno']
        username = result['username']
    except:
        return "Error: submitted form is malformed."
        
    if 'goback' in result:
        update_user_pageno(username,pageno = max(1, pageno - 1))
        return "SUCCESS"
    else:
        try:
            data = result['data']
            comment = result['comment']
        except KeyError:
            return "Error: submitted form is malformed."

        export_data = {"data":data,"comment":comment}
        filename = url_results + "%s-%s.json" % (username, pageno)
        f = open(filename ,'w')
        f.write(json.dumps(export_data, sort_keys = True, indent = 4))
        f.close()
        update_user_pageno(username,pageno = pageno + 1)

    return 'SUCCESS'



@app.route('/page/<username>/')
def page( username = None, pageno=None):
    """
    Load a page of html data, pass it to the page_of_records template
    This page expects a variable 'username' and will pass it to
    the template. This value is needed when the ajax call submits the
    form back to Flask.
    """
    
    
    if pageno is None:
        pageno = get_user_pageno(username)
        
    datafile = url_data + '%s/%s.html' % (experiment, pageno)

    try:
        f = open(datafile)
        html = f.read()
    except :
        print "bad data file"
        return render_template('goodbye.html')
    if html == "": 
        return render_template('goodbye.html')
        
    return render_template('page_of_records.html', data = html, style_url = get_static_url('style.css'), pageno = pageno, username = username)




@app.route('/')
def login():
    """
    Load workers' status, Show login screen. 
    If login is successful, the login page will
    redirect to the next available page.
    """
    save_to_file('hello','log.txt')
    return render_template('login.html', style_url = get_static_url('style-login.css'))


@app.route('/login/', methods=['POST','GET'])
def login_verify():
    """
    Receive username and password from the login view
    and return verificaion result.
    """
    
    if request.method != "POST": 
        return "Error: data must be submitted using the POST method."
    
    data = json.loads(request.form['post'])
    username = data['username']
    password = data['password']
    
    if verify_user_pass(username,password):
        return "SUCCESS"
    else:
        return "WRONG USERNAME OR PASSWORD"
        
        
        

def verify_user_pass(username = None,password = None):
    """
    Return True if username,password combination 
    found in credentials file.
    """

    if username is None or password is None: return False
    f = open(url_credentials)
    s = f.read()
    f.close()

    credentials = json.loads(s)
    try:
        if credentials[username]==password:
            return True
        else:
            return False
    except:
        return False
    

def get_user_pageno(username):
    """
    Return the current page for the 
    specified username. If not found, 
    return 1.
    """
    f = open(url_user_data)
    s = f.read()
    data = json.loads(s)
    f.close()
    try:
        pageno = data[username]['pageno']
    except:
        pageno = 1
    return pageno

def update_user_pageno(username, pageno):
    """
    Update the current page for the 
    specified username. 
    """
    f = open(url_user_data)
    s = f.read()
    data = json.loads(s)
    f.close()


    try:
        data[username]['pageno'] = pageno
    except:
        pass

    f = open(url_user_data,'w')
    s = json.dumps(data, sort_keys = True, indent = 4)
    f.write(s)
    f.close()


def save_to_file(s,filename):
    f = open(filename,'w')
    f.write(s)
    f.close()



if __name__ == '__main__':
    app.debug = True
    app.run(host="0.0.0.0")
