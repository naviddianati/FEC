#! /home1/naviddia/public_html/Software/python/bin/python 

#from flup.server.fcgi import WSGIServer
#from webapp import app as application


#WSGIServer(application).run()


from wsgiref.handlers import CGIHandler
from webapp import app

app.debug = True

CGIHandler().run(app)
