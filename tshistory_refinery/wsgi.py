from pm_utils.flaskutil import ReverseProxied

from tshistory_refinery.helper import config
from tshistory_refinery.webapi import make_app


app = make_app(config())
app.wsgi_app = ReverseProxied(app.wsgi_app)
