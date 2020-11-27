from pm_utils.flaskutil import ReverseProxied

from tshistory_refinery import helper
from tshistory_refinery.webapi import make_app


config = helper.config()
tsa = helper.apimaker(config)
app = make_app(config, tsa)
app.wsgi_app = ReverseProxied(app.wsgi_app)
