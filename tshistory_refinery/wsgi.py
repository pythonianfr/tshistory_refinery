from tshistory.http.util import nosecurity

from tshistory_refinery.webapp import AppMaker


app = AppMaker().app()
app.wsgi_app = nosecurity(app.wsgi_app)
