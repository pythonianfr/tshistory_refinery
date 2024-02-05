from tshistory.http.util import nosecurity

from tshistory_refinery.webapp import (
    httpapi,
    make_app
)

app = make_app(httpapi=httpapi)
app.wsgi_app = nosecurity(app.wsgi_app)
