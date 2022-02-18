from tshistory_xl.http import (
    xl_httpapi,
    XLClient
)


class refinery_httpapi(xl_httpapi):
    __slots__ = 'tsa', 'bp', 'api', 'nss', 'nsg'


class RefineryClient(XLClient):

    def __repr__(self):
        return f"refinery-http-client(uri='{self.uri}')"
