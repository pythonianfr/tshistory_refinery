from tshistory_supervision.http import (
    supervision_httpapi,
    SupervisionClient
)

from tshistory_formula.http import (
    formula_httpapi,
    FormulaClient
)

from tshistory_xl.http import (
    xl_httpapi,
    XLClient
)



class refinery_httpapi(supervision_httpapi, formula_httpapi, xl_httpapi):
    __slots__ = 'tsa', 'bp', 'api', 'nss', 'nsg'


class RefineryClient(SupervisionClient, FormulaClient, XLClient):

    def __repr__(self):
        return f"refinery-http-client(uri='{self.uri}')"
