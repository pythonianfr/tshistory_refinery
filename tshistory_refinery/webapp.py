from flask import Flask

from sqlalchemy import create_engine

from dbcache.http import kvstore_httpapi
from dbcache.api import kvstore
from tsview.blueprint import tsview
from rework_ui.blueprint import reworkui

from tshistory.api import timeseries
from tshistory_xl.blueprint import blueprint as excel

from tshistory_refinery import http, blueprint


# mix refinery http stuff with dbcache stores api

class httpapi(http.refinery_httpapi,
              kvstore_httpapi):

    def __init__(self, tsa, uri, kvstore_apimap, vkvstore_apimap):
        http.refinery_httpapi.__init__(
            self,
            tsa
        )
        kvstore_httpapi.__init__(
            self,
            uri,
            kvstore_apimap,
            vkvstore_apimap
        )


class AppMaker:

    def __init__(self, dburi=None, sources=None, more_sections=None):
        if dburi:
            # that will typically for the tests
            # or someone doing something fancy
            self.tsa = timeseries(dburi, sources=sources)
        else:
            # this will take everything from `tshistory.cfg`
            self.tsa = timeseries()
            dburi = str(self.tsa.engine.url)

        self.dburi = dburi
        self.sources = sources
        self.more_sections = more_sections
        self.engine = create_engine(dburi)

    def app(self):
        app = Flask('refinery')
        self.tsview(app)
        self.reworkui(app)
        self.excel(app)
        self.api(app)
        self.refinery(app)
        return app

    def tsview(self, app):
        app.register_blueprint(
            tsview(self.tsa)
        )

    def reworkui(self, app):
        app.register_blueprint(
            reworkui(self.engine),
            url_prefix='/tasks'
        )

    def excel(self, app):
        app.register_blueprint(
            excel(self.tsa)
        )

    def api(self, app):
        # refinery api
        app.register_blueprint(
            httpapi(
                self.tsa,
                self.dburi,
                {
                    'dashboards': kvstore(self.dburi, 'dashboards'),
                    'balances': kvstore(self.dburi, 'balances')
                 },
                {}  # no vkvstore yet
            ).bp,
            url_prefix='/api'
        )

    def refinery(self, app):
        app.register_blueprint(
            blueprint.refinery_bp(
                self.tsa,
                more_sections=self.more_sections
            )
        )
