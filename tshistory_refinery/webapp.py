from flask import Flask

from sqlalchemy import create_engine

from dbcache.http import kvstore_httpapi
from dbcache.api import kvstore
from tsview.blueprint import tsview
from tsview.history import historic
from tsview.editor import editor
from rework_ui.blueprint import reworkui

from tshistory.api import timeseries
from tshistory_xl.blueprint import blueprint as excel
from tswatch.webapp import make_blueprint as tswatch

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


def make_app(dburi=None, sources=None, httpapi=None, more_sections=None):
    if dburi:
        # that will typically for the tests
        # or someone doing something fancy
        tsa = timeseries(dburi, sources=sources)
    else:
        # this will take everything from `tshistory.cfg`
        tsa = timeseries()
        dburi = str(tsa.engine.url)

    app = Flask('refinery')
    engine = create_engine(dburi)

    def has_permission(perm):
        return True

    # tsview
    app.register_blueprint(
        tsview(
            tsa,
            has_permission=has_permission
        )
    )

    # rework-ui
    app.register_blueprint(
        reworkui(
            engine,
            has_permission=has_permission
        ),
        url_prefix='/tasks'
    )

    # history (tsview)
    historic(
        app,
        tsa,
        request_pathname_prefix='/'
    )

    # editor (tsview)
    editor(
        app,
        tsa,
        has_permission=has_permission,
        request_pathname_prefix='/'
    )

    # tswatch
    app.register_blueprint(
        tswatch(tsa),
        url_prefix='/tswatch',
    )

    # excel
    app.register_blueprint(
        excel(tsa)
    )

    # refinery api
    app.register_blueprint(
        httpapi(
            tsa,
            dburi,
            {
                'tswatch': kvstore(dburi, 'tswatch'),
                'dashboards': kvstore(dburi, 'dashboards'),
                'balances': kvstore(dburi, 'balances')
             },
            {}  # no vkvstore yet
        ).bp,
        url_prefix='/api'
    )

    # refinery web ui
    app.register_blueprint(
        blueprint.refinery_bp(
            tsa,
            more_sections=more_sections
        )
    )

    return app
