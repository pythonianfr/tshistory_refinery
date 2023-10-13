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


def make_app(dburi, sources=None, more_sections=None):
    tsa = timeseries(dburi, sources=sources)
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

    # dbcache
    app.register_blueprint(
        kvstore_httpapi(
            dburi,
            {
                'tswatch': kvstore(dburi),
                'dashboards': kvstore(dburi),
                'balances': kvstore(dburi)
             },
            {}  # no vkvstore yet
        ).bp,
        url_prefix='/stores'
    )

    # refinery api
    app.register_blueprint(
        http.refinery_httpapi(tsa).bp,
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
