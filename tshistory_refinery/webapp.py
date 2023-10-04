from flask import Flask

from sqlalchemy import create_engine

from tsview.blueprint import tsview
from tsview.history import historic
from tsview.editor import editor
from rework_ui.blueprint import reworkui

from tshistory.api import timeseries
from tshistory_xl.blueprint import blueprint as excel

from tshistory_refinery import http, blueprint


def make_app(dburi, sources=None, more_sections=None):
    tsa = timeseries(dburi, sources=sources)
    app = Flask('refinery')
    engine = create_engine(dburi)

    def has_permission(perm):
        return True

    app.register_blueprint(
        tsview(
            tsa,
            has_permission=has_permission
        )
    )
    app.register_blueprint(
        reworkui(
            engine,
            has_permission=has_permission
        ),
        url_prefix='/tasks'
    )
    historic(
        app,
        tsa,
        request_pathname_prefix='/'
    )
    editor(
        app,
        tsa,
        has_permission=has_permission,
        request_pathname_prefix='/'
    )

    app.register_blueprint(
        http.refinery_httpapi(tsa).bp,
        url_prefix='/api'
    )

    app.register_blueprint(
        excel(tsa)
    )

    app.register_blueprint(
        blueprint.refinery_bp(
            tsa,
            more_sections=more_sections
        )
    )

    return app
