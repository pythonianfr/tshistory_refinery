from flask import Flask

from sqlalchemy import create_engine

from tsview.blueprint import tsview
from tsview.history import historic
from tsview.editor import editor
from rework_ui.blueprint import reworkui

from tshistory_xl.blueprint import blueprint as excel

from tshistory_refinery import http, blueprint


def make_app(config, tsa, editor_callback=None, more_sections=None):
    app = Flask('refinery')
    dburi = config['db']['uri']
    engine = create_engine(dburi)

    try:
        # in the near future we want to completely
        # get rid of this
        segment = config['nginx']['segment']
    except:
        segment = '/'

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
        request_pathname_prefix=segment
    )
    editor(
        app,
        tsa,
        has_permission=has_permission,
        request_pathname_prefix=segment,
        additionnal_info=editor_callback
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
