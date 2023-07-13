from pathlib import Path

import pytest
from sqlalchemy import create_engine
import webtest

from pytest_sa_pg import db

from rework import api as rapi

from tshistory.api import timeseries
from tshistory_refinery import (
    helper,
    schema,
    tsio,
    webapi,
    tasks  # be registrable  # noqa: F401
)


DATADIR = Path(__file__).parent / 'test' / 'data'


def _initschema(engine, ns='tsh'):
    schema.init(engine, namespace=ns, drop=True)
    rapi.freeze_operations(engine)


@pytest.fixture(scope='session')
def engine(request):
    port = 5433
    db.setup_local_pg_cluster(request, DATADIR, port)
    uri = 'postgresql://localhost:{}/postgres'.format(port)
    e = create_engine(uri)
    _initschema(e)
    _initschema(e, 'remote')
    yield e


@pytest.fixture(scope='session')
def tsh(engine):
    return tsio.timeseries()


@pytest.fixture(scope='session', params=['tsh', 'fancy-ns'])
def tsa(request, engine):
    _initschema(engine, request.param)

    return timeseries(
        str(engine.url),
        namespace=request.param,
        handler=tsio.timeseries,
        sources={}
    )


@pytest.fixture(scope='session')
def federated(request, engine):
    _initschema(engine, 'tsh')

    return timeseries(
        str(engine.url),
        namespace='tsh',
        handler=tsio.timeseries,
        sources={'remote': (str(engine.url), 'remote')}
    )


class NonSuckingWebTester(webtest.TestApp):

    def _check_status(self, status, res):
        try:
            super()._check_status(self, status, res)
        except:
            pass
            # raise <- default behaviour on 4xx is silly


BASECONFIG = {
    'tasks': {'hostid': 'http://this'},
    'nginx': {'segment': ''},
    'security': {
        'read': 'anything-goes',
        'write': 'anython-goes'
    },
}

APP = None


def webapp(engine):
    global APP
    if APP is not None:
        return APP
    BASECONFIG['db'] = {'uri': str(engine.url)}
    BASECONFIG['sources'] = {
        'remote': f'{engine.url},remote'
    }
    APP = webapi.make_app(BASECONFIG, helper.apimaker(BASECONFIG))
    return APP


@pytest.fixture(scope='session')
def client(engine):
    return NonSuckingWebTester(webapp(engine))


@pytest.fixture(scope='session')
def remote(engine):
    return timeseries(
        str(engine.url),
        namespace='remote',
        handler=tsio.timeseries,
        sources={'remote': (str(engine.url), 'remote')}
    )


@pytest.fixture(scope='session')
def local(engine):
    _initschema(engine, 'remote')

    return timeseries(
        str(engine.url),
        namespace='remote',
        handler=tsio.timeseries,
        sources={'remote': (str(engine.url), 'remote')}
    )

