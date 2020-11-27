from pathlib import Path
import io
from functools import partial

import pytest
import responses
from sqlalchemy import create_engine
import webtest

from pytest_sa_pg import db

from rework import api

from tshistory.api import timeseries
from tshistory_refinery import (
    helper,
    schema,
    tsio,
    webapi
)


DATADIR = Path(__file__).parent / 'test' / 'data'


@pytest.fixture(scope='session')
def engine(request):
    port = 5433
    db.setup_local_pg_cluster(request, DATADIR, port)
    uri = 'postgresql://localhost:{}/postgres'.format(port)
    e = create_engine(uri)
    schema.init(e, drop=True)
    schema.init(e, 'remote', rework=False, drop=True)
    api.freeze_operations(e)
    yield e


@pytest.fixture(scope='session')
def tsh(engine):
    return tsio.timeseries()


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
        sources=[
            (str(engine.url), 'remote')
        ]
    )


@pytest.fixture(scope='session')
def local(engine):
    schema.init(engine, 'remote', rework=False)

    return timeseries(
        str(engine.url),
        namespace='remote',
        handler=tsio.timeseries,
        sources=[
            (str(engine.url), 'remote')
        ]
    )

