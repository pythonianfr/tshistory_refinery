from pathlib import Path

import click
from sqlalchemy import create_engine
from sqlhelp import sqlfile

from rework.helper import host
from rework import api
from tshistory.util import find_dburi
from tshistory_refinery.helper import config, apimaker
from tshistory_refinery.schema import refinery_schema
from tshistory_refinery import cache


@click.command()
@click.option('--port', default=5000)
def webstart(port=500):
    from tshistory_refinery.wsgi import app
    app.run(host=host(), port=port, debug=True)


@click.command('setup-tasks')
@click.argument('db-uri')
def setup_tasks(db_uri):
    from tshistory_refinery import tasks  # make them available at freeze time  # noqa: F401
    dburi = find_dburi(db_uri)
    engine = create_engine(dburi)
    with engine.begin() as cn:
        cn.execute(
            "delete from rework.operation "
            "where path like '%%tshistory_refinery%%'"
        )

    api.freeze_operations(engine)


@click.command('refresh-cache')
@click.argument('db-uri')
@click.argument('policy-name')
@click.option('--initial', default=False, is_flag=True)
def refresh_cache(db_uri, policy_name, initial=False):
    dburi = find_dburi(db_uri)
    engine = create_engine(dburi)
    t = api.schedule(
        engine,
        'refresh_formula_cache',
        domain='timeseries',
        inputdata={
            'policy': policy_name,
            'initial': initial
        },
    )
    print(f'queued {t.tid}')


@click.command('list-series-locks')
@click.argument('db-uri')
@click.option('--policy-name', default=None)
@click.option('--kill', default=False, is_flag=True,
              help='remove the locks')
def list_series_locks(db_uri, policy_name=None, kill=False):
    dburi = find_dburi(db_uri)
    engine = create_engine(dburi)

    tsa = apimaker(config())
    print('Series having a lock, per policy')

    if policy_name:
        policies = [policy_name]
    else:
        policies = tsa.cache_policies()

    for polname in policies:
        print(f'Policy `{polname}`')
        for name in tsa.cache_policy_series(polname):
            if cache.series_ready(engine, name):
                continue

            print(f'* {name}')
            if kill:
                cache._set_series_ready(
                    engine,
                    name,
                    True
                )


@click.command('migrate-to-cache')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def migrate_to_cache(db_uri, namespace='tsh'):
    dburi = find_dburi(db_uri)
    engine = create_engine(dburi)

    exists = engine.execute(
        "select 1 from pg_tables where schemaname = 'tsh' and tablename = %(name)s",
        name='cache_policy'
    ).scalar()

    if not exists:
        cache_policy = Path(__file__).parent.parent / 'tshistory_refinery/schema.sql'
        with engine.begin() as cn:
            cn.execute(sqlfile(cache_policy, ns=namespace))

    from tshistory.schema import tsschema
    schem = tsschema(f'{namespace}-cache')
    schem.create(engine)


@click.command('init-db')
@click.argument('db-uri')
@click.option('--no-dry-run', is_flag=True, default=False)
def initdb(db_uri, no_dry_run=False):
    dburi = find_dburi(db_uri)
    if not no_dry_run:
        print('this would reset and init the db at {}'.format(dburi))
        return
    # register all component schemas
    engine = create_engine(dburi)
    refinery_schema().create(engine, rework=True)
