from pathlib import Path

import click
from sqlalchemy import create_engine
from sqlhelp import sqlfile

from rework.helper import host
from rework import api
from tshistory.util import find_dburi
from tshistory_refinery.helper import config, apimaker
from tshistory_refinery.schema import init


@click.command()
def webstart():
    from tshistory_refinery.wsgi import app
    app.run(host=host(), debug=True)


@click.command('setup-tasks')
@click.argument('db-uri')
def setup_tasks(db_uri):
    from tshistory_refinery import tasks  # make them available at freeze time
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


@click.command('migrate-to-cache')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def migrate_to_cache(db_uri, namespace='tsh'):
    dburi = find_dburi(db_uri)
    engine = create_engine(dburi)

    exists = engine.execute(
        "select 1 from pg_tables where schemaname = 'tsh' and tablename = %(name)s",
        name=f'cache_policy'
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
    init(engine)
