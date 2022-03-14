import click
from sqlalchemy import create_engine
from sqlhelp import sqlfile

from rework.helper import host
from tshistory_refinery.helper import config, apimaker
from tshistory_refinery.schema import init


@click.command()
def webstart():
    from tshistory_refinery.wsgi import app
    app.run(host=host(), debug=True)


@click.command('migrate-to-cache')
@click.option('--namespace', default='tsh')
def migrate_to_cache(namespace='tsh'):
    from pathlib import Path
    cfg = config()
    dburi = cfg['db']['uri']
    engine = create_engine(dburi)

    exists = engine.execute(
        'select 1 from information_schema.schemata where schema_name = %(name)s',
        name=f'{namespace}.cache_policy'
    ).scalar()

    if exists:
        print('nothing to do.')
        return
    
    cache_policy = Path(__file__).parent.parent / 'tshistory_refinery/schema.sql'
    with engine.begin() as cn:
        cn.execute(sqlfile(cache_policy, ns=namespace))


@click.command('init-db')
@click.option('--no-dry-run', is_flag=True, default=False)
def initdb(no_dry_run=False):
    cfg = config()
    dburi = cfg['db']['uri']
    if not no_dry_run:
        print('this would reset and init the db at {}'.format(dburi))
        return
    # register all component schemas
    engine = create_engine(dburi)
    init(engine)
