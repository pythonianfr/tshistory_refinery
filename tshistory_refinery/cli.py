import click
from sqlalchemy import create_engine

from rework.helper import host
from rework import api
from tshistory.config import configuration
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
    dburi = configuration().find_dburi(db_uri)
    engine = create_engine(dburi)

    with cache.suspended_policies(engine):
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
    dburi = configuration().find_dburi(db_uri)
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


@click.command('init-db')
@click.argument('db-uri')
@click.option('--no-dry-run', is_flag=True, default=False)
def initdb(db_uri, no_dry_run=False):
    dburi = configuration().find_dburi(db_uri)
    if not no_dry_run:
        print('this would reset and init the db at {}'.format(dburi))
        return
    # register all component schemas
    engine = create_engine(dburi)
    refinery_schema().create(engine)
