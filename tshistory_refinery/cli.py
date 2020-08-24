import click
from sqlalchemy import create_engine

from tshistory_refinery.helper import host, config, apimaker
from tshistory_refinery.schema import init


@click.command()
def webstart():
    from tshistory_refinery.wsgi import app
    app.run(host=host(), debug=True)


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
