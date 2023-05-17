from dbcache import api as dbapi

from tshistory.util import read_versions

from tshistory.migrate import (
    fix_user_metadata,
    migrate_metadata
)


def run_migrations(engine, namespace, interactive=False):
    print('Running migrations for tshistory_refinery.')
    # determine versions
    storens = f'{namespace}-kvstore'
    stored_version, known_version = read_versions(
        str(engine.url),
        namespace,
        'tshistory-refinery-version'
    )

    if stored_version is None:
        # first time
        from tshistory_refinery import __version__ as known_version
        store = dbapi.kvstore(str(engine.url), namespace=storens)
        # we probably want to go further
        initial_migration(engine, namespace, interactive)
        store.set('tshistory-refinery-version', known_version)


def initial_migration(engine, namespace, interactive):
    print('initial migration')
    migrate_metadata(engine, f'{namespace}-cache', interactive)
    fix_user_metadata(engine, f'{namespace}-cache', interactive)
