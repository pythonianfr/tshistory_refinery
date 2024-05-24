from tshistory.migrate import (
    fix_user_metadata,
    migrate_metadata,
    Migrator as _Migrator,
    version
)

from tshistory_refinery import __version__


class Migrator(_Migrator):
    _order = 2
    _package_version = __version__
    _package = 'tshistory-refinery'

    def initial_migration(self):
        print('initial migration')
        migrate_metadata(self.engine, f'{self.namespace}-cache', self.interactive)
        fix_user_metadata(self.engine, f'{self.namespace}-cache', self.interactive)


@version('tshistory-refinery', '0.9.0')
def migrate_revision_table(engine, namespace, interactive):
    from tshistory.migrate import migrate_add_diffstart_diffend

    migrate_add_diffstart_diffend(engine, f'{namespace}-cache', interactive)
