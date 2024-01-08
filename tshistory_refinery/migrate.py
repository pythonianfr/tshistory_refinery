from tshistory.migrate import (
    fix_user_metadata,
    migrate_metadata,
    Migrator as _Migrator
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

        from tswatch.utils import tswatch_init
        tswatch_init(self.engine, drop=False)
