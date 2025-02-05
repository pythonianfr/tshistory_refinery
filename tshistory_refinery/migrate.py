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




@version('tshistory-refinery', '0.10.0')
def migrate_rework_schema_interval(engine, namespace, interactive):
    # rework schema
    engine.execute(
        'alter table rework.operation add column if not exists vacuum interval'
    )

    # remove useless tables
    with engine.begin() as cn:
        cn.execute(
            f'drop table if exists "{namespace}.group".basket'
        )

        cn.execute(
            f'drop table if exists "{namespace}-formula-patch".basket'
        )
        cn.execute(
            f'drop table if exists "{namespace}-formula-patch".groupmap'
        )
        cn.execute(
            f'drop table if exists "{namespace}-formula-patch".group_registry'
        )

        cn.execute(
            f'drop table if exists "{namespace}-formula-patch.group".basket'
        )
        cn.execute(
            f'drop table if exists "{namespace}-formula-patch.group".registry'
        )
        cn.execute(
            f'drop table if exists "{namespace}-formula-patch.group".revision_metadata'
        )

        cn.execute(
            f'drop table if exists "{namespace}-formula-patch.group-kvstore".kvstore'
        )
        cn.execute(
            f'drop table if exists "{namespace}-formula-patch.group-kvstore".things'
        )
        cn.execute(
            f'drop table if exists "{namespace}-formula-patch.group-kvstore".version'
        )
        cn.execute(
            f'drop table if exists "{namespace}-formula-patch.group-kvstore".vkvstore'
        )

    # intervals
    from tshistory.migrate import migrate_intervals
    migrate_intervals(engine, f'{namespace}-cache', interactive)


@version('tshistory-refinery', '0.9.1')
def migrate_drop_ready(engine, namespace, interactive):
    sql = (
        f'alter table "{namespace}".cache_policy_series '
        f'drop column if exists ready'
    )
    with engine.begin() as cn:
        cn.execute(sql)


@version('tshistory-refinery', '0.9.0')
def migrate_revision_table(engine, namespace, interactive):
    from tshistory.migrate import migrate_add_diffstart_diffend

    migrate_add_diffstart_diffend(engine, f'{namespace}-cache', interactive)
