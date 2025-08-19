from tshistory.migrate import (
    do_cleanup_kvstore,
    do_enforce_series_metadata_integrity,
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
def migrate_0100(engine, namespace, interactive):
    migrate_rework_events_to_task_events(engine, namespace, interactive)
    do_cleanup_kvstore(engine, f'{namespace}-cache', interactive)
    cleanup_cache_group_artifacts(engine, namespace, interactive)
    migrate_metadata_integrity(engine, namespace, interactive)
    migrate_fix_refinery_indexes(engine, namespace, interactive)


def migrate_rework_events_to_task_events(engine, namespace, interactive):
    with engine.begin() as cn:
        # check if already migrated
        task_events_exists = cn.execute(
            """
            select exists (
                select 1 from information_schema.tables
                where table_schema = 'rework'
                and table_name = 'task_events'
            )
            """
        ).scalar()

        if task_events_exists:
            return  # already migrated

        print('migrating rework.events to rework.task_events')

        # clean up old schema completely
        cn.execute('drop trigger if exists trace_events on rework.task')
        cn.execute('drop function if exists rework.trace_events() cascade')
        cn.execute('drop table if exists rework.events cascade')

        # recreate from rework_ui schema
        from rework_ui.schema import SCHEMAFILE
        from sqlhelp import sqlfile

        sql = sqlfile(SCHEMAFILE, ns='rework')
        cn.execute(sql, _binary=False)


def migrate_metadata_integrity(engine, namespace, interactive):
    # Apply metadata integrity migration to cache namespace (series only)
    do_enforce_series_metadata_integrity(engine, f'{namespace}-cache', interactive)


def cleanup_cache_group_artifacts(engine, namespace, interactive):
    """Clean up group-related schemas and tables from cache namespace"""
    with engine.begin() as cn:
        for schema in (f'{namespace}-cache.group', f'{namespace}-cache.group-kvstore'):
            cn.execute(f'drop schema if exists "{schema}" cascade')

        for table in ('group_registry', 'groupmap', 'basket'):
            cn.execute(f'drop table if exists "{namespace}-cache".{table} cascade')


def migrate_fix_refinery_indexes(engine, namespace, interactive):
    """Fix refinery-specific cache table indexes to use explicit names"""
    from tshistory.migrate import do_fix_indexes
    from tshistory_refinery.helper import EXPECTED_INDEXES

    # Format index names with actual namespace
    refinery_indexes = {
        k: (v[0].format(ns=namespace), v[1])
        for k, v in EXPECTED_INDEXES.items()
    }

    do_fix_indexes(engine, namespace, interactive, refinery_indexes)


@version('tshistory-refinery', '0.9.2')
def migrate_rework_schema_interval(engine, namespace, interactive):
    # rework schema
    with engine.begin() as cn:
        cn.execute(
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
            f'drop table if exists "{namespace}-formula-patch.group".revision_metadata'
        )
        cn.execute(
            f'drop table if exists "{namespace}-formula-patch.group".registry'
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
