from datetime import datetime
from contextlib import contextmanager
from functools import cmp_to_key
import traceback

from croniter import (
    croniter,
    croniter_range
)
import pandas as pd
from psyl import lisp
from rework import (
    api as rapi,
    io as rio,
    task as rtask
)
from sqlhelp import (
    insert,
    update
)

from tshistory_formula import registry
from tshistory_refinery import helper
from tshistory_refinery import tsio


def eval_moment(expr, env={}):
    env = env.copy()
    env.update(rio._MOMENT_ENV)
    return lisp.evaluate(expr, env=lisp.Env(env))


def validate_policy(
        initial_revdate,
        look_before,
        look_after,
        revdate_rule,
        schedule_rule
):
    """ Validate each of the four parameters of a given cache policy """
    badinputs = []
    env = {'now': datetime.utcnow()}
    for name, val in (
            ('initial_revdate', initial_revdate),
            ('look_before', look_before),
            ('look_after', look_after)):
        try:
            eval_moment(val, env)
        except:
            badinputs.append((name, val))

    if not croniter.is_valid(revdate_rule):
        badinputs.append(('revdate_rule', revdate_rule))
    if not croniter.is_valid(schedule_rule):
        badinputs.append(('schedule_rule', schedule_rule))
    return dict(badinputs)


def new_policy(
        engine,
        name,
        initial_revdate,
        look_before,
        look_after,
        revdate_rule,
        schedule_rule,
        namespace='tsh'
):
    """ Create a new cache policy """
    badinputs = validate_policy(
        initial_revdate,
        look_before,
        look_after,
        revdate_rule,
        schedule_rule
    )
    if badinputs:
        raise ValueError(
            f'Bad inputs for the cache policy: {badinputs}'
        )

    with engine.begin() as cn:
        q = insert(
            f'"{namespace}".cache_policy'
        ).values(
            name=name,
            initial_revdate=initial_revdate,
            look_before=look_before,
            look_after=look_after,
            revdate_rule=revdate_rule,
            schedule_rule=schedule_rule
        )
        q.do(cn).scalar()


def edit_policy(
        engine,
        name,
        initial_revdate,
        look_before,
        look_after,
        revdate_rule,
        schedule_rule,
        namespace='tsh'
):
    """ Edit a cache policy """
    badinputs = validate_policy(
        initial_revdate,
        look_before,
        look_after,
        revdate_rule,
        schedule_rule
    )
    if badinputs:
        raise ValueError(
            f'Bad inputs for the cache policy: {badinputs}'
        )

    with engine.begin() as cn:
        q = update(
            f'"{namespace}".cache_policy'
        ).where(name=name
        ).values(
            initial_revdate=initial_revdate,
            look_before=look_before,
            look_after=look_after,
            revdate_rule=revdate_rule,
            schedule_rule=schedule_rule
        )
        q.do(cn)


def schedule_policy(engine, name, namespace='tsh'):
    with engine.begin() as cn:
        cid, schedule_rule = cn.execute(
            f'select id, schedule_rule from "{namespace}".cache_policy '
            f'where name = %(name)s',
            name=name
        ).fetchone()
        sid = rapi.prepare(
            engine,
            'refresh_formula_cache',
            domain='timeseries',
            rule='0 ' + schedule_rule,
            inputdata={
                'policy': name,
                'initial': 0
            },
        )
        cn.execute(
            f'insert into "{namespace}".cache_policy_sched '
            f'(cache_policy_id, prepared_task_id) '
            f'values (%(policy_id)s, %(sched_id)s)',
            policy_id=cid,
            sched_id=sid
        )
    # immediately schedule the initial import
    return rapi.schedule(
        engine,
        'refresh_formula_cache',
        domain='timeseries',
        inputdata={
            'policy': name,
            'initial': 1
        },
    )


def scheduled_policy(engine, name, namespace='tsh'):
    with engine.begin() as cn:
        return bool(
            cn.execute(
                f'select cps.prepared_task_id '
                f'from "{namespace}".cache_policy_sched as cps, '
                f'     "{namespace}".cache_policy as cp '
                f'where cp.name = %(name)s and '
                f'      cps.cache_policy_id = cp.id',
                name=name
            ).scalar()
        )


def _remove_scheduled_tasks(cn, name, namespace):
    cn.execute(
        f'delete from rework.sched as s '
        f'using "{namespace}".cache_policy_sched as cs, '
        f'      "{namespace}".cache_policy as c '
        f'where c.name = %(name)s and '
        f'      cs.cache_policy_id = c.id and '
        f'      cs.prepared_task_id = s.id',
        name=name
    )


def unschedule_policy(engine, name, namespace='tsh'):
    with engine.begin() as cn:
        _remove_scheduled_tasks(cn, name, namespace)
        cn.execute(
            f'delete from "{namespace}".cache_policy_sched as cps '
            f'using "{namespace}".cache_policy as cp '
            f'where cps.cache_policy_id = cp.id and '
            f'      cp.name = %(name)s',
            name=name
        )


def active_task(engine, policy_name, namespace='tsh'):
    r = engine.execute(
        'select t.id '
        'from rework.task as t, '
        '     rework.operation as o '
        'where t.operation = o.id and '
        '      o.name = \'refresh_formula_cache\''
    )
    for tid, in r.fetchall():
        task = rtask.Task.byid(engine, tid)
        if task.input['policy'] == policy_name:
            if task.status != 'done':
                return task


def delete_policy(engine, policy_name, namespace='tsh'):
    with engine.begin() as cn:
        _remove_scheduled_tasks(cn, policy_name, namespace)

        tsh = tsio.timeseries(namespace=namespace)
        for name in policy_series(cn, policy_name, namespace=namespace):
            tsh.cache.delete(cn, name)

        cn.execute(
            f'delete from "{namespace}".cache_policy '
            f'where name = %(name)s',
            name=policy_name
        )


def policy_by_name(engine, name, namespace='tsh'):
    """ Return a cache policy by name, as a dict """
    with engine.begin() as cn:
        p = cn.execute(
            f'select initial_revdate, '
            f'       revdate_rule, schedule_rule '
            f'from "{namespace}".cache_policy '
            f'where name = %(name)s',
            name=name
        ).fetchone()
    return dict(p)


def policy_series(cn, policy_name, namespace='tsh'):
    """ Return the series associated with a cache policy """
    q = (
        f'select series.name '
        f'from "{namespace}".cache_policy as cache, '
        f'     "{namespace}".cache_policy_series as middle, '
        f'     "{namespace}".registry as series '
        f'where cache.id = middle.cache_policy_id and '
        f'      series_id = series.id and '
        f'      cache.name = %(cachename)s'
    )
    p = cn.execute(
        q,
        cachename=policy_name
    ).fetchall()
    return [item for item, in p]


def set_policy(cn, policy_name, series_name, namespace='tsh'):
    """ Associate a cache policy to a series """
    q = (
        f'insert into "{namespace}".cache_policy_series '
        f'(cache_policy_id, series_id) '
        f'values ( '
        f' (select id '
        f'  from "{namespace}".cache_policy '
        f'  where name = %(cachename)s), '
        f' (select id '
        f'  from "{namespace}".registry '
        f'  where name = %(seriesname)s) '
        f')'
    )
    cn.execute(
        q,
        cachename=policy_name,
        seriesname=series_name
    )


def unset_policy(cn, series_name, namespace='tsh'):
    q = (
        f'delete from "{namespace}".cache_policy_series '
        f'where series_id in ('
        f' select id from "{namespace}".registry where '
        f' name = %(name)s'
        f')'
    )
    cn.execute(
        q,
        name=series_name
    )


def policy_ready(cn, policyname, namespace='tsh'):
    """ Return the cache readiness """
    q = (
        f'select ready '
        f'from "{namespace}".cache_policy '
        f'where name = %(policyname)s'
    )
    return cn.execute(
        q,
        policyname=policyname
    ).scalar()


def series_policy_ready(cn, series_name, namespace='tsh'):
    """ Return the cache readiness for a series """
    q = (
        f'select cache.ready '
        f'from "{namespace}".cache_policy as cache, '
        f'     "{namespace}".cache_policy_series as middle, '
        f'     "{namespace}".registry as series '
        f'where cache.id = middle.cache_policy_id and '
        f'      series_id = series.id and '
        f'      series.name = %(seriesname)s'
    )
    return cn.execute(
        q,
        seriesname=series_name
    ).scalar()


def series_ready(cn, series_name, namespace='tsh'):
    """ Return the cache readiness for a series """
    q = (
        f'select middle.ready '
        f'from "{namespace}".cache_policy_series as middle, '
        f'     "{namespace}".registry as series '
        f'where middle.series_id = series.id and '
        f'      series.name = %(seriesname)s'
    )
    return cn.execute(
        q,
        seriesname=series_name
    ).scalar()


def set_policy_ready(engine, policy_name, val, namespace='tsh'):
    """ Mark a cache policy as ready """
    assert isinstance(val, bool)
    print('set ready', policy_name, val, namespace)
    q = (
        f'update "{namespace}".cache_policy '
        f'set ready = %(val)s '
        f'where name = %(name)s'
    )
    with engine.begin() as cn:
        cn.execute(
            q,
            name=policy_name,
            val=val
        )


def _set_series_ready(engine, series_name, val, namespace='tsh'):
    """ Mark the cache readiness for a series """
    assert isinstance(val, bool)
    print('set ready', series_name, val, namespace)
    q = (
        f'update "{namespace}".cache_policy_series as middle '
        f'set ready = %(val)s '
        f'from "{namespace}".registry as series '
        f'where middle.series_id = series.id and '
        f'      series.name = %(seriesname)s'
    )
    with engine.begin() as cn:
        cn.execute(
            q,
            seriesname=series_name,
            val=val
        )


def series_policy(cn, series_name, namespace='tsh'):
    """ Return the cache policy for a series """
    q = (
        f'select cache.name, initial_revdate, '
        f'       look_before, look_after, '
        f'       revdate_rule, schedule_rule '
        f'from "{namespace}".cache_policy as cache, '
        f'     "{namespace}".cache_policy_series as middle, '
        f'     "{namespace}".registry as series '
        f'where cache.id = middle.cache_policy_id and '
        f'      series_id = series.id and '
        f'      series.name = %(seriesname)s'
    )
    p = cn.execute(
        q,
        seriesname=series_name
    ).fetchone()
    if p is None:
        return
    return dict(p)


@contextmanager
def series_refresh_lock(engine, name, namespace):
    _set_series_ready(engine, name, False, namespace=namespace)
    try:
        yield
    except:
        traceback.print_exc()
        raise
    finally:
        _set_series_ready(engine, name, True, namespace=namespace)


def _insertion_dates(tsa,
                     name,
                     from_insertion_date=None,
                     to_insertion_date=None):
    """ Produces the insertion dates of a series, bypassing its local cache
    while still using its component series cache if any

    """
    tsh, engine = tsa.tsh, tsa.engine
    formula = tsa.formula(name)
    tree = tsh._expanded_formula(
        engine,
        formula,
        qargs={}  # we want the stopnames mecanism to work for us
    )
    idates = []

    with engine.begin() as cn:
        for sname, localtree in tsh.find_series(cn, tree).items():
            if not tsh.exists(cn, sname):
                # delegate to auto operator idates impl.
                # localtree looks like:
                # ['<opname> <param1> ... <paramn>]
                idatefunc = registry.IDATES.get(
                    localtree[0]
                )
                if idatefunc:
                    idates += idatefunc(
                        cn,
                        tsh,
                        # localtree given in full: all params will be needed
                        # to build a proper series id
                        localtree,
                        from_insertion_date=from_insertion_date,
                        to_insertion_date=to_insertion_date
                    )
                    continue
                # delegate to other instance
                idates += tsa.insertion_dates(
                    name,
                    from_insertion_date=from_insertion_date,
                    to_insertion_date=to_insertion_date
                )
                continue

            if tsh.cache.exists(cn, sname):
                idates += tsh.cache.insertion_dates(
                    cn,
                    sname,
                    from_insertion_date=from_insertion_date,
                    to_insertion_date=to_insertion_date
                )
            else:
                idates += tsh.insertion_dates(
                    cn,
                    sname,
                    from_insertion_date=from_insertion_date,
                    to_insertion_date=to_insertion_date
                )

    return sorted(
        set(idates)
    )


def has_today(formula):
    tree = lisp.parse(formula)

    def _findtoday(tree):
        if isinstance(tree, list):
            if tree[0] == 'today':
                return True

            for item in tree[1:]:
                if _findtoday(item):
                    return True

        return False

    return _findtoday(tree)


def refresh_series(engine, tsa, name, final_revdate=None):
    """ Refresh a series cache """
    tsh = tsa.tsh
    policy = series_policy(engine, name, tsh.namespace)

    if not series_ready(engine, name, namespace=tsh.namespace):
        print(f'Series {name} already being updated. Bailing out. {tsh.namespace=}')
        return

    # now, prepare the formula
    formula = tsa.formula(name)

    with series_refresh_lock(engine, name, tsh.namespace):
        exists = tsh.cache.exists(engine, name)
        if exists:
            cache_idates = tsh.cache.insertion_dates(engine, name)
            cached_last_idate = cache_idates[-1]
            policy_initial_revdate = pd.Timestamp(
                eval_moment(policy['initial_revdate']),
                tz='UTC'
            )
            # usefull for discontinued series & edited caches
            initial_revdate = max(cached_last_idate, policy_initial_revdate)
        else:
            # cache creation
            initial_revdate = pd.Timestamp(
                eval_moment(policy['initial_revdate']),
                tz='UTC'
            )
            # the first cache revision contains a full horizon view of
            # the underlying series
            ts = tsa.eval_formula(
                formula,
                revision_date=initial_revdate
            )
            print(f'{initial_revdate} -> {len(ts)} points (initial full horizon import)')
            if len(ts):
                tsh.cache.update(
                    engine,
                    ts,
                    name,
                    'formula-cacher',
                    insertion_date=initial_revdate
                )
            else:
                print(f'there was no data (!) for the first cache revision ({name})')

        now = pd.Timestamp.utcnow()
        idates = _insertion_dates(
            tsa,
            name,
            from_insertion_date=initial_revdate,
            to_insertion_date=now
        )
        do_all_idates = has_today(formula)
        if (not idates or not len(idates)) and not do_all_idates:
            print(f'no idate over {initial_revdate} -> {now}, no refresh')
            return  # that's an odd series, let's bail out

        final_revdate = final_revdate or pd.Timestamp(datetime.utcnow(), tz='UTC')
        print('starting range refresh', initial_revdate, '->', final_revdate)

        cron_range = croniter_range(
            initial_revdate,
            final_revdate,
            policy['revdate_rule']
        )

        if do_all_idates:
            # let's not prune anything
            reduced_cron = cron_range
        else:
            reduced_cron = helper.reduce_frequency(list(cron_range), idates)

        for idx, revdate in enumerate(reduced_cron):
            # native python datetimes lack some method
            revdate = pd.Timestamp(revdate)

            if exists:
                if revdate == initial_revdate:
                    continue

            if not idx and not exists:
                # cache creation: first revision was created before
                continue

            from_value_date = eval_moment(
                policy['look_before'],
                {'now': revdate}
            )
            to_value_date = eval_moment(
                policy['look_after'],
                {'now': revdate}
            )

            ts = tsa.eval_formula(
                formula,
                revision_date=revdate,
                from_value_date=from_value_date,
                to_value_date=to_value_date,
            )
            print(f'{revdate} -> {len(ts)} points')
            if len(ts):
                tsh.cache.update(
                    engine,
                    ts,
                    name,
                    'formula-cacher',
                    insertion_date=revdate
                )


def refresh_now(engine, tsa, name):
    """ Refresh a series cache on the spot (do not follow revdate_rule) """
    tsh = tsa.tsh
    policy = series_policy(engine, name, tsh.namespace)

    if not series_ready(engine, name, namespace=tsh.namespace):
        print(f'Series {name} already being updated. Bailing out.')
        return

    exists = tsh.cache.exists(engine, name)
    if not exists:
        print(f'refresh_now only works on an established cache (series {name}).')
        return

    now = pd.Timestamp.utcnow()
    formula = tsa.formula(name)
    with series_refresh_lock(engine, name, tsh.namespace):
        from_value_date = eval_moment(
            policy['look_before'],
            {'now': now}
        )
        to_value_date = eval_moment(
            policy['look_after'],
            {'now': now}
        )

        ts = tsa.eval_formula(
            formula,
            from_value_date=from_value_date,
            to_value_date=to_value_date,
        )
        if len(ts):
            tsh.cache.update(
                engine,
                ts,
                name,
                'formula-cacher'
            )


def refresh_policy(tsa, policy, initial, final_revdate=None):
    tsh = tsa.tsh
    names = policy_series(
        tsa.engine,
        policy,
        namespace=tsh.namespace
    )

    print(
        f'Refreshing cache policy `{policy}` '
        f'({initial=}) (ns={tsh.namespace})'
    )
    # sort series by dependency order
    # we want the leafs to be computed
    engine = tsa.engine

    if not initial and not policy_ready(engine, policy, namespace=tsh.namespace):
        print('Cache is not ready and this is not an initial run, stopping now.')
        return

    unames = set()
    # put the uncached serie at the end
    for name in names:
        if not tsh.cache.exists(engine, name):
            unames.add(name)

    names = [
        name for name in names
        if name not in unames
    ]

    cmp = helper.comparator(tsh, engine)
    names.sort(key=cmp_to_key(cmp))

    unames = list(unames)
    unames.sort(key=cmp_to_key(cmp))

    print(f'refresh in order: {names}, then {unames}')

    failed = []

    # first batch (potentially just a refresh if not an initial run)
    print(f'first batch (cache update) ({len(names)} series)')
    for name in names:
        print('refresh ->', name)
        with engine.begin() as cn:
            if tsh.live_content_hash(cn, name) != tsh.content_hash(cn, name):
                tsh.invalidate_cache(cn, name)

        try:
            refresh_series(
                engine,
                tsa,
                name,
                final_revdate=final_revdate
            )
        except Exception as err:
            failed.append(name)
            traceback.print_exc()
            print(f'series `{name}` crashed because {err}')

    # second batch (potentially re-filling invalidated caches)
    print(f'second batch (full cache construction) ({len(unames)} series)')
    for name in unames:
        print('refresh ->', name)
        try:
            refresh_series(
                engine,
                tsa,
                name,
                final_revdate=final_revdate
            )
        except Exception as err:
            failed.append(name)
            traceback.print_exc()
            print(f'series `{name}` crashed because {err}')

    set_policy_ready(engine, policy, True, namespace=tsh.namespace)
    assert policy_ready(engine, policy, namespace=tsh.namespace)

    if failed:
        print(
            f'the following series failed to be refreshed: '
            f'{", ".join(failed)}'
        )
        raise Exception(f'failed series on refresh: {failed}')


def refresh_policy_now(tsa, policy):
    tsh = tsa.tsh
    engine = tsa.engine
    names = policy_series(
        tsa.engine,
        policy,
        namespace=tsh.namespace
    )

    print(
        f'Spot refresh of cache policy `{policy}` '
        f'(ns={tsh.namespace}) series: {names}'
    )
    if not policy_ready(engine, policy, namespace=tsh.namespace):
        print('Cache is not ready and this is not an initial run, stopping now.')
        return

    unames = set()
    # put the uncached serie at the end
    for name in names:
        if not tsh.cache.exists(engine, name):
            unames.add(name)

    names = [
        name for name in names
        if name not in unames
    ]

    cmp = helper.comparator(tsh, engine)
    names.sort(key=cmp_to_key(cmp))

    unames = list(unames)
    unames.sort(key=cmp_to_key(cmp))

    print(f'updating ({len(names)} series)')
    for name in names:
        print('refresh ->', name)
        refresh_now(
            engine,
            tsa,
            name,
        )

    if len(unames):
        print(f'second batch (full cache construction) ({len(unames)} series)')
        print('only a regular update can fix them')


@contextmanager
def suspended_policies(engine, namespace='tsh'):
    """A context manager to deactivate  / reactivate policies.
    Will be useful when playing (re-setting for instance) the
    rework tasks and schedule rules in a ... brutal way.
    """
    polnames = {
        name for name, in
        engine.execute(
            f'select name from "{namespace}".cache_policy'
        ).fetchall()
    }

    active = {
        name for name in polnames
        if scheduled_policy(engine, name, namespace)
    }
    for name in active:
        unschedule_policy(engine, name, namespace)

    try:
        yield
    finally:
        for name in active:
            schedule_policy(engine, name, namespace)
