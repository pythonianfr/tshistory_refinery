from datetime import (
    datetime,
    timedelta
)

from croniter import (
    croniter,
    croniter_range
)
import pandas as pd
from psyl import lisp
from rework import (
    api as rapi,
    io as rio
)
from sqlhelp import (
    insert,
    update
)


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
    rapi.schedule(
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


def delete_policy(engine, name, namespace='tsh'):
    with engine.begin() as cn:
        _remove_scheduled_tasks(cn, name, namespace)

        cn.execute(
            f'delete from "{namespace}".cache_policy '
            f'where name = %(name)s',
            name=name
        )


def policy_by_name(engine, name, namespace='tsh'):
    """ Return a cache policy by name, as a dict """
    with engine.begin() as cn:
        p = cn.execute(
            f'select initial_revdate, '
            f'       revdate_rule, schedule_rule '
            f'from "{namespace}".cache_policy'
        ).fetchone()
    return dict(p)


def policy_series(cn, policyname, namespace='tsh'):
    q = (
        f'select series.name '
        f'from "{namespace}".cache_policy as cache, '
        f'     "{namespace}".cache_policy_series as middle, '
        f'     "{namespace}".formula as series '
        f'where cache.id = middle.cache_policy_id and '
        f'      series_id = series.id and '
        f'      cache.name = %(policyname)s '
        f'order by series.name asc'
    )
    return [
        name for name, in cn.execute(
            q,
            policyname=policyname
        ).fetchall()
    ]


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
        f'  from "{namespace}".formula '
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
        f' select id from "{namespace}".formula where '
        f' name = %(name)s'
        f')'
    )
    cn.execute(
        q,
        name=series_name
    )


def ready(cn, series_name, namespace='tsh'):
    """ Return the cache readiness for a series """
    q = (
        f'select ready '
        f'from "{namespace}".cache_policy as cache, '
        f'     "{namespace}".cache_policy_series as middle, '
        f'     "{namespace}".formula as series '
        f'where cache.id = middle.cache_policy_id and '
        f'      series_id = series.id and '
        f'      series.name = %(seriesname)s'
    )
    return cn.execute(
        q,
        seriesname=series_name
    ).scalar()


def set_ready(engine, policy_name, val, namespace='tsh'):
    """ Mark a cache policy as ready """
    assert isinstance(val, bool)
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


def series_policy(cn, series_name, namespace='tsh'):
    """ Return the cache policy for a series """
    q = (
        f'select initial_revdate, '
        f'       look_before, look_after, '
        f'       revdate_rule, schedule_rule '
        f'from "{namespace}".cache_policy as cache, '
        f'     "{namespace}".cache_policy_series as middle, '
        f'     "{namespace}".formula as series '
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


def policy_series(cn, policy_name, namespace='tsh'):
    """ Return the series associated with a cache policy """
    q = (
        f'select series.name '
        f'from "{namespace}".cache_policy as cache, '
        f'     "{namespace}".cache_policy_series as middle, '
        f'     "{namespace}".formula as series '
        f'where cache.id = middle.cache_policy_id and '
        f'      series_id = series.id and '
        f'      cache.name = %(cachename)s'
    )
    p = cn.execute(
        q,
        cachename=policy_name
    ).fetchall()
    return [item for item, in p]


def invalidate(cn, series_name, namespace='tsh'):
    """ Reset the cache readiness for a series """
    q = (
        f'update "{namespace}".cache_policy '
        f'set ready = false '
        f'from "{namespace}".cache_policy as cache, '
        f'     "{namespace}".cache_policy_series as middle, '
        f'     "{namespace}".formula as series '
        f'where cache.id = middle.cache_policy_id and '
        f'      series_id = series.id and '
        f'      series.name = %(seriesname)s'
    )
    return cn.execute(
        q,
        seriesname=series_name
    )


def refresh(engine, tsa, name, final_revdate=None, initial=False):
    """ Refresh a series cache """
    tsh = tsa.tsh
    policy = series_policy(engine, name, tsh.namespace)

    exists = tsh.cache.exists(engine, name)
    if exists and not initial and not ready(engine, name, namespace=tsh.namespace):
        print(f'Initial cache for `{name}` is already building, bailing out.')
        return

    if exists:
        idates = tsh.cache.insertion_dates(engine, name)
        initial_revdate = idates[-1]
        lastidate = idates[0]
    else:
        initial_revdate = pd.Timestamp(
            eval_moment(policy['initial_revdate']),
            tz='UTC'
        )
        # That is well and nice but what if some autotrophic
        # operator's earlier idates comes after the specified initial
        # revdate ? In some timeseries systems, there is no idates api
        # ...  and it costs a lot scanning over non-existent data (as
        # absurd as it sounds). Hence, we will get the first known
        # idate there and use that.
        now = pd.Timestamp.utcnow()
        idates = tsa.insertion_dates(
            name,
            from_insertion_date=initial_revdate,
            to_insertion_date=now,
            nocache=True
        )
        if not idates:
            print(f'no idate over {initial_revdate} -> {now}, no refresh')
            return  # that's an odd series, let's bail out

        if idates[0] > initial_revdate:
            initial_revdate = idates[0]
        # we want to not filter out the first revdate
        lastidate = initial_revdate - timedelta(days=1)

    final_revdate = final_revdate or pd.Timestamp(datetime.utcnow(), tz='UTC')
    print('starting range refresh', initial_revdate, '->', final_revdate)

    for revdate in croniter_range(
        initial_revdate,
        final_revdate,
        policy['revdate_rule']
    ):
        # native python datetimes lack some method
        revdate = pd.Timestamp(revdate)

        if exists:
            if revdate == initial_revdate:
                continue
        else:
            curidate = max(idate for idate in idates
                           if idate <= revdate)
            if curidate == lastidate:
                # while revdate advances, the source idate is the same
                # as before -> the current revdate is spurious,
                # let's avoid a useless source query
                print('skip spurious revdate', revdate)
                continue
            lastidate = curidate

        from_value_date = eval_moment(
            policy['look_before'],
            {'now': revdate}
        )
        to_value_date = eval_moment(
            policy['look_after'],
            {'now': revdate}
        )

        print(revdate)
        ts = tsa.get(
            name,
            revision_date=revdate,
            from_value_date=from_value_date,
            to_value_date=to_value_date,
            nocache=True
        )
        if len(ts):
            tsh.cache.update(
                engine,
                ts,
                name,
                'formula-cacher',
                insertion_date=revdate
            )
