from datetime import datetime

from croniter import (
    croniter,
    croniter_range
)
import pandas as pd
from psyl import lisp
from rework import io as rio
from sqlhelp import insert


def eval_moment(expr):
    return lisp.evaluate(expr, env=rio._MOMENT_ENV)


def validate_policy(
        initial_revdate,
        from_date,
        to_date,
        look_before,
        look_after,
        revdate_rule,
        schedule_rule
):
    """ Validate each of the four parameters of a given cache policy """
    badinputs = []
    for name, val in (
            ('initial_revdate', initial_revdate),
            ('from_date', from_date),
            ('to_date', to_date),
            ('look_before', look_before),
            ('look_after', look_after)):
        try:
            eval_moment(val)
        except:
            badinputs.append((name, val))

    if not croniter.is_valid(revdate_rule):
        badinputs.append(('revdate_rule', revdate_rule))
    if not croniter.is_valid(schedule_rule):
        badinputs.append(('schedule_rule', schedule_rule))
    return badinputs


def new_cache_policy(
        engine,
        name,
        initial_revdate,
        from_date,
        to_date,
        look_before,
        look_after,
        revdate_rule,
        schedule_rule
):
    """ Create a new cache policy """
    badinputs = validate_policy(
        initial_revdate,
        from_date,
        to_date,
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
            'tsh.cache_policy'
        ).values(
            name=name,
            initial_revdate=initial_revdate,
            from_date=from_date,
            to_date=to_date,
            look_before=look_before,
            look_after=look_after,
            revdate_rule=revdate_rule,
            schedule_rule=schedule_rule
        )
        q.do(cn)


def cache_policy_by_name(engine, name):
    """ Return a cache policy by name, as a dict """
    with engine.begin() as cn:
        p = cn.execute(
            'select initial_revdate, from_date, to_date, '
            '       revdate_rule, schedule_rule '
            'from tsh.cache_policy'
        ).fetchone()
    return dict(p)


def set_cache_policy(cn, policy_name, series_name):
    """ Associate a cache policy to a series """
    q = (
        'insert into tsh.cache_policy_series '
        '(cache_policy_id, series_id) '
        'values ('
        ' (select id '
        '  from tsh.cache_policy '
        '  where name = %(cachename)s), '
        ' (select id '
        '  from tsh.formula '
        '  where name = %(seriesname)s) '
        ')'
    )
    cn.execute(
        q,
        cachename=policy_name,
        seriesname=series_name
    )


def ready(cn, series_name):
    """ Return the cache readiness for a series """
    q = (
        'select ready '
        'from tsh.cache_policy as cache, '
        '     tsh.cache_policy_series as middle, '
        '     tsh.formula as series '
        'where cache.id = middle.cache_policy_id and '
        '      series_id = series.id and '
        '      series.name = %(seriesname)s'
    )
    return cn.execute(
        q,
        seriesname=series_name
    ).scalar()


def series_policy(cn, series_name):
    """ Return the cache policy for a series """
    q = (
        'select initial_revdate, from_date, to_date, '
        '       revdate_rule, schedule_rule '
        'from tsh.cache_policy as cache, '
        '     tsh.cache_policy_series as middle, '
        '     tsh.formula as series '
        'where cache.id = middle.cache_policy_id and '
        '      series_id = series.id and '
        '      series.name = %(seriesname)s'
    )
    p = cn.execute(
        q,
        seriesname=series_name
    ).fetchone()
    if p is None:
        return
    return dict(p)



def refresh_cache(engine, tsh, tsa, name, final_revdate=None):
    policy = series_policy(engine, name)

    if tsh.cache.exists(engine, name):
        idates = tsh.cache.insertion_dates(engine, name)
        initial_revdate = idates[0]
        now = pd.Timestamp(datetime.utcnow(), tz='utc')
        from_value_date = now + policy['look_before']
        to_value_date = now + policy['look_after']
    else:
        initial_revdate = eval_moment(policy['initial_revdate'])
        from_value_date = eval_moment(policy['from_date'])
        to_value_date = eval_moment(policy['to_date'])

    for revdate in croniter_range(
            initial_revdate,
            final_revdate or datetime.now(),
            policy['revdate_rule']
    ):
        ts = tsa.get(
            name,
            revision_date=revdate,
            from_value_date=from_value_date,
            to_value_date=to_value_date
        )
        tsh.cache.update(
            engine,
            ts,
            name,
            'formula-cacher'
        )

    with engine.begin() as cn:
        cn.execute('update tsh.cache_policy set ready = true')
