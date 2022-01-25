from croniter import croniter
from psyl import lisp
from rework import io as rio
from sqlhelp import insert


def validate_policy(
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
            ('from_date', from_date),
            ('to_date', to_date),
            ('look_before', look_before),
            ('look_after', look_after)):
        try:
            lisp.evaluate(val, env=rio._MOMENT_ENV)
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
        from_date,
        to_date,
        look_before,
        look_after,
        revdate_rule,
        schedule_rule
):
    """ Create a new cache policy """
    badinputs = validate_policy(
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
            'select from_date, to_date, revdate_rule, schedule_rule '
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


def cache_policy_for_series(cn, series_name):
    " return the cache policy associated with a series "
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
