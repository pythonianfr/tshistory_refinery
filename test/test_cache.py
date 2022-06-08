from functools import cmp_to_key

import pandas as pd
import pytest

from rework import api
from tshistory.testutil import (
    assert_df,
    assert_hist,
    utcdt
)

from tshistory_refinery import cache
from tshistory_refinery.helper import comparator, reduce_frequency


def test_invalid_cache():
    bad = cache.validate_policy(
        'not a moment',
        'not a moment',
        'not a moment either',
        'not a cron rule',
        'you guessed it right'
    )
    assert bad == {
        'initial_revdate': 'not a moment',
        'look_after': 'not a moment either',
        'look_before': 'not a moment',
        'revdate_rule': 'not a cron rule',
        'schedule_rule': 'you guessed it right'
    }

    with pytest.raises(ValueError):
        cache.new_policy(
            None,  # we won't even need the engine
            'bogus-policy',
            'not a moment',
            'not a moment',
            'not a moment',
            'not a moment either',
            'not a cron rule',
            'you guessed it right'
        )


def test_good_cache(engine):
    with engine.begin() as cn:
        cn.execute('delete from tsh.cache_policy')

    api.freeze_operations(engine)

    assert engine.execute('select count(*) from tsh.cache_policy').scalar() == 0
    assert engine.execute('select count(*) from rework.sched').scalar() == 0

    cache.new_policy(
        engine,
        'my-policy',
        initial_revdate='(date "2020-1-1")',
        look_before='(shifted (today) #:days 15)',
        look_after='(shifted (today) #:days -10)',
        revdate_rule='0 1 * * *',
        schedule_rule='0 8-18 * * *',
    )

    p = cache.policy_by_name(engine, 'my-policy')
    assert p == {
        'initial_revdate': '(date "2020-1-1")',
        'revdate_rule': '0 1 * * *',
        'schedule_rule': '0 8-18 * * *'
    }

    names = cache.policy_series(engine, 'my-policy')
    assert len(names) == 0

    assert engine.execute('select count(*) from tsh.cache_policy').scalar() == 1
    assert engine.execute('select count(*) from rework.sched').scalar() == 0

    assert not cache.scheduled_policy(engine, 'my-policy')
    t = cache.schedule_policy(engine, 'my-policy')
    assert t.status == 'queued'

    t2 = cache.active_task(engine, 'my-policy')
    assert t.tid == t2.tid

    assert engine.execute('select count(*) from rework.sched').scalar() == 1
    assert cache.scheduled_policy(engine, 'my-policy')
    assert engine.execute('select count(*) from rework.task').scalar() == 1

    cache.unschedule_policy(engine, 'my-policy')
    assert engine.execute('select count(*) from rework.sched').scalar() == 0
    assert not cache.scheduled_policy(engine, 'my-policy')

    cache.edit_policy(
        engine,
        'my-policy',
        initial_revdate='(date "2022-1-1")',
        look_before='(shifted (today) #:days -15)',
        look_after='(shifted (today) #:days 10)',
        revdate_rule='0 1 * * *',
        schedule_rule='0 8-18 * * *',
    )
    p = cache.policy_by_name(engine, 'my-policy')
    assert p == {
        'initial_revdate': '(date "2022-1-1")',
        'revdate_rule': '0 1 * * *',
        'schedule_rule': '0 8-18 * * *'
    }

    cache.schedule_policy(engine, 'my-policy')
    cache.delete_policy(engine, 'my-policy')
    assert not cache.scheduled_policy(engine, 'my-policy')

    assert engine.execute('select count(*) from tsh.cache_policy').scalar() == 0
    assert engine.execute('select count(*) from rework.sched').scalar() == 0



def test_cache_a_series(engine, tsa):
    tsh = tsa.tsh

    with engine.begin() as cn:
        cn.execute(f'delete from "{tsh.namespace}".cache_policy')

    ts = pd.Series(
        [1., 2., 3.],
        index=pd.date_range(
            pd.Timestamp('2022-1-1'),
            freq='D', periods=3
        )
    )

    tsh.update(
        engine, ts, 'ground-0', 'Babar',
        insertion_date=pd.Timestamp('2022-1-1', tz='utc')
    )
    tsh.register_formula(
        engine,
        'over-ground-0',
        '(series "ground-0")'
    )

    cache.new_policy(
        engine,
        'a-policy',
        '(date "2022-1-1")',
        '(shifted now #:days -10)',
        '(shifted now #:days 10)',
        '0 1 * * *',
        '0 8-18 * * *',
        namespace=tsh.namespace
    )

    cache.set_policy(
        engine,
        'a-policy',
        'over-ground-0',
        namespace=tsh.namespace
    )
    r = cache.series_policy_ready(
        engine,
        'no-such-series',
        namespace=tsh.namespace
    )
    assert r is None

    r = cache.series_policy_ready(
        engine,
        'over-ground-0',
        namespace=tsh.namespace
    )
    assert r == False

    p = cache.series_policy(
        engine,
        'over-ground-0',
        namespace=tsh.namespace
    )
    assert p == {
        'name': 'a-policy',
        'initial_revdate': '(date "2022-1-1")',
        'look_after': '(shifted now #:days 10)',
        'look_before': '(shifted now #:days -10)',
        'revdate_rule': '0 1 * * *',
        'schedule_rule': '0 8-18 * * *'
    }

    cache.refresh(
        engine,
        tsa,
        'over-ground-0',
        final_revdate=pd.Timestamp('2022-1-5', tz='UTC')
    )
    cache.set_policy_ready(engine, 'a-policy', True, namespace=tsh.namespace)

    r = cache.series_policy_ready(
        engine,
        'over-ground-0',
        namespace=tsh.namespace
    )
    assert r

    assert_df("""
2022-01-01    1.0
2022-01-02    2.0
2022-01-03    3.0
""", tsh.get(engine, 'over-ground-0'))

    # series per policy
    names = cache.policy_series(engine, 'a-policy', namespace=tsh.namespace)
    assert len(names) == 1

    tsh.register_formula(
        engine,
        'over-ground-0-b',
        '(+ 1 (series "ground-0"))'
    )
    cache.set_policy(
        engine,
        'a-policy',
        'over-ground-0-b',
        namespace=tsh.namespace
    )
    names = cache.policy_series(engine, 'a-policy', namespace=tsh.namespace)
    assert len(names) == 2

    tsh.unset_cache_policy(
        engine,
        'over-ground-0'
    )
    names = cache.policy_series(engine, 'a-policy', namespace=tsh.namespace)
    assert len(names) == 1


def test_cache_refresh(engine, tsa):
    tsa.delete('over-ground-1')
    tsh = tsa.tsh

    cache.new_policy(
        engine,
        'another-policy',
        initial_revdate='(date "2022-1-1")',
        look_before='(shifted now #:days -10)',
        look_after='(shifted now #:days 10)',
        revdate_rule='0 0 * * *',
        schedule_rule='0 8-18 * * *',
        namespace=tsh.namespace
    )
    # let's prepare a 3 points series with 5 revisions
    for idx, idate in enumerate(
            pd.date_range(
                utcdt(2022, 1, 1),
                freq='D',
                periods=5
            )
    ):
        ts = pd.Series(
            [1, 2, 3],
            index=pd.date_range(
                utcdt(2022, 1, 1 + idx),
                freq='D',
                periods=3
            )
        )
        tsa.update(
            'ground-1',
            ts,
            'Babar',
            insertion_date=idate
        )

    assert_hist("""
insertion_date             value_date               
2022-01-01 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    2.0
                           2022-01-03 00:00:00+00:00    3.0
2022-01-02 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    2.0
                           2022-01-04 00:00:00+00:00    3.0
2022-01-03 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    1.0
                           2022-01-04 00:00:00+00:00    2.0
                           2022-01-05 00:00:00+00:00    3.0
2022-01-04 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    1.0
                           2022-01-04 00:00:00+00:00    1.0
                           2022-01-05 00:00:00+00:00    2.0
                           2022-01-06 00:00:00+00:00    3.0
2022-01-05 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    1.0
                           2022-01-04 00:00:00+00:00    1.0
                           2022-01-05 00:00:00+00:00    1.0
                           2022-01-06 00:00:00+00:00    2.0
                           2022-01-07 00:00:00+00:00    3.0
""", tsa.history('ground-1'))

    # the formula that refers to the series
    tsa.register_formula(
        'over-ground-1',
        '(series "ground-1")'
    )

    cache.set_policy(
        engine,
        'another-policy',
        'over-ground-1',
        namespace=tsh.namespace
    )
    assert cache.series_policy(engine, 'over-ground-1', namespace=tsh.namespace)
    r = cache.series_policy_ready(
        engine,
        'over-ground-1',
        namespace=tsh.namespace
    )
    assert r == False

    # we only refresh up to the first 3 revisions
    cache.refresh(
        engine,
        tsa,
        'over-ground-1',
        final_revdate=pd.Timestamp('2022-1-3', tz='UTC')
    )
    cache.set_policy_ready(engine, 'another-policy', True, namespace=tsh.namespace)
    assert cache.series_policy(engine, 'over-ground-1', namespace=tsh.namespace)

    r = cache.series_policy_ready(
        engine,
        'over-ground-1',
        namespace=tsh.namespace
    )
    assert r

    # a formula over the formula (to check second order effects of the
    # api options like `live` and `nocache`)
    tsa.register_formula(
        'over-over-ground-1',
        '(series "over-ground-1")'
    )

    # indeed, we have 3 revs in cache
    assert_hist("""
insertion_date             value_date               
2022-01-01 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    2.0
                           2022-01-03 00:00:00+00:00    3.0
2022-01-02 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    2.0
                           2022-01-04 00:00:00+00:00    3.0
2022-01-03 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    1.0
                           2022-01-04 00:00:00+00:00    2.0
                           2022-01-05 00:00:00+00:00    3.0
""", tsh.cache.history(engine, 'over-ground-1'))

    # get: cache + no live
    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    1.0
2022-01-03 00:00:00+00:00    1.0
2022-01-04 00:00:00+00:00    2.0
2022-01-05 00:00:00+00:00    3.0
""", tsa.get('over-ground-1'))

    # indirect get: cache + no live
    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    1.0
2022-01-03 00:00:00+00:00    1.0
2022-01-04 00:00:00+00:00    2.0
2022-01-05 00:00:00+00:00    3.0
""", tsa.get('over-over-ground-1'))

    # get: cache + live
    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    1.0
2022-01-03 00:00:00+00:00    2.0
2022-01-04 00:00:00+00:00    3.0
""", tsa.get('over-ground-1', live=True, revision_date=pd.Timestamp('2022-1-2')))

    # indirect get: cache + live
    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    1.0
2022-01-03 00:00:00+00:00    2.0
2022-01-04 00:00:00+00:00    3.0
""", tsa.get('over-over-ground-1', live=True, revision_date=pd.Timestamp('2022-1-2')))

    # get: cache + live + revdate
    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    1.0
2022-01-03 00:00:00+00:00    1.0
2022-01-04 00:00:00+00:00    1.0
2022-01-05 00:00:00+00:00    1.0
2022-01-06 00:00:00+00:00    2.0
2022-01-07 00:00:00+00:00    3.0
""", tsa.get('over-ground-1', live=True, revision_date=pd.Timestamp('2022-1-5')))

    # indirect get: cache + live + revdate
    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    1.0
2022-01-03 00:00:00+00:00    1.0
2022-01-04 00:00:00+00:00    1.0
2022-01-05 00:00:00+00:00    1.0
2022-01-06 00:00:00+00:00    2.0
2022-01-07 00:00:00+00:00    3.0
""", tsa.get('over-over-ground-1', live=True, revision_date=pd.Timestamp('2022-1-5')))

    # get: cache + live
    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    1.0
2022-01-03 00:00:00+00:00    1.0
2022-01-04 00:00:00+00:00    1.0
2022-01-05 00:00:00+00:00    1.0
2022-01-06 00:00:00+00:00    2.0
2022-01-07 00:00:00+00:00    3.0
""", tsa.get('over-ground-1', live=True))

    # indirect get: cache + live
    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    1.0
2022-01-03 00:00:00+00:00    1.0
2022-01-04 00:00:00+00:00    1.0
2022-01-05 00:00:00+00:00    1.0
2022-01-06 00:00:00+00:00    2.0
2022-01-07 00:00:00+00:00    3.0
""", tsa.get('over-over-ground-1', live=True))

    # get: nocache
    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    1.0
2022-01-03 00:00:00+00:00    1.0
2022-01-04 00:00:00+00:00    1.0
2022-01-05 00:00:00+00:00    1.0
2022-01-06 00:00:00+00:00    2.0
2022-01-07 00:00:00+00:00    3.0
""", tsa.get('over-ground-1', nocache=True))

    # indirect get: nocache
    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    1.0
2022-01-03 00:00:00+00:00    1.0
2022-01-04 00:00:00+00:00    2.0
2022-01-05 00:00:00+00:00    3.0
""", tsa.get('over-over-ground-1', nocache=True))

    # insertion dates: only 3 vs 5
    idates = tsa.insertion_dates('over-ground-1')
    assert idates == [
        pd.Timestamp('2022-01-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-02 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-03 00:00:00+0000', tz='UTC')
    ]

    # idates: indirect
    idates = tsa.insertion_dates('over-over-ground-1')
    assert idates == [
        pd.Timestamp('2022-01-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-02 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-03 00:00:00+0000', tz='UTC')
    ]

    # idates: nocache
    idates = tsa.insertion_dates('over-ground-1', nocache=True)
    assert idates == [
        pd.Timestamp('2022-01-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-02 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-03 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-04 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-05 00:00:00+0000', tz='UTC')
    ]

    # indirect idates: nocache
    idates = tsa.insertion_dates('over-over-ground-1', nocache=True)
    assert idates == [
        pd.Timestamp('2022-01-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-02 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-03 00:00:00+0000', tz='UTC')
    ]

    # history points: only 3 vs 5
    assert len(tsa.history('over-ground-1')) == 3
    assert len(tsa.history('over-ground-1', nocache=True)) == 5

    # indirect
    assert len(tsa.history('over-over-ground-1')) == 3
    assert len(tsa.history('over-over-ground-1', nocache=True)) == 3

    # let's pretend two new revisions showed up
    cache.refresh(
        engine,
        tsa,
        'over-ground-1',
        final_revdate=pd.Timestamp('2022-1-5', tz='UTC')
    )

    hist = tsh.cache.history(engine, 'over-ground-1')
    assert_hist("""
insertion_date             value_date               
2022-01-01 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    2.0
                           2022-01-03 00:00:00+00:00    3.0
2022-01-02 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    2.0
                           2022-01-04 00:00:00+00:00    3.0
2022-01-03 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    1.0
                           2022-01-04 00:00:00+00:00    2.0
                           2022-01-05 00:00:00+00:00    3.0
2022-01-04 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    1.0
                           2022-01-04 00:00:00+00:00    1.0
                           2022-01-05 00:00:00+00:00    2.0
                           2022-01-06 00:00:00+00:00    3.0
2022-01-05 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    1.0
                           2022-01-04 00:00:00+00:00    1.0
                           2022-01-05 00:00:00+00:00    1.0
                           2022-01-06 00:00:00+00:00    2.0
                           2022-01-07 00:00:00+00:00    3.0
""", hist)

    # now cached and uncached are the same
    assert tsa.get('over-ground-1').equals(tsa.get('over-ground-1', nocache=True))

    # cache invalidation
    with engine.begin() as cn:
        tsh.invalidate_cache(cn, 'over-ground-1')

    # cached and uncached are *still* the same
    assert tsa.get('over-ground-1').equals(tsa.get('over-ground-1', nocache=True))

    r = cache.series_policy_ready(
        engine,
        'over-ground-1',
        namespace=tsh.namespace
    )
    assert r

    # we only refresh up to the first 3 revisions
    cache.refresh(
        engine,
        tsa,
        'over-ground-1',
        final_revdate=pd.Timestamp('2022-1-3', tz='UTC')
    )
    cache.set_policy_ready(engine, 'another-policy', True, namespace=tsh.namespace)

    r = cache.series_policy_ready(
        engine,
        'over-ground-1',
        namespace=tsh.namespace
    )
    assert r

    assert len(tsa.insertion_dates('over-ground-1')) == 3

    # the formula that refers to the series
    tsa.register_formula(
        'over-ground-1',
        '(+ 1 (series "ground-1"))'
    )
    # cache has been reset
    assert len(tsa.insertion_dates('over-ground-1')) == 5

    # we only refresh up to the first 3 revisions
    cache.refresh(
        engine,
        tsa,
        'over-ground-1',
        final_revdate=pd.Timestamp('2022-1-3', tz='UTC')
    )
    cache.set_policy_ready(engine, 'another-policy', True, namespace=tsh.namespace)

    assert_df("""
2022-01-01 00:00:00+00:00    2.0
2022-01-02 00:00:00+00:00    2.0
2022-01-03 00:00:00+00:00    2.0
2022-01-04 00:00:00+00:00    3.0
2022-01-05 00:00:00+00:00    4.0
""", tsa.get('over-ground-1', live=False))

    # now, let's pretend upstream does something obnoxious
    tsa.delete('ground-1')

    # let's prepare a 3 points series with 5 revisions
    # WITH DIFFERENT VALUES THIS TIME
    for idx, idate in enumerate(
            pd.date_range(
                utcdt(2022, 1, 1),
                freq='D',
                periods=5
            )
    ):
        ts = pd.Series(
            [1.1, 2.1, 3.1],
            index=pd.date_range(
                utcdt(2022, 1, 1 + idx),
                freq='D',
                periods=3
            )
        )
        tsa.update(
            'ground-1',
            ts,
            'Babar',
            insertion_date=idate
        )

    # we refresh up to rev 5
    cache.refresh(
        engine,
        tsa,
        'over-ground-1',
        final_revdate=pd.Timestamp('2022-1-5', tz='UTC')
    )

    # at this point we have a weird mix in the cache
    # but hey, that's life
    assert_hist("""
insertion_date             value_date               
2022-01-01 00:00:00+00:00  2022-01-01 00:00:00+00:00    2.0
                           2022-01-02 00:00:00+00:00    3.0
                           2022-01-03 00:00:00+00:00    4.0
2022-01-02 00:00:00+00:00  2022-01-01 00:00:00+00:00    2.0
                           2022-01-02 00:00:00+00:00    2.0
                           2022-01-03 00:00:00+00:00    3.0
                           2022-01-04 00:00:00+00:00    4.0
2022-01-03 00:00:00+00:00  2022-01-01 00:00:00+00:00    2.0
                           2022-01-02 00:00:00+00:00    2.0
                           2022-01-03 00:00:00+00:00    2.0
                           2022-01-04 00:00:00+00:00    3.0
                           2022-01-05 00:00:00+00:00    4.0
2022-01-04 00:00:00+00:00  2022-01-01 00:00:00+00:00    2.1
                           2022-01-02 00:00:00+00:00    2.1
                           2022-01-03 00:00:00+00:00    2.1
                           2022-01-04 00:00:00+00:00    2.1
                           2022-01-05 00:00:00+00:00    3.1
                           2022-01-06 00:00:00+00:00    4.1
2022-01-05 00:00:00+00:00  2022-01-01 00:00:00+00:00    2.1
                           2022-01-02 00:00:00+00:00    2.1
                           2022-01-03 00:00:00+00:00    2.1
                           2022-01-04 00:00:00+00:00    2.1
                           2022-01-05 00:00:00+00:00    2.1
                           2022-01-06 00:00:00+00:00    3.1
                           2022-01-07 00:00:00+00:00    4.1
""", tsa.history('over-ground-1'))


def test_rename_delete(engine, tsa):
    tsh = tsa.tsh

    with engine.begin() as cn:
        cn.execute(f'delete from "{tsh.namespace}".cache_policy')

    cache.new_policy(
        engine,
        'policy-3',
        initial_revdate='(date "2022-1-1")',
        look_before='(shifted now #:days -10)',
        look_after='(shifted now #:days 10)',
        revdate_rule='0 0 * * *',
        schedule_rule='0 8-18 * * *',
        namespace=tsh.namespace
    )

    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(
            utcdt(2022, 1, 1),
            freq='D',
            periods=3
        )
    )
    tsa.update(
        'ground-2',
        ts,
        'Babar',
        insertion_date=pd.Timestamp('2022-1-1')
    )

    # the formula that refers to the series
    tsa.register_formula(
        'over-ground-2',
        '(series "ground-2")'
    )

    cache.set_policy(
        engine,
        'policy-3',
        'over-ground-2',
        namespace=tsh.namespace
    )
    assert not tsh.cache.exists(engine, 'over-ground-2')

    cache.refresh(
        engine,
        tsa,
        'over-ground-2',
        final_revdate=pd.Timestamp('2022-1-1', tz='UTC')
    )

    assert tsh.cache.exists(engine, 'over-ground-2')
    tsa.rename('over-ground-2', 'a-fancy-name')
    assert not tsh.cache.exists(engine, 'over-ground-2')
    assert tsh.cache.exists(engine, 'a-fancy-name')

    tsa.delete('a-fancy-name')
    assert not tsh.cache.exists(engine, 'a-fancy-name')


def test_cache_coherency(engine, tsa):
    tsh = tsa.tsh
    engine = tsa.engine
    with engine.begin() as cn:
        cn.execute(f'delete from "{tsh.namespace}".cache_policy')

    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(
            utcdt(2022, 1, 1),
            freq='D',
            periods=3
        )
    )
    tsa.update(
        'ground-3',
        ts,
        'Babar',
        insertion_date=pd.Timestamp('2022-1-1')
    )

    tsa.register_formula(
        'ground-formula',
        '(series "ground-3")'
    )

    tsa.register_formula(
        'invalidate-me',
        '(series "ground-formula")'
    )

    cache.new_policy(
        engine,
        'policy-4',
        initial_revdate='(date "2022-1-1")',
        look_before='(shifted now #:days -1)',
        look_after='(shifted now #:days 2)',
        revdate_rule='0 0 * * *',
        schedule_rule='0 8-18 * * *',
        namespace=tsh.namespace
    )
    cache.set_policy(
        engine,
        'policy-4',
        'invalidate-me',
        namespace=tsh.namespace
    )
    assert not tsh.cache.exists(engine, 'invalidate-me')

    names = cache.policy_series(
        engine,
        'policy-4',
        namespace=tsh.namespace
    )
    assert names == ['invalidate-me']

    cache.refresh(
        engine,
        tsa,
        'invalidate-me',
        final_revdate=pd.Timestamp('2022-1-2', tz='UTC')
    )
    assert tsh.cache.exists(engine, 'invalidate-me')

    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    2.0
2022-01-03 00:00:00+00:00    3.0
""", tsa.get('invalidate-me'))

    # update without change
    tsa.register_formula(
        'ground-formula',
        '(series "ground-3")'
    )
    assert tsh.cache.exists(engine, 'invalidate-me')
    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    2.0
2022-01-03 00:00:00+00:00    3.0
""", tsa.get('invalidate-me'))

    tsa.register_formula(
        'ground-formula',
        '(+ 1 (series "ground-3"))'
    )
    assert not tsh.cache.exists(engine, 'invalidate-me')

    # here we see the truth -- the cache has been proeprly wiped
    assert_df("""
2022-01-01 00:00:00+00:00    2.0
2022-01-02 00:00:00+00:00    3.0
2022-01-03 00:00:00+00:00    4.0
""", tsa.get('invalidate-me'))


def test_federation_cache_coherency(engine, federated, remote):
    tsh = federated.tsh
    engine = federated.engine
    with engine.begin() as cn:
        cn.execute(f'delete from "{tsh.namespace}".cache_policy')

    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(
            utcdt(2022, 1, 1),
            freq='D',
            periods=3
        )
    )
    remote.update(
        'ground-remote',
        ts,
        'Babar',
        insertion_date=pd.Timestamp('2022-1-1')
    )

    remote.register_formula(
        'remote-formula',
        '(series "ground-remote")'
    )

    federated.register_formula(
        'invalidate-me',
        '(series "remote-formula")'
    )

    cache.new_policy(
        engine,
        'policy-5',
        initial_revdate='(date "2022-1-1")',
        look_before='(shifted now #:days -1)',
        look_after='(shifted now #:days 1)',
        revdate_rule='0 0 * * *',
        schedule_rule='0 8-18 * * *',
        namespace=tsh.namespace
    )
    cache.set_policy(
        engine,
        'policy-5',
        'invalidate-me',
        namespace=tsh.namespace
    )
    assert not tsh.cache.exists(engine, 'invalidate-me')

    cache.refresh(
        engine,
        federated,
        'invalidate-me',
        final_revdate=pd.Timestamp('2022-1-2', tz='UTC')
    )
    assert tsh.cache.exists(engine, 'invalidate-me')
    cache.set_policy_ready(engine, 'policy-5', True, namespace=tsh.namespace)

    # Why are we missing the point of day 3 ?
    # Since the underlying series only has 1 revision, the refresher
    # did not bother reading an already read version. Because of the
    # tiny read window we don't grab day 3.  Understand this as a
    # warning on the interaction of the revdate rule, underlying
    # revisions, and read window.
    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    2.0
""", federated.get('invalidate-me'))

    # update without change
    remote.register_formula(
        'remote-formula',
        '(series "ground-remote")'
    )
    assert tsh.cache.exists(engine, 'invalidate-me')
    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    2.0
""", federated.get('invalidate-me'))

    remote.register_formula(
        'remote-formula',
        '(+ 1 (series "ground-remote"))'
    )
    # we don't want that but this is currently unavoidable
    assert tsh.cache.exists(engine, 'invalidate-me')


def test_formula_order(engine, tsh):
    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2022, 1, 1), periods=3, freq='D')
    )

    tsh.update(
        engine,
        ts,
        'dep-base',
        'Babar'
    )

    tsh.register_formula(
        engine,
        'dep-bottom',
        '(series "dep-base")'
    )
    tsh.register_formula(
        engine,
        'dep-middle-left',
        '(+ -1 (series "dep-bottom"))'
    )
    tsh.register_formula(
        engine,
        'dep-middle-right',
        '(+ 1 (series "dep-bottom"))'
    )
    tsh.register_formula(
        engine,
        'dep-top',
        '(add (series "dep-middle-left") (series "dep-middle-right"))'
    )

    cmp = comparator(tsh, engine)
    assert cmp('a', 'a') == 0
    assert cmp('a', 'b') == -1

    assert cmp('dep-bottom', 'dep-top') == 1
    assert cmp('dep-bottom', 'dep-middle-left') == 1
    assert cmp('dep-bottom', 'dep-middle-right') == 1
    assert cmp('dep-middle-left', 'dep-top') == 1
    assert cmp('dep-middle-right', 'dep-top') == 1

    assert cmp('dep-top', 'dep-middle-left') == -1

    names = [
        'dep-bottom',
        'dep-middle-left',
        'dep-middle-right',
        'dep-top'
    ]
    names.sort(key=cmp_to_key(cmp))
    assert names == [
        'dep-top',
        'dep-middle-left',
        'dep-middle-right',
        'dep-bottom'
    ]


def test_refresh_policy(engine, tsa):
    tsh = tsa.tsh
    with engine.begin() as cn:
        cn.execute(f'delete from "{tsh.namespace}".cache_policy')

    for i in range(5):
        ts = pd.Series(
            [i] * 3,
            index=pd.date_range(
                utcdt(2022, 1, 1 + i),
                freq='D',
                periods=3
            )
        )
        tsa.update(
            'ground-ref-pol',
            ts,
            'Babar',
            insertion_date=pd.Timestamp(f'2022-1-{i+1}', tz='utc')
        )

    tsa.register_formula(
        'f1',
        '(series "ground-ref-pol")'
    )
    tsa.register_formula(
        'f2',
        '(+ 1 (series "ground-ref-pol"))'
    )

    tsa.new_cache_policy(
        'test-refresh',
        initial_revdate='(date "2022-1-1")',
        look_before='(shifted now #:days -1)',
        look_after='(shifted now #:days 1)',
        revdate_rule='0 0 * * *',
        schedule_rule='0 8-18 * * *',
    )
    tsa.set_cache_policy('test-refresh', ['f1', 'f2'])

    for name in ('f1', 'f2'):
        assert not cache.series_policy_ready(engine, name, namespace=tsh.namespace)

    # non initial refresh, cache not ready, nothing should happen
    cache.refresh_policy(tsa, 'test-refresh', False)

    for name in ('f1', 'f2'):
        assert not cache.series_policy_ready(engine, name, namespace=tsh.namespace)

    # initial refresh, cache not ready, first revisions should appear
    cache.refresh_policy(
        tsa, 'test-refresh', True,
        final_revdate=pd.Timestamp('2022-1-4', tz='utc')
    )

    assert cache.policy_ready(engine, 'test-refresh', namespace=tsh.namespace)
    for name in ('f1', 'f2'):
        assert cache.series_policy_ready(engine, name, namespace=tsh.namespace)

    # non initial refresh, cache ready, subsequent revisions should appear
    cache.refresh_policy(
        tsa, 'test-refresh', False,
        final_revdate=pd.Timestamp('2022-1-10', tz='utc')
    )

    assert cache.policy_ready(engine, 'test-refresh', namespace=tsh.namespace)
    for name in ('f1', 'f2'):
        assert cache.series_policy_ready(engine, name, namespace=tsh.namespace)

    for name in ('f1', 'f2'):
        assert tsa.has_cache(name)
    tsa.delete_cache_policy('test-refresh')
    for name in ('f1', 'f2'):
        assert not tsa.has_cache(name)


def test_reduce_cron():
    cronlist = [
        utcdt(2022, 1, 1),
        utcdt(2022, 1, 2),
        utcdt(2022, 1, 3),
        utcdt(2022, 1, 4),
        utcdt(2022, 1, 5),
    ]

    # c  c  c  c  c
    #      i    ii
    idates_inclusive = [
        utcdt(2022, 1, 2, 12),
        utcdt(2022, 1, 4, 8),
        utcdt(2022, 1, 4, 16),
        utcdt(2022, 1, 5),
    ]

    assert [
        utcdt(2022, 1, 3),
        utcdt(2022, 1, 5),
    ] == reduce_frequency(cronlist, idates_inclusive)


    #   c  c  c  c  c
    # i  ii    i
    idates_overlap_before = [
        utcdt(2021, 12, 31),
        utcdt(2022, 1, 1, 8),
        utcdt(2022, 1, 3, 16),
    ]

    assert [
        utcdt(2022, 1, 1),
        utcdt(2022, 1, 2),
        utcdt(2022, 1, 4),
    ] == reduce_frequency(cronlist, idates_overlap_before)

    #  c  c  c  c  c
    #   ii    i      i    i
    idates_overlap_after = [
        utcdt(2022, 1, 1, 8),
        utcdt(2022, 1, 1, 16),
        utcdt(2022, 1, 3, 16),
        utcdt(2022, 1, 6, 16),

    ]

    assert [
        utcdt(2022, 1, 2),
        utcdt(2022, 1, 4),
    ] == reduce_frequency(cronlist, idates_overlap_after)

    #   c  c  c  c  c
    # i      i    ii   i
    idates_overlap_both = [
        utcdt(2021, 12, 31),
        utcdt(2022, 1, 2, 12),
        utcdt(2022, 1, 4, 8),
        utcdt(2022, 1, 4, 16),
        utcdt(2022, 1, 6, 16),
    ]

    assert [
        utcdt(2022, 1, 1),
        utcdt(2022, 1, 3),
        utcdt(2022, 1, 5),
    ] == reduce_frequency(cronlist, idates_overlap_both)
