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

    with cache.suspended_policies(engine):
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

        assert not cache.scheduled_policy(engine, 'my-policy')
    assert cache.scheduled_policy(engine, 'my-policy')

    cache.delete_policy(engine, 'my-policy')
    assert not cache.scheduled_policy(engine, 'my-policy')

    assert engine.execute('select count(*) from tsh.cache_policy').scalar() == 0
    assert engine.execute('select count(*) from rework.sched').scalar() == 0


def test_policy_by_name(engine):
    cache.new_policy(
        engine,
        'my-policy-1',
        initial_revdate='(date "2022-1-1")',
        look_before='(shifted (today) #:days 15)',
        look_after='(shifted (today) #:days -10)',
        revdate_rule='0 1 * * *',
        schedule_rule='0 8-18 * * *',
    )
    cache.new_policy(
        engine,
        'my-policy-2',
        initial_revdate='(date "2023-1-1")',
        look_before='(shifted (today) #:days 15)',
        look_after='(shifted (today) #:days -10)',
        revdate_rule='0 1 * * *',
        schedule_rule='0 8-18 * * *',
    )

    p = cache.policy_by_name(engine, 'my-policy-2')
    assert p == {
        'initial_revdate': '(date "2023-1-1")',
        'revdate_rule': '0 1 * * *',
        'schedule_rule': '0 8-18 * * *'
    }


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

    cache.refresh_series(
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
    cache.refresh_series(
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
2022-01-04 00:00:00+00:00    1.0
2022-01-05 00:00:00+00:00    1.0
2022-01-06 00:00:00+00:00    2.0
2022-01-07 00:00:00+00:00    3.0
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
        pd.Timestamp('2022-01-03 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-04 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-05 00:00:00+0000', tz='UTC')
    ]

    # history points: only 3 vs 5
    assert len(tsa.history('over-ground-1')) == 3
    assert len(tsa.history('over-ground-1', nocache=True)) == 5

    # indirect
    assert len(tsa.history('over-over-ground-1')) == 3
    assert len(tsa.history('over-over-ground-1', nocache=True)) == 5

    # let's pretend two new revisions showed up
    cache.refresh_series(
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
    cache.refresh_series(
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
    cache.refresh_series(
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
    cache.refresh_series(
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

    cache.refresh_series(
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

    cache.refresh_series(
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

    cache.refresh_series(
        engine,
        federated,
        'invalidate-me',
        final_revdate=pd.Timestamp('2022-1-2', tz='UTC')
    )
    assert tsh.cache.exists(engine, 'invalidate-me')
    cache.set_policy_ready(engine, 'policy-5', True, namespace=tsh.namespace)

    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    2.0
2022-01-03 00:00:00+00:00    3.0
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
2022-01-03 00:00:00+00:00    3.0
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


def test_cache_refresh_series_now(engine, tsa):
    tsh = tsa.tsh

    tsa.update(
        'ground-refresh-now',
        pd.Series(
            range(3),
            index=pd.date_range(
                pd.Timestamp('2022-1-1'),
                freq='D',
                periods=3
            )
        ),
        'Babar',
        insertion_date=pd.Timestamp('2022-1-1', tz='UTC')
    )
    tsa.register_formula(
        'refresh-now',
        '(series "ground-refresh-now")'
    )

    tsa.new_cache_policy(
        'policy-series-refresh-now',
        initial_revdate='(date "2022-1-1")',
        look_before='(shifted now #:days -10)',
        look_after='(shifted now #:days 10)',
        revdate_rule='0 0 * * *',
        schedule_rule='0 8-18 * * *'
    )
    tsa.set_cache_policy(
        'policy-series-refresh-now',
        ['refresh-now']
    )

    assert not tsh.cache.exists(engine, 'refresh-now')
    cache.refresh_now(
        engine,
        tsa,
        'refresh-now'
    )
    # because it does not work on an empty cache
    assert not tsh.cache.exists(engine, 'refresh-now')

    cache.refresh_series(
        engine,
        tsa,
        'refresh-now'
    )
    assert tsh.cache.exists(engine, 'refresh-now')

    assert_df("""
2022-01-01    0.0
2022-01-02    1.0
2022-01-03    2.0
""", tsa.get('refresh-now'))

    tsa.update(
        'ground-refresh-now',
        pd.Series(
            range(5),
            index=pd.date_range(
                pd.Timestamp('2022-1-1'),
                freq='D',
                periods=5
            )
        ),
        'Babar',
        insertion_date=pd.Timestamp('2022-1-2', tz='UTC')
    )
    cache.refresh_now(
        engine,
        tsa,
        'refresh-now'
    )

    assert_df("""
2022-01-01    0.0
2022-01-02    1.0
2022-01-03    2.0
2022-01-04    3.0
2022-01-05    4.0
""", tsa.get('refresh-now'))

    tsa.update(
        'ground-refresh-now',
        pd.Series(
            range(7),
            index=pd.date_range(
                pd.Timestamp('2022-1-1'),
                freq='D',
                periods=7
            )
        ),
        'Babar',
        insertion_date=pd.Timestamp('2022-1-3', tz='UTC')
    )
    cache.refresh_policy_now(
        tsa,
        'policy-series-refresh-now'
    )
    assert_df("""
2022-01-01    0.0
2022-01-02    1.0
2022-01-03    2.0
2022-01-04    3.0
2022-01-05    4.0
2022-01-06    5.0
2022-01-07    6.0
""", tsa.get('refresh-now'))


def test_always_live_in_the_deep_past(engine, tsa):
    for m in range(1, 5):
        ts = pd.Series(
            [m] * 5,
            index=pd.date_range(f'2022-{m}-1', freq='D', periods=5)
        )
        tsa.update(
            'deep-fried',
            ts,
            'Babar',
            insertion_date=pd.Timestamp(f'2022-{m}-1', tz='utc')
        )

    assert_df("""
2022-01-01    1.0
2022-01-02    1.0
2022-01-03    1.0
2022-01-04    1.0
2022-01-05    1.0
2022-02-01    2.0
2022-02-02    2.0
2022-02-03    2.0
2022-02-04    2.0
2022-02-05    2.0
2022-03-01    3.0
2022-03-02    3.0
2022-03-03    3.0
2022-03-04    3.0
2022-03-05    3.0
2022-04-01    4.0
2022-04-02    4.0
2022-04-03    4.0
2022-04-04    4.0
2022-04-05    4.0
""", tsa.get('deep-fried'))

    tsa.register_formula(
        'f-deep-fried',
        '(series "deep-fried")'
    )

    tsa.new_cache_policy(
        'p-deep-fried',
        initial_revdate='(date "2022-3-1")',
        look_before='(shifted now #:days -5)',
        look_after='(shifted now #:days 5)',
        # monthly (we stop at month 3)
        revdate_rule='0 0 1 1-3 *',
        schedule_rule='0 0 1 1-3 *'
    )
    tsa.set_cache_policy(
        'p-deep-fried',
        ['f-deep-fried']
    )

    cache.refresh_series(
        engine,
        tsa,
        'f-deep-fried',
        final_revdate=pd.Timestamp('2022-3-1', tz='UTC')
    )
    cache.set_policy_ready(engine, 'p-deep-fried', True, tsa.tsh.namespace)

    idates = tsa.insertion_dates('f-deep-fried')
    assert idates == [
        pd.Timestamp('2022-01-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-02-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-03-01 00:00:00+0000', tz='UTC')
    ]

    ts = tsa.get('f-deep-fried', revision_date=pd.Timestamp('2022-1-1', tz='utc'))
    assert len(ts) == 5

    hist = tsa.history('f-deep-fried')
    assert list(hist.keys())  == [
        pd.Timestamp('2022-01-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-02-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-03-01 00:00:00+0000', tz='UTC')
    ]

    # test more code paths
    # overlapping + explicit from idate
    idates = tsa.insertion_dates(
        'f-deep-fried',
        from_insertion_date=pd.Timestamp('2022-2-1', tz='utc')
    )
    assert idates == [
        pd.Timestamp('2022-02-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-03-01 00:00:00+0000', tz='UTC')
    ]

    hist = tsa.history(
        'f-deep-fried',
        from_insertion_date=pd.Timestamp('2022-2-1', tz='utc')
    )
    assert list(hist.keys())  == [
        pd.Timestamp('2022-02-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-03-01 00:00:00+0000', tz='UTC')
    ]

    # pure left out of cache read
    idates = tsa.insertion_dates(
        'f-deep-fried',
        from_insertion_date=pd.Timestamp('2022-1-1', tz='utc'),
        to_insertion_date=pd.Timestamp('2022-2-1', tz='utc')
    )
    assert idates == [
        pd.Timestamp('2022-01-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-02-01 00:00:00+0000', tz='UTC')
    ]

    hist = tsa.history(
        'f-deep-fried',
        from_insertion_date=pd.Timestamp('2022-1-1', tz='utc'),
        to_insertion_date=pd.Timestamp('2022-2-1', tz='utc')
    )
    assert list(hist.keys())  == [
        pd.Timestamp('2022-01-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-02-01 00:00:00+0000', tz='UTC')
    ]


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


def test_values_marker_origin_and_cache(engine, tsa):
    ts = pd.Series(
        [1.] * 3,
        index=pd.date_range(
            pd.Timestamp('2022-1-1'),
            freq='D', periods=3
        )
    )
    tsa.update(
        'prim-many',
        ts,
        'test',
        insertion_date=pd.Timestamp('2022-1-1', tz='UTC')
    )

    tsa.register_formula(
        'formula-many',
        '(series "prim-many")'
    )

    tsa.register_formula(
        'second-level-formula',
        '(series "formula-many")'
    )

    tsh = tsa.tsh

    tsa.new_cache_policy(
        'policy-get-many',
        initial_revdate='(date "2022-1-1")',
        look_before='(date "2022-1-3")',
        look_after='(date "2022-1-6")',
        revdate_rule='0 0 * * *',
        schedule_rule='0 8-18 * * *'
    )
    tsa.set_cache_policy(
        'policy-get-many',
        ['formula-many']
    )

    cache.refresh_series(
        engine,
        tsa,
        'formula-many',
        final_revdate = pd.Timestamp('2022-1-2', tz='UTC')
    )
    assert tsh.cache.exists(engine, 'formula-many')
    cache.set_policy_ready(
        engine,
        'policy-get-many',
        True,
        tsa.tsh.namespace
    )

    # we refresh the underlying data, but not the cache
    ts = pd.Series(
        [1.] * 5,
        index=pd.date_range(
            pd.Timestamp('2022-1-1'),
            freq='D', periods=5
        )
    )
    tsa.update(
        'prim-many',
        ts,
        'test',
    )

    assert len(tsa.get('formula-many')) == 3
    assert len(tsa.get('formula-many', nocache=True)) == 5

    # the 2nd level formula is not cached but depends on a cached series
    assert len(tsa.get('second-level-formula')) == 3
    assert len(tsa.get('second-level-formula', nocache=True)) == 5

    # now with values_markers_origins: read correctly the data in cache
    assert len(tsa.values_markers_origins('formula-many')[0]) == 3

    # the api point values_markers_origins does have the 'nocache' argument
    with pytest.raises(TypeError) as error:
        tsa.values_markers_origins('formula-many', nocache=True)
    assert str(error.value) == (
        "values_markers_origins() got an unexpected keyword argument 'nocache'"
    )

    # test of the 2nd level formula (also read the cache as intended)
    assert len(tsa.values_markers_origins('second-level-formula')[0]) == 3


def test_errors_in_refresh_policy(engine, tsa):
    ts = pd.Series(
        [1.] * 3,
        index=pd.date_range(
            pd.Timestamp('2022-1-1'),
            freq='D', periods=3
        )
    )
    tsa.update(
        'whatever',
        ts,
        'test',
        insertion_date=pd.Timestamp('2022-01-01', tz='UTC')
    )

    tsa.register_formula(
        'formula-working',
        '(series "whatever")'
    )

    tsa.register_formula(
        'formula-failing',
        '(resample (series "whatever") "toto")'
    )

    tsa.new_cache_policy(
        'policy-with-failure',
        initial_revdate='(date "2022-1-1")',
        look_before='(date "2022-1-1")',
        look_after='(date "2022-1-6")',
        revdate_rule='0 0 * * *',
        schedule_rule='0 8-18 * * *'
    )
    tsa.set_cache_policy(
        'policy-with-failure',
        [
            'formula-working',
            'formula-failing'
        ]
    )

    with pytest.raises(Exception) as error:
        cache.refresh_policy(
            tsa,
            'policy-with-failure',
            True,
            final_revdate=pd.Timestamp('2022-01-02', tz='UTC')
        )

    # this exception is the one raised after the try/except
    assert error.value.args[0] == "failed series on refresh: ['formula-failing']"


def test_cache_revdate(engine, tsa):
    ts = pd.Series(
        [0] * 5,
        index=pd.date_range(
            pd.Timestamp('2022-1-1'),
            freq='D', periods=5
        )
    )
    tsa.update(
        'prim-revdate',
        ts,
        'test',
        insertion_date=pd.Timestamp('2022-1-1', tz='UTC')
    )
    # with one revision
    tsa.update(
        'prim-revdate',
        ts + 1,
        'test',
        insertion_date=pd.Timestamp('2022-1-2', tz='UTC')
    )

    tsa.register_formula(
        'formula-revdate',
        '(series "prim-revdate")'
    )

    tsh = tsa.tsh

    tsa.new_cache_policy(
        'policy-revdate',
        initial_revdate='(date "2022-1-1")',
        look_before='(date "2022-1-3")',
        look_after='(date "2022-1-6")',
        revdate_rule='0 0 * * *',
        schedule_rule='0 8-18 * * *'
    )
    tsa.set_cache_policy(
        'policy-revdate',
        ['formula-revdate']
    )

    # we insert different values on the cache manually
    # in order to test if the cache is read vs the formula
    # we have to tweak the series and build something weird (a cache
    # smaller that the original series)
    tsh.cache.update(
        engine,
        ts[:3] + 10 ,
        'formula-revdate',
        'pseudo-cache',
        insertion_date=pd.Timestamp('2022-1-1 01:00:00', tz='UTC')
    )
    tsh.cache.update(
        engine,
        ts[:3] + 11 ,
        'formula-revdate',
        'pseudo-cache',
        insertion_date=pd.Timestamp('2022-1-2 01:00:00', tz='UTC')
    )
    cache.set_policy_ready(
        engine,
        'policy-revdate',
        True,
        tsa.tsh.namespace
    )

    tsa.get('formula-revdate')

    # with nocache=True, we get the original series
    assert_df("""
2022-01-01    1.0
2022-01-02    1.0
2022-01-03    1.0
2022-01-04    1.0
2022-01-05    1.0
""", tsh.get(engine, 'formula-revdate', nocache=True))

    # with nocache=False, we get the cached series
    assert_df("""
2022-01-01    11.0
2022-01-02    11.0
2022-01-03    11.0
""", tsh.get(engine, 'formula-revdate'))

    # if we request outside the bounds of the cache, we return nothing:

    assert len(
        tsh.get(
            engine,
            'formula-revdate',
            from_value_date=pd.Timestamp('2022-1-4'),
            to_value_date=pd.Timestamp('2022-1-5'),
        )
    ) == 0

    # get with a revdate after the first cache insertion and
    # outside the value date bounds should return an empty series

    result = tsh.get(
        engine,
        'formula-revdate',
        from_value_date=pd.Timestamp('2022-1-4'),
        to_value_date=pd.Timestamp('2022-1-5'),
        revision_date=pd.Timestamp('2022-1-1 12:00:00', tz='UTC')
    )

    assert len(result) == 0

    # So what is the point of this contrived example?
    # In real life some series are discontinued (e.g. a pipe is closed).
    # If we request outside the bounds of the value dates but in the bounds
    # of the revision date (i.e. after the first cache insertion) we should return
    # an empty series without reading the underlying formula.
    # In case of an authotrophic operator, it would fire a request with the
    # overheads associated.
    # Since we return an empty series, it means that the underlying series was not read
    # If it was read, we would gather the points from the cache, i.e.:

    result_nocache = tsh.get(
        engine,
        'formula-revdate',
        from_value_date=pd.Timestamp('2022-1-4'),
        to_value_date=pd.Timestamp('2022-1-5'),
        revision_date=pd.Timestamp('2022-1-1 12:00:00', tz='UTC'),
        nocache=True,
    )

    assert_df("""
2022-01-04    0.0
2022-01-05    0.0
""", result_nocache)


def test_refresh_using_middle_cache(engine, tsa):
    # base serie with 2 revs
    # middle formula with 1 rev in the cache
    # top-level formula with a cache using the middle formula cache
    for i in range(1, 3):
        tsa.update(
            '2-revs',
            pd.Series(
                [i],
                index=pd.date_range(
                    pd.Timestamp(f'2022-1-{i}', tz='utc'),
                    freq='D',
                    periods=1
                )
            ),
            'Babar',
            insertion_date=pd.Timestamp(f'2022-1-{i}', tz='utc')
        )

    tsa.register_formula(
        'cache-1rev-middle',
        '(series "2-revs")'
    )

    cache.new_policy(
        engine,
        'middle-policy',
        initial_revdate='(date "2022-1-1")',
        look_before='(shifted now #:days -1)',
        look_after='(shifted now #:days 0)',
        revdate_rule='0 0 * * *',
        schedule_rule='0 8-18 * * *',
        namespace=tsa.tsh.namespace
    )
    cache.set_policy(
        engine,
        'middle-policy',
        'cache-1rev-middle',
        namespace=tsa.tsh.namespace
    )
    cache.set_policy_ready(engine, 'middle-policy', True, namespace=tsa.tsh.namespace)
    cache.refresh_series(
        engine,
        tsa,
        'cache-1rev-middle',
        final_revdate=pd.Timestamp('2022-1-1', tz='UTC')
    )

    assert len(tsa.get('cache-1rev-middle')) == 1

    tsa.register_formula(
        'cache-top',
        '(series "cache-1rev-middle")'
    )
    cache.new_policy(
        engine,
        'top-policy',
        initial_revdate='(date "2022-1-1")',
        look_before='(shifted now #:days -1)',
        look_after='(shifted now #:days 0)',
        revdate_rule='0 0 * * *',
        schedule_rule='0 8-18 * * *',
        namespace=tsa.tsh.namespace
    )
    cache.set_policy(
        engine,
        'top-policy',
        'cache-top',
        namespace=tsa.tsh.namespace
    )
    cache.set_policy_ready(engine, 'top-policy', True, namespace=tsa.tsh.namespace)
    cache.refresh_series(
        engine,
        tsa,
        'cache-top'
        # NOTE: no final-rev there
    )

    # and here we show that while being based on a cached series
    # we now use the "middle" cache
    assert len(tsa.get('cache-top')) == 1

    # for comparison with nocache:
    assert len(tsa.get('cache-top', nocache=True)) == 2


def test_interaction_hijack_and_cache(engine, tsa):
    """
    Since both the cache and the hijack_formula
    use the .exanded_formula with stopnames, this test checks
    if no bad interaction occured between the twos.

    We build a dependency formula tree as follow
    A -> B -> C
    with B cached and C hijacked by a group.
    We want to make sure that the expanded formula used in the hijacking
    can reach C without being stoped at B
    """

    ts = pd.Series(
        [1., 2., 3.],
        index=pd.date_range(
            pd.Timestamp('2022-1-1'),
            freq='D', periods=3
        )
    )

    group = pd.DataFrame(
        [[0, 1, 2], [1, 2, 3], [2, 3, 4]],
        index=pd.date_range(
            pd.Timestamp('2022-1-1'),
            freq='D', periods=3
        ),
        columns=['a', 'b', 'c']
    )

    tsa.update('ts-c', ts, 'oim', insertion_date=pd.Timestamp('2022-1-1', tz='UTC'))
    tsa.register_formula('ts-b', """(series "ts-c")""")
    tsa.register_formula('ts-a', """(series "ts-b")""")

    assert_df("""
2022-01-01    1.0
2022-01-02    2.0
2022-01-03    3.0
    """, tsa.get('ts-a'))

    tsa.group_replace('group-y', group, 'oim')
    binding = pd.DataFrame(
        [['ts-c', 'group-y', 'whatever']],
        columns=['series', 'group', 'family']
    )

    tsa.register_formula_bindings(
        'hijacked-formula',
        'ts-a',
        bindings=binding,
    )

    df = tsa.group_get('hijacked-formula')

    assert_df("""
              a    b    c
2022-01-01  0.0  1.0  2.0
2022-01-02  1.0  2.0  3.0
2022-01-03  2.0  3.0  4.0
    """, df)

    # so far, so good, we are in the standard case of the hijacking
    # now, let's setup the cache

    tsh = tsa.tsh
    tsa.new_cache_policy(
        'policy-group',
        initial_revdate='(date "2022-1-2")',
        look_before='(date "2022-1-1")',
        look_after='(date "2022-1-4")',
        revdate_rule='0 0 * * *',
        schedule_rule='0 8-18 * * *'
    )
    tsa.set_cache_policy(
        'policy-group',
        ['ts-b']
    )

    # we insert negative values on the cache manually
    tsh.cache.update(
        engine,
        -ts,
        'ts-b',
        'pseudo-cache',
    )
    cache.set_policy_ready(
        engine,
        'policy-group',
        True,
        namespace=tsh.namespace
    )

    assert_df("""
2022-01-01   -1.0
2022-01-02   -2.0
2022-01-03   -3.0
    """, tsa.get('ts-a'))

    # the negative value come from the cache: everything is Ok

    # now the point of all this:

    df = tsa.group_get('hijacked-formula')
    assert_df("""
              a    b    c
2022-01-01  0.0  1.0  2.0
2022-01-02  1.0  2.0  3.0
2022-01-03  2.0  3.0  4.0
        """, df)
    # Thanks to a dev in tshistory_formula.tsio._hijack_formula,
    # the cache does not interfer anymore with the hijacking


def test_autotrophic_series_in_cache(engine, tsa):
    tsa.delete('autotrophic_series')
    tsa.delete('upstream')
    tsh = tsa.tsh

    from tshistory_formula.registry import func, finder, insertion_dates
    from datetime import datetime

    # 1. create autotrophic series with a series we can update for the test
    @func('auto_series', auto=True)
    def auto_series(__interpreter__,
                    __from_value_date__,
                    __to_value_date__,
                    __revision_date__,
                    external_identifier: str) -> pd.Series:
        i = __interpreter__
        return i.tsh.get(
            i.cn,
            'upstream',
            revision_date=__revision_date__,
            from_value_date=__from_value_date__,
            to_value_date=__to_value_date__
        )

    @finder('auto_series')
    def auto_series_finder(cn, tsh, tree):
        return {
            'auto_series': tree
        }

    @insertion_dates('auto_series')
    def auto_series_idates(cn, tsh, tree,
                           from_insertion_date=None,
                           to_insertion_date=None,
                           from_value_date=None,
                           to_value_date=None):
        return tsh.insertion_dates(
            cn,
            'upstream',
            from_insertion_date= from_insertion_date,
            to_insertion_date=to_insertion_date,
            from_value_date=from_value_date,
            to_value_date=to_value_date,
            nocache=True
        )

    # 2. assign first values with insertion date = 2022-1-1
    plain_ts = pd.Series(
        [1] * 7,
        index=pd.date_range(
            start=datetime(2014, 12, 31),
            freq='D',
            periods=7,
        )
    )
    tsa.update(
        'upstream',
        plain_ts,
        'Babar',
        insertion_date=pd.Timestamp('2022-1-1', tz='UTC')
    )

    tsh.register_formula(
        engine,
        'autotrophic_series',
        '(auto_series "EXT-ID")'
    )

    # 3. get the data from the autotrophic series
    result_without_cache = tsh.get(
        engine,
        'autotrophic_series'
    )

    # 4. set a new cache policy
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
        'autotrophic_series',
        namespace=tsh.namespace
    )

    r = cache.series_policy_ready(
        engine,
        'autotrophic_series',
        namespace=tsh.namespace
    )
    assert r == False

    p = cache.series_policy(
        engine,
        'autotrophic_series',
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

    # 5. initial cache load with our autotrophic_series series
    cache.refresh_series(
        engine,
        tsa,
        'autotrophic_series',
    )
    cache.set_policy_ready(
        engine,
        'a-policy',
        True,
        namespace=tsh.namespace
    )

    #  6. check that cached series and non cached series are the same
    result_from_cache = tsh.get(
        engine,
        'autotrophic_series'
    )
    assert str(result_without_cache) == str(result_from_cache)

    # 7. update the upstream series on insertion 1st feb with values 2 instead of 1
    plain_ts = pd.Series(
        [2] * 7,
        index=pd.date_range(
            start=datetime(2022, 1, 25),
            freq='D',
            periods=7,
        )
    )
    tsa.update(
        'upstream',
        plain_ts,
        'Babar',
        insertion_date=pd.Timestamp('2022-2-1', tz='UTC')
    )

    # 8. check that series in not the same in and out the cache
    result_without_cache = tsh.get(
        engine,
        'autotrophic_series',
        nocache=True
    )
    result_from_cache = tsh.get(
        engine,
        'autotrophic_series'
    )
    assert str(result_without_cache) != str(result_from_cache)

    # 9. refresh series in cache
    cache.refresh_series(
        engine,
        tsa,
        'autotrophic_series'
    )
    assert tsa.has_cache('autotrophic_series')

    # 10 check that now, the series is updated
    result_from_cache = tsh.get(
        engine,
        'autotrophic_series'
    )
    assert str(result_without_cache) != str(result_from_cache)
