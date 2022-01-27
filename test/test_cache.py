import pandas as pd
import pytest

from tshistory.testutil import (
    assert_df,
    assert_hist,
    utcdt
)

from tshistory_refinery.cache import (
    new_cache_policy,
    validate_policy,
    cache_policy_by_name,
    series_policy,
    ready,
    refresh_cache,
    set_cache_policy
)


def test_invalid_cache():
    bad = validate_policy(
        'not a moment',
        'not a moment',
        'not a moment either',
        'not a moment',
        'not a moment either',
        'not a cron rule',
        'you guessed it right'
    )
    assert bad == [
        ('initial_revdate', 'not a moment'),
        ('from_date', 'not a moment'),
        ('to_date', 'not a moment either'),
        ('look_before', 'not a moment'),
        ('look_after', 'not a moment either'),
        ('revdate_rule', 'not a cron rule'),
        ('schedule_rule', 'you guessed it right')
    ]

    with pytest.raises(ValueError):
        new_cache_policy(
            None,  # we won't even need the engine
            'bogus-policy',
            'not a moment',
            'not a moment',
            'not a moment either',
            'not a moment',
            'not a moment either',
            'not a cron rule',
            'you guessed it right'
        )


def test_good_cache(engine):
    new_cache_policy(
        engine,
        'my-policy',
        '(date "2020-1-1")',
        '(date "2010-1-1")',
        '(shifted (today) #:days 15)',
        '(shifted (today) #:days -10)',
        '(shifted (today) #:days 10)',
        '0 1 * * *',
        '0 8-18 * * *'
    )

    p = cache_policy_by_name(engine, 'my-policy')
    assert p == {
        'initial_revdate': '(date "2020-1-1")',
        'from_date': '(date "2010-1-1")',
        'revdate_rule': '0 1 * * *',
        'schedule_rule': '0 8-18 * * *',
        'to_date': '(shifted (today) #:days 15)'
    }


def test_cache_a_series(engine, tsh, tsa):
    with engine.begin() as cn:
        cn.execute('delete from tsh.cache_policy')

    ts = pd.Series(
        [1., 2., 3.],
        index=pd.date_range(
            pd.Timestamp('2022-1-1'),
            freq='D', periods=3
        )
    )

    tsh.update(engine, ts, 'ground-0', 'Babar')
    tsh.register_formula(
        engine,
        'over-ground-0',
        '(series "ground-0")'
    )

    new_cache_policy(
        engine,
        'a-policy',
        '(date "2023-1-1")',
        '(date "2022-1-1")',
        '(date "2022-1-5")',
        '(shifted now #:days -10)',
        '(shifted now #:days 10)',
        '0 1 * * *',
        '0 8-18 * * *'
    )

    set_cache_policy(
        engine,
        'a-policy',
        'over-ground-0'
    )
    r = ready(
        engine,
        'no-such-series'
    )
    assert r is None

    r = ready(
        engine,
        'over-ground-0'
    )
    assert r == False

    p = series_policy(
        engine,
        'over-ground-0'
    )
    assert p == {
        'from_date': '(date "2022-1-1")',
        'initial_revdate': '(date "2023-1-1")',
        'look_after': '(shifted now #:days 10)',
        'look_before': '(shifted now #:days -10)',
        'revdate_rule': '0 1 * * *',
        'schedule_rule': '0 8-18 * * *',
        'to_date': '(date "2022-1-5")'
    }

    refresh_cache(
        engine,
        tsh,
        tsa,
        'over-ground-0',
        final_revdate=pd.Timestamp('2023-1-5', tz='UTC')
    )

    r = ready(
        engine,
        'over-ground-0'
    )
    assert r

    assert_df("""
2022-01-01    1.0
2022-01-02    2.0
2022-01-03    3.0
""", tsh.get(engine, 'over-ground-0'))


def test_cache_refresh(engine, tsh, tsa):
    new_cache_policy(
        engine,
        'another-policy',
        initial_revdate='(date "2023-1-1")',
        from_date='(date "2022-1-1")',
        to_date='(date "2022-1-31")',
        look_before='(shifted now #:days -10)',
        look_after='(shifted now #:days 10)',
        revdate_rule='0 0 * * *',
        schedule_rule='0 8-18 * * *'
    )

    # let's prepare a 3 points series with 5 revisions
    for idx, idate in enumerate(
            pd.date_range(
                utcdt(2023, 1, 1),
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
2023-01-01 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    2.0
                           2022-01-03 00:00:00+00:00    3.0
2023-01-02 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    2.0
                           2022-01-04 00:00:00+00:00    3.0
2023-01-03 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    1.0
                           2022-01-04 00:00:00+00:00    2.0
                           2022-01-05 00:00:00+00:00    3.0
2023-01-04 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    1.0
                           2022-01-04 00:00:00+00:00    1.0
                           2022-01-05 00:00:00+00:00    2.0
                           2022-01-06 00:00:00+00:00    3.0
2023-01-05 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
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

    set_cache_policy(
        engine,
        'another-policy',
        'over-ground-1'
    )
    r = ready(
        engine,
        'over-ground-1'
    )
    assert r == False

    # we only refresh up to the first 3 revisions
    refresh_cache(
        engine,
        tsh,
        tsa,
        'over-ground-1',
        final_revdate=pd.Timestamp('2023-1-3', tz='UTC')
    )

    r = ready(
        engine,
        'over-ground-1'
    )
    assert r

    # indeed, we have 3 revs in cache
    assert_hist("""
insertion_date             value_date               
2023-01-01 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    2.0
                           2022-01-03 00:00:00+00:00    3.0
2023-01-02 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    2.0
                           2022-01-04 00:00:00+00:00    3.0
2023-01-03 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    1.0
                           2022-01-04 00:00:00+00:00    2.0
                           2022-01-05 00:00:00+00:00    3.0
""", tsh.cache.history(engine, 'over-ground-1'))

    # get: cache vs nocache
    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    1.0
2022-01-03 00:00:00+00:00    1.0
2022-01-04 00:00:00+00:00    2.0
2022-01-05 00:00:00+00:00    3.0
""", tsa.get('over-ground-1'))

    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    1.0
2022-01-03 00:00:00+00:00    1.0
2022-01-04 00:00:00+00:00    1.0
2022-01-05 00:00:00+00:00    1.0
2022-01-06 00:00:00+00:00    2.0
2022-01-07 00:00:00+00:00    3.0
""", tsa.get('over-ground-1', nocache=True))

    # let's pretend two new revisions showed up
    refresh_cache(
        engine,
        tsh,
        tsa,
        'over-ground-1',
        now=pd.Timestamp('2022-1-7'),
        final_revdate=pd.Timestamp('2023-1-5', tz='UTC')
    )

    hist = tsh.cache.history(engine, 'over-ground-1')
    assert_hist("""
insertion_date             value_date               
2023-01-01 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    2.0
                           2022-01-03 00:00:00+00:00    3.0
2023-01-02 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    2.0
                           2022-01-04 00:00:00+00:00    3.0
2023-01-03 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    1.0
                           2022-01-04 00:00:00+00:00    2.0
                           2022-01-05 00:00:00+00:00    3.0
2023-01-04 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    1.0
                           2022-01-04 00:00:00+00:00    1.0
                           2022-01-05 00:00:00+00:00    2.0
                           2022-01-06 00:00:00+00:00    3.0
2023-01-05 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    1.0
                           2022-01-04 00:00:00+00:00    1.0
                           2022-01-05 00:00:00+00:00    1.0
                           2022-01-06 00:00:00+00:00    2.0
                           2022-01-07 00:00:00+00:00    3.0
""", hist)

    # now cached and uncached are the same
    tsa.get('over-ground-1').equals(tsa.get('over-ground-1', nocache=True))