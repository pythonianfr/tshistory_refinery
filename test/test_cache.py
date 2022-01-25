import pandas as pd
import pytest

from tshistory.testutil import assert_df

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
        '(shifted (today) #:days -10)',
        '(shifted (today) #:days 10)',
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
        'revdate_rule': '0 1 * * *',
        'schedule_rule': '0 8-18 * * *',
        'to_date': '(date "2022-1-5")'
    }

    refresh_cache(
        engine,
        tsh,
        tsa,
        'over-ground-0',
        final_revdate=pd.Timestamp('2023-1-5')
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
