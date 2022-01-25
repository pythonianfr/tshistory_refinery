import pytest

from tshistory_refinery.cache import (
    new_cache_policy,
    validate_policy,
    cache_policy_by_name
)


def test_invalid_cache():
    bad = validate_policy(
        'not a moment',
        'not a moment either',
        'not a cron rule',
        'you guessed it right'
    )
    assert bad == [
        ('from_date', 'not a moment'),
        ('to_date', 'not a moment either'),
        ('revdate_rule', 'not a cron rule'),
        ('schedule_rule', 'you guessed it right')
    ]

    with pytest.raises(ValueError):
        new_cache_policy(
            None,  # we won't even need the engine
            'bogus-policy',
            'not a moment',
            'not a moment either',
            'not a cron rule',
            'you guessed it right'
        )


def test_good_cache(engine):
    new_cache_policy(
        engine,
        'my-policy',
        '(date "2010-1-1")',
        '(shifted (today) #:days 15)',
        '0 1 * * *',
        '0 8-18 * * *'
    )

    p = cache_policy_by_name(engine, 'my-policy')
    assert p == {
        'from_date': '(date "2010-1-1")',
        'revdate_rule': '0 1 * * *',
        'schedule_rule': '0 8-18 * * *',
        'to_date': '(shifted (today) #:days 15)'
    }
