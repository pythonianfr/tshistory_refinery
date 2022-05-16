from datetime import datetime

import pytest
import pandas as pd

from tshistory import api
from tshistory.testutil import (
    assert_df,
    assert_hist,
    make_tsx,
    utcdt
)

from tshistory_refinery import (
    cache,
    http,
    tsio,
    schema
)


def _initschema(engine, ns='tsh'):
    schema.init(engine, namespace=ns, drop=True)


def make_api(engine, ns, sources=()):
    _initschema(engine, ns)

    return api.timeseries(
        str(engine.url),
        namespace=ns,
        handler=tsio.timeseries,
        sources=sources
    )


@pytest.fixture(scope='session')
def tsa(engine):
    return make_api(engine, 'test-api')


@pytest.fixture(scope='session')
def tsa1(engine):
    tsa = make_api(
        engine,
        'test-api',
        [(str(engine.url), 'test-remote')]
    )

    return tsa


tsx = make_tsx(
    'http://test.me',
    _initschema,
    tsio.timeseries,
    http.refinery_httpapi,
    http.RefineryClient
)


@pytest.fixture(scope='session')
def tsa2(engine):
    ns = 'test-remote'
    _initschema(engine, ns)
    dburi = str(engine.url)

    return api.timeseries(
        dburi,
        namespace=ns,
        handler=tsio.timeseries
    )


@pytest.fixture(scope='session')
def tsa3(engine):
    return make_api(engine, 'tsh')


def genserie(start, freq, repeat, initval=None, tz=None, name=None):
    if initval is None:
        values = range(repeat)
    else:
        values = initval * repeat
    return pd.Series(values,
                     name=name,
                     index=pd.date_range(start=start,
                                         freq=freq,
                                         periods=repeat,
                                         tz=tz))


def test_manual_overrides(tsx):
    # start testing manual overrides
    ts_begin = genserie(datetime(2010, 1, 1), 'D', 5, [2.])
    ts_begin.loc['2010-01-04'] = -1
    tsx.update('ts_mixte', ts_begin, 'test')

    # -1 represents bogus upstream data
    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04   -1.0
2010-01-05    2.0
""", tsx.get('ts_mixte'))

    # test marker for first inserstion
    _, marker = tsx.edited('ts_mixte')
    assert False == marker.any()

    # refresh all the period + 1 extra data point
    ts_more = genserie(datetime(2010, 1, 2), 'D', 5, [2])
    ts_more.loc['2010-01-04'] = -1
    tsx.update('ts_mixte', ts_more, 'test')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04   -1.0
2010-01-05    2.0
2010-01-06    2.0
""", tsx.get('ts_mixte'))

    # just append an extra data point
    # with no intersection with the previous ts
    ts_one_more = genserie(datetime(2010, 1, 7), 'D', 1, [2])
    tsx.update('ts_mixte', ts_one_more, 'test')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04   -1.0
2010-01-05    2.0
2010-01-06    2.0
2010-01-07    2.0
""", tsx.get('ts_mixte'))

    assert tsx.supervision_status('ts_mixte') == 'unsupervised'

    # edit the bogus upstream data: -1 -> 3
    # also edit the next value
    ts_manual = genserie(datetime(2010, 1, 4), 'D', 2, [3])
    tsx.update('ts_mixte', ts_manual, 'test', manual=True)
    assert tsx.supervision_status('ts_mixte') == 'supervised'

    tsx.edited('ts_mixte')
    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    3.0
2010-01-05    3.0
2010-01-06    2.0
2010-01-07    2.0
""", tsx.get('ts_mixte'))

    # refetch upstream: the fixed value override must remain in place
    assert -1 == ts_begin['2010-01-04']
    tsx.update('ts_mixte', ts_begin, 'test')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    3.0
2010-01-05    3.0
2010-01-06    2.0
2010-01-07    2.0
""", tsx.get('ts_mixte'))

    # upstream provider fixed its bogus value: the manual override
    # should be replaced by the new provider value
    ts_begin_amend = ts_begin.copy()
    ts_begin_amend.iloc[3] = 2
    tsx.update('ts_mixte', ts_begin_amend, 'test')
    ts, marker = tsx.edited('ts_mixte')

    assert_df("""
2010-01-01    False
2010-01-02    False
2010-01-03    False
2010-01-04    False
2010-01-05     True
2010-01-06    False
2010-01-07    False
""", marker)

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    2.0
2010-01-05    3.0
2010-01-06    2.0
2010-01-07    2.0
""", ts)

    # another iterleaved editing session
    ts_edit = genserie(datetime(2010, 1, 4), 'D', 1, [2])
    tsx.update('ts_mixte', ts_edit, 'test', manual=True)
    assert 2 == tsx.get('ts_mixte')['2010-01-04']  # still
    ts, marker = tsx.edited('ts_mixte')

    assert_df("""
2010-01-01    False
2010-01-02    False
2010-01-03    False
2010-01-04    False
2010-01-05     True
2010-01-06    False
2010-01-07    False
""", marker)

    # another iterleaved editing session
    drange = pd.date_range(start=datetime(2010, 1, 4), periods=1)
    ts_edit = pd.Series([4], index=drange)
    tsx.update('ts_mixte', ts_edit, 'test', manual=True)
    assert 4 == tsx.get('ts_mixte')['2010-01-04']  # still

    ts_auto_resend_the_same = pd.Series([2], index=drange)
    tsx.update('ts_mixte', ts_auto_resend_the_same, 'test')
    assert 4 == tsx.get('ts_mixte')['2010-01-04']  # still

    ts_auto_fix_value = pd.Series([7], index=drange)
    tsx.update('ts_mixte', ts_auto_fix_value, 'test')
    assert 7 == tsx.get('ts_mixte')['2010-01-04']  # still

    # test the marker logic
    # which helps put nice colour cues in the excel sheet
    # get_ts_marker returns a ts and its manual override mask
    # test we get a proper ts
    ts_auto, _ = tsx.edited('ts_mixte')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    7.0
2010-01-05    3.0
2010-01-06    2.0
2010-01-07    2.0
""", ts_auto)

    ts_manual = genserie(datetime(2010, 1, 5), 'D', 2, [3])
    tsx.update('ts_mixte', ts_manual, 'test', manual=True)

    ts_manual = genserie(datetime(2010, 1, 9), 'D', 1, [3])
    tsx.update('ts_mixte', ts_manual, 'test', manual=True)
    tsx.update('ts_mixte', ts_auto, 'test')

    upstream_fix = pd.Series([2.5], index=[datetime(2010, 1, 5)])
    tsx.update('ts_mixte', upstream_fix, 'test')

    # we had three manual overrides, but upstream fixed one of its values
    tip_ts, tip_marker = tsx.edited('ts_mixte')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    7.0
2010-01-05    2.5
2010-01-06    3.0
2010-01-07    2.0
2010-01-09    3.0
""", tip_ts)

    assert_df("""
2010-01-01    False
2010-01-02    False
2010-01-03    False
2010-01-04    False
2010-01-05    False
2010-01-06     True
2010-01-07    False
2010-01-09     True
""", tip_marker)

    # just another override for the fun
    ts_manual.iloc[0] = 4
    tsx.update('ts_mixte', ts_manual, 'test', manual=True)
    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    7.0
2010-01-05    2.5
2010-01-06    3.0
2010-01-07    2.0
2010-01-09    4.0
""", tsx.get('ts_mixte'))


def test_get_many(tsx):
    for name in ('scalarprod', 'base'):
        tsx.delete(name)

    ts_base = genserie(datetime(2010, 1, 1), 'D', 3, [1])
    tsx.update('base', ts_base, 'test')

    tsx.register_formula(
        'scalarprod',
        '(* 2 (series "base"))'
    )

    v, m, o = tsx.values_markers_origins('scalarprod')
    assert m is None
    assert o is None
    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
""", v)

    # get_many, republications & revision date
    for idx, idate in enumerate(pd.date_range(datetime(2015, 1, 1),
                                              datetime(2015, 1, 3),
                                              freq='D',
                                              tz='utc')):
        tsx.update('comp1', ts_base * idx, 'test',
                   insertion_date=idate)
        tsx.update('comp2', ts_base * idx, 'test',
                   insertion_date=idate)

    tsx.register_formula(
        'repusum',
        '(add (series "comp1") (series "comp2"))'
    )

    tsx.register_formula(
        'repuprio',
        '(priority (series "comp1") (series "comp2"))'
    )

    lastsum, _, _ = tsx.values_markers_origins('repusum')
    pastsum, _, _ = tsx.values_markers_origins(
        'repusum',
        revision_date=datetime(2015, 1, 2, 18)
    )

    lastprio, _, _ = tsx.values_markers_origins('repuprio')
    pastprio, _, _ = tsx.values_markers_origins(
        'repuprio',
        revision_date=datetime(2015, 1, 2, 18)
    )

    assert_df("""
2010-01-01    4.0
2010-01-02    4.0
2010-01-03    4.0
""", lastsum)

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
""", pastsum)

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
""", lastprio)

    assert_df("""
2010-01-01    1.0
2010-01-02    1.0
2010-01-03    1.0
""", pastprio)


def test_get_many_federated(tsa1, tsa2):
    # same test as above
    # tsa1: local with remote source
    # tsa2: remote source
    for name in ('scalarprod', 'base', 'comp1', 'comp2', 'repusum', 'repuprio'):
        tsa2.delete(name)

    ts_base = genserie(datetime(2010, 1, 1), 'D', 3, [1])
    tsa2.update('base', ts_base, 'test')

    tsa2.register_formula(
        'scalarprod',
        '(* 2 (series "base"))'
    )

    v, m, o = tsa1.values_markers_origins('scalarprod')
    assert m is None
    assert o is None
    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
""", v)

    # get_many, republications & revision date
    for idx, idate in enumerate(pd.date_range(datetime(2015, 1, 1),
                                              datetime(2015, 1, 3),
                                              freq='D',
                                              tz='utc')):
        tsa2.update('comp1', ts_base * idx, 'test',
                   insertion_date=idate)
        tsa2.update('comp2', ts_base * idx, 'test',
                   insertion_date=idate)

    tsa2.register_formula(
        'repusum',
        '(add (series "comp1") (series "comp2"))'
    )

    tsa2.register_formula(
        'repuprio',
        '(priority (series "comp1") (series "comp2"))'
    )

    lastsum, _, _ = tsa1.values_markers_origins('repusum')
    pastsum, _, _ = tsa1.values_markers_origins(
        'repusum',
        revision_date=datetime(2015, 1, 2, 18)
    )

    lastprio, _, _ = tsa1.values_markers_origins('repuprio')
    pastprio, _, _ = tsa1.values_markers_origins(
        'repuprio',
        revision_date=datetime(2015, 1, 2, 18)
    )

    assert_df("""
2010-01-01    4.0
2010-01-02    4.0
2010-01-03    4.0
""", lastsum)

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
""", pastsum)

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
""", lastprio)

    assert_df("""
2010-01-01    1.0
2010-01-02    1.0
2010-01-03    1.0
""", pastprio)


def test_origin(tsx):
    ts_real = genserie(datetime(2010, 1, 1), 'D', 10, [1])
    ts_nomination = genserie(datetime(2010, 1, 1), 'D', 12, [2])
    ts_forecast = genserie(datetime(2010, 1, 1), 'D', 20, [3])

    tsx.update('realised', ts_real, 'test')
    tsx.update('nominated', ts_nomination, 'test')
    tsx.update('forecasted', ts_forecast, 'test')

    tsx.register_formula(
        'serie5',
        '(priority (series "realised") (series "nominated") (series "forecasted"))'
    )

    values, _, origin = tsx.values_markers_origins('serie5')

    assert_df("""
2010-01-01    1.0
2010-01-02    1.0
2010-01-03    1.0
2010-01-04    1.0
2010-01-05    1.0
2010-01-06    1.0
2010-01-07    1.0
2010-01-08    1.0
2010-01-09    1.0
2010-01-10    1.0
2010-01-11    2.0
2010-01-12    2.0
2010-01-13    3.0
2010-01-14    3.0
2010-01-15    3.0
2010-01-16    3.0
2010-01-17    3.0
2010-01-18    3.0
2010-01-19    3.0
2010-01-20    3.0
""", values)

    assert_df("""
2010-01-01      realised
2010-01-02      realised
2010-01-03      realised
2010-01-04      realised
2010-01-05      realised
2010-01-06      realised
2010-01-07      realised
2010-01-08      realised
2010-01-09      realised
2010-01-10      realised
2010-01-11     nominated
2010-01-12     nominated
2010-01-13    forecasted
2010-01-14    forecasted
2010-01-15    forecasted
2010-01-16    forecasted
2010-01-17    forecasted
2010-01-18    forecasted
2010-01-19    forecasted
2010-01-20    forecasted
""", origin)


def test_origin_federated(tsa1, tsa2):
    # same test as above
    # tsa1: local with remote source
    # tsa2: remote source
    ts_real = genserie(datetime(2010, 1, 1), 'D', 10, [1])
    ts_nomination = genserie(datetime(2010, 1, 1), 'D', 12, [2])
    ts_forecast = genserie(datetime(2010, 1, 1), 'D', 20, [3])

    for name in ('realised', 'nominated', 'forecasted', 'serie5', 'serie6', 'serie7'):
        tsa2.delete(name)

    tsa2.update('realised', ts_real, 'test')
    tsa2.update('nominated', ts_nomination, 'test')
    tsa2.update('forecasted', ts_forecast, 'test')

    tsa2.register_formula(
        'serie5',
        '(priority (series "realised") (series "nominated") (series "forecasted"))'
    )

    values, _, origin = tsa1.values_markers_origins('serie5')

    assert_df("""
2010-01-01    1.0
2010-01-02    1.0
2010-01-03    1.0
2010-01-04    1.0
2010-01-05    1.0
2010-01-06    1.0
2010-01-07    1.0
2010-01-08    1.0
2010-01-09    1.0
2010-01-10    1.0
2010-01-11    2.0
2010-01-12    2.0
2010-01-13    3.0
2010-01-14    3.0
2010-01-15    3.0
2010-01-16    3.0
2010-01-17    3.0
2010-01-18    3.0
2010-01-19    3.0
2010-01-20    3.0
""", values)

    assert_df("""
2010-01-01      realised
2010-01-02      realised
2010-01-03      realised
2010-01-04      realised
2010-01-05      realised
2010-01-06      realised
2010-01-07      realised
2010-01-08      realised
2010-01-09      realised
2010-01-10      realised
2010-01-11     nominated
2010-01-12     nominated
2010-01-13    forecasted
2010-01-14    forecasted
2010-01-15    forecasted
2010-01-16    forecasted
2010-01-17    forecasted
2010-01-18    forecasted
2010-01-19    forecasted
2010-01-20    forecasted
""", origin)



def test_today_vs_revision_date(tsx):
    tsx.register_formula(
        'constant-1',
        '(constant 1. (date "2020-1-1") (today) "D" (date "2020-2-1"))'
    )

    ts, _, _ = tsx.values_markers_origins(
        'constant-1',
        revision_date=datetime(2020, 2, 1)
    )
    assert len(ts) == 32


def test_cache(engine, tsx, tsa3):
    with engine.begin() as cn:
        cn.execute(f'delete from "tsh".cache_policy')

    with pytest.raises(ValueError) as err:
        tsx.new_cache_policy(
            'another-policy',
            initial_revdate='BOGUS',
            look_before='(shifted now #:days -10)',
            look_after='(shifted now #:days 10)',
            revdate_rule='0 0 * * *',
            schedule_rule='0 8-18 * * *'
        )
    assert err.value.args[0] == "Bad inputs for the cache policy: {'initial_revdate': 'BOGUS'}"

    tsx.new_cache_policy(
        'another-policy',
        initial_revdate='(date "2023-1-1")',
        look_before='(shifted now #:days -15)',
        look_after='(shifted now #:days 10)',
        revdate_rule='0 0 * * *',
        schedule_rule='0 8-18 * * *'
    )

    with pytest.raises(ValueError) as err:
        tsx.edit_cache_policy(
            'another-policy',
            initial_revdate='BOGUS',
            look_before='(shifted now #:days -10)',
            look_after='(shifted now #:days 10)',
            revdate_rule='0 0 * * *',
            schedule_rule='0 8-18 * * *'
        )
    assert err.value.args[0] == "Bad inputs for the cache policy: {'initial_revdate': 'BOGUS'}"

    tsx.edit_cache_policy(
        'another-policy',
        initial_revdate='(date "2022-1-1")',
        look_before='(shifted now #:days -10)',
        look_after='(shifted now #:days 2)',
        revdate_rule='0 0 * * *',
        schedule_rule='0 8-18 * * *'
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
        tsx.update(
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
""", tsx.history('ground-1'))

    # the formula that refers to the series
    tsx.register_formula(
        'over-ground-1',
        '(series "ground-1")'
    )
    tsx.register_formula(
        'over-ground-2',
        '(series "ground-1")'
    )

    tsx.set_cache_policy(
        'another-policy',
        ['over-ground-1', 'over-ground-2']
    )
    assert tsx.cache_policy_series('another-policy') == [
        'over-ground-1',
        'over-ground-2'
    ]
    r = cache.ready(
        engine,
        'over-ground-1'
    )
    assert r == False

    # we only refresh up to the first 3 revisions
    cache.refresh(
        engine,
        tsa3,
        'over-ground-1',
        final_revdate=pd.Timestamp('2022-1-3', tz='UTC')
    )
    cache.set_ready(engine, 'another-policy', True)

    r = cache.ready(
        engine,
        'over-ground-1'
    )
    assert r

    # get: cache (not live patching)
    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    1.0
2022-01-03 00:00:00+00:00    1.0
2022-01-04 00:00:00+00:00    2.0
2022-01-05 00:00:00+00:00    3.0
""", tsx.get('over-ground-1'))

    # get: cache + live patching
    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    1.0
2022-01-03 00:00:00+00:00    1.0
2022-01-04 00:00:00+00:00    1.0
2022-01-05 00:00:00+00:00    1.0
2022-01-06 00:00:00+00:00    2.0
2022-01-07 00:00:00+00:00    3.0
""", tsx.get('over-ground-1', live=True, revision_date=pd.Timestamp('2022-1-5')))

    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    1.0
2022-01-03 00:00:00+00:00    1.0
2022-01-04 00:00:00+00:00    1.0
2022-01-05 00:00:00+00:00    1.0
2022-01-06 00:00:00+00:00    2.0
2022-01-07 00:00:00+00:00    3.0
""", tsx.get('over-ground-1', nocache=True))

    assert tsx.has_cache('over-ground-1')

    # insertion dates: only 3 vs 5
    idates = tsx.insertion_dates('over-ground-1')
    assert idates == [
        pd.Timestamp('2022-01-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-02 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-03 00:00:00+0000', tz='UTC')
    ]

    idates = tsx.insertion_dates('over-ground-1', nocache=True)
    assert idates == [
        pd.Timestamp('2022-01-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-02 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-03 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-04 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-05 00:00:00+0000', tz='UTC')
    ]

    # history points: only 3 vs 5
    assert len(tsx.history('over-ground-1')) == 3
    assert len(tsx.history('over-ground-1', nocache=True)) == 5

    assert tsx.cache_free_series() == []
    assert tsx.cache_policies() == ['another-policy']

    # unset
    tsx.unset_cache_policy(
        ['over-ground-1', 'over-ground-2']
    )
    assert tsx.cache_policy_series('another-policy') == []

    # delete
    tsx.delete_cache_policy(
        'another-policy'
    )

    assert tsx.cache_free_series() == ['over-ground-1', 'over-ground-2']
    assert tsx.cache_policies() == []

    assert tsx.has_cache('over-ground-1')


def test_cacheable_formulas(tsa1, tsa2):
    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2022, 1, 1), freq='D', periods=3)
    )
    tsa1.update('cacheable-base-local', ts, 'Babar')
    tsa2.update('cacheable-base-remote', ts, 'Celeste')

    tsa1.register_formula(
        'cacheable-0',
        '(series "cacheable-base-local")'
    )
    tsa1.register_formula(
        'cacheable-1',
        '(series "cacheable-base-remote")'
    )
    tsa1.register_formula(
        'cacheable-2',
        '(add (series "cacheable-base-local") (series "cacheable-base-remote"))'
    )
    tsa1.register_formula(
        'cacheable-3',
        '(series "cacheable-2")'
    )

    tsh = tsa1.tsh
    engine = tsa1.engine

    assert tsh.cacheable_formulas(engine) == [
        'cacheable-0',
        'cacheable-1',
        'cacheable-2',
        'cacheable-3'
    ]
