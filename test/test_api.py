from datetime import datetime

import pytest
import pandas as pd

from tshistory import api
from tshistory.testutil import (
    assert_df
)
from tshistory.schema import tsschema
from tshistory_formula.schema import formula_schema

from tshistory_refinery import tsio


def make_api(engine, ns, sources=()):
    tsschema(ns).create(engine)
    tsschema(ns + '-upstream').create(engine)
    formula_schema(ns).create(engine)

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


@pytest.fixture(scope='session')
def tsa2(engine):
    ns = 'test-remote'
    tsschema(ns).create(engine)
    tsschema(ns + '-upstream').create(engine)
    formula_schema(ns).create(engine)
    dburi = str(engine.url)

    return api.timeseries(
        dburi,
        namespace=ns,
        handler=tsio.timeseries
    )


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


def test_manual_overrides(tsa):
    # start testing manual overrides
    ts_begin = genserie(datetime(2010, 1, 1), 'D', 5, [2.])
    ts_begin.loc['2010-01-04'] = -1
    tsa.update('ts_mixte', ts_begin, 'test')

    # -1 represents bogus upstream data
    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04   -1.0
2010-01-05    2.0
""", tsa.get('ts_mixte'))

    # test marker for first inserstion
    _, marker = tsa.edited('ts_mixte')
    assert False == marker.any()

    # refresh all the period + 1 extra data point
    ts_more = genserie(datetime(2010, 1, 2), 'D', 5, [2])
    ts_more.loc['2010-01-04'] = -1
    tsa.update('ts_mixte', ts_more, 'test')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04   -1.0
2010-01-05    2.0
2010-01-06    2.0
""", tsa.get('ts_mixte'))

    # just append an extra data point
    # with no intersection with the previous ts
    ts_one_more = genserie(datetime(2010, 1, 7), 'D', 1, [2])
    tsa.update('ts_mixte', ts_one_more, 'test')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04   -1.0
2010-01-05    2.0
2010-01-06    2.0
2010-01-07    2.0
""", tsa.get('ts_mixte'))

    assert tsa.supervision_status('ts_mixte') == 'unsupervised'

    # edit the bogus upstream data: -1 -> 3
    # also edit the next value
    ts_manual = genserie(datetime(2010, 1, 4), 'D', 2, [3])
    tsa.update('ts_mixte', ts_manual, 'test', manual=True)
    assert tsa.supervision_status('ts_mixte') == 'supervised'

    tsa.edited('ts_mixte')
    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    3.0
2010-01-05    3.0
2010-01-06    2.0
2010-01-07    2.0
""", tsa.get('ts_mixte'))

    # refetch upstream: the fixed value override must remain in place
    assert -1 == ts_begin['2010-01-04']
    tsa.update('ts_mixte', ts_begin, 'test')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    3.0
2010-01-05    3.0
2010-01-06    2.0
2010-01-07    2.0
""", tsa.get('ts_mixte'))

    # upstream provider fixed its bogus value: the manual override
    # should be replaced by the new provider value
    ts_begin_amend = ts_begin.copy()
    ts_begin_amend.iloc[3] = 2
    tsa.update('ts_mixte', ts_begin_amend, 'test')
    ts, marker = tsa.edited('ts_mixte')

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
    tsa.update('ts_mixte', ts_edit, 'test', manual=True)
    assert 2 == tsa.get('ts_mixte')['2010-01-04']  # still
    ts, marker = tsa.edited('ts_mixte')

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
    tsa.update('ts_mixte', ts_edit, 'test', manual=True)
    assert 4 == tsa.get('ts_mixte')['2010-01-04']  # still

    ts_auto_resend_the_same = pd.Series([2], index=drange)
    tsa.update('ts_mixte', ts_auto_resend_the_same, 'test')
    assert 4 == tsa.get('ts_mixte')['2010-01-04']  # still

    ts_auto_fix_value = pd.Series([7], index=drange)
    tsa.update('ts_mixte', ts_auto_fix_value, 'test')
    assert 7 == tsa.get('ts_mixte')['2010-01-04']  # still

    # test the marker logic
    # which helps put nice colour cues in the excel sheet
    # get_ts_marker returns a ts and its manual override mask
    # test we get a proper ts
    ts_auto, _ = tsa.edited('ts_mixte')

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
    tsa.update('ts_mixte', ts_manual, 'test', manual=True)

    ts_manual = genserie(datetime(2010, 1, 9), 'D', 1, [3])
    tsa.update('ts_mixte', ts_manual, 'test', manual=True)
    tsa.update('ts_mixte', ts_auto, 'test')

    upstream_fix = pd.Series([2.5], index=[datetime(2010, 1, 5)])
    tsa.update('ts_mixte', upstream_fix, 'test')

    # we had three manual overrides, but upstream fixed one of its values
    tip_ts, tip_marker = tsa.edited('ts_mixte')

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
    tsa.update('ts_mixte', ts_manual, 'test', manual=True)
    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    7.0
2010-01-05    2.5
2010-01-06    3.0
2010-01-07    2.0
2010-01-09    4.0
""", tsa.get('ts_mixte'))


def test_get_many(tsa):
    ts_base = genserie(datetime(2010, 1, 1), 'D', 3, [1])
    tsa.update('base', ts_base, 'test')

    tsa.register_formula(
        'scalarprod',
        '(* 2 (series "base"))'
    )

    v, m, o = tsa.values_markers_origins('scalarprod')
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
        tsa.update('comp1', ts_base * idx, 'test',
                   insertion_date=idate)
        tsa.update('comp2', ts_base * idx, 'test',
                   insertion_date=idate)

    tsa.register_formula(
        'repusum',
        '(add (series "comp1") (series "comp2"))'
    )

    tsa.register_formula(
        'repuprio',
        '(priority (series "comp1") (series "comp2"))'
    )

    lastsum, _, _ = tsa.values_markers_origins('repusum')
    pastsum, _, _ = tsa.values_markers_origins(
        'repusum',
        revision_date=datetime(2015, 1, 2, 18)
    )

    lastprio, _, _ = tsa.values_markers_origins('repuprio')
    pastprio, _, _ = tsa.values_markers_origins(
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


def test_origin(tsa):
    ts_real = genserie(datetime(2010, 1, 1), 'D', 10, [1])
    ts_nomination = genserie(datetime(2010, 1, 1), 'D', 12, [2])
    ts_forecast = genserie(datetime(2010, 1, 1), 'D', 20, [3])

    tsa.update('realised', ts_real, 'test')
    tsa.update('nominated', ts_nomination, 'test')
    tsa.update('forecasted', ts_forecast, 'test')

    tsa.register_formula(
        'serie5',
        '(priority (series "realised") (series "nominated") (series "forecasted"))'
    )

    values, _, origin = tsa.values_markers_origins('serie5')

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

    # we remove the last value of the 2 first series which are considered as bogus

    tsa.register_formula(
        'serie6',
        '(priority '
        ' (series "realised" #:prune 1)'
        ' (series "nominated" #:prune 1)'
        ' (series "forecasted" #:prune 0))'
    )

    values, _, origin = tsa.values_markers_origins('serie6')

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
2010-01-10    2.0
2010-01-11    2.0
2010-01-12    3.0
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
2010-01-10     nominated
2010-01-11     nominated
2010-01-12    forecasted
2010-01-13    forecasted
2010-01-14    forecasted
2010-01-15    forecasted
2010-01-16    forecasted
2010-01-17    forecasted
2010-01-18    forecasted
2010-01-19    forecasted
2010-01-20    forecasted
""", origin)

    tsa.register_formula(
        'serie7',
        '(priority '
        ' (series "realised" #:prune 1)'
        ' (series "nominated" #:prune 3)'
        ' (series "forecasted" #:prune 0))'
    )

    values, _, origin = tsa.values_markers_origins('serie7')

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
2010-01-10    forecasted
2010-01-11    forecasted
2010-01-12    forecasted
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

    # we remove the last value of the 2 first series which are considered as bogus

    tsa2.register_formula(
        'serie6',
        '(priority '
        ' (series "realised" #:prune 1)'
        ' (series "nominated" #:prune 1)'
        ' (series "forecasted" #:prune 0))'
    )

    values, _, origin = tsa1.values_markers_origins('serie6')

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
2010-01-10    2.0
2010-01-11    2.0
2010-01-12    3.0
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
2010-01-10     nominated
2010-01-11     nominated
2010-01-12    forecasted
2010-01-13    forecasted
2010-01-14    forecasted
2010-01-15    forecasted
2010-01-16    forecasted
2010-01-17    forecasted
2010-01-18    forecasted
2010-01-19    forecasted
2010-01-20    forecasted
""", origin)

    tsa2.register_formula(
        'serie7',
        '(priority '
        ' (series "realised" #:prune 1)'
        ' (series "nominated" #:prune 3)'
        ' (series "forecasted" #:prune 0))'
    )

    values, _, origin = tsa1.values_markers_origins('serie7')

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
2010-01-10    forecasted
2010-01-11    forecasted
2010-01-12    forecasted
2010-01-13    forecasted
2010-01-14    forecasted
2010-01-15    forecasted
2010-01-16    forecasted
2010-01-17    forecasted
2010-01-18    forecasted
2010-01-19    forecasted
2010-01-20    forecasted
""", origin)
