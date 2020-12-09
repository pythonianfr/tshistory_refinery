import json
from pathlib import Path
from datetime import datetime, timedelta
from dateutil import parser

import pandas as pd
import numpy as np

from tshistory.util import empty_series
from tshistory.testutil import (
    assert_df,
    genserie,
    utcdt
)
from tshistory_formula.interpreter import jsontypes


DATADIR = Path(__file__).parent / 'data'


def ingest_formulas(tsh, engine, formula_file):
    df = pd.read_csv(formula_file)
    with engine.begin() as cn:
        for row in df.itertuples():
            tsh.register_formula(
                cn,
                row.name,
                row.text,
                update=True
            )


def test_rename(engine, tsh):
    tsh.update(engine, genserie(datetime(2010, 1, 1), 'D', 3),
               'rename-me', 'Babar')
    tsh.update(engine, genserie(datetime(2010, 1, 1), 'D', 3),
               'rename-me', 'Celeste', manual=True)

    tsh.rename(engine, 'rename-me', 'renamed')
    assert tsh.get(engine, 'rename-me') is None
    assert tsh.get(engine, 'renamed') is not None
    assert tsh.upstream.get(engine, 'rename-me') is None
    assert tsh.upstream.get(engine, 'renamed') is not None


def test_delete_formula_create_primary(engine, tsh):
    tsh.register_formula(
        engine,
        'formula-then-primary',
        '(+ 2 (series "nope"))',
        False
    )

    with engine.begin() as cn:
        tsh.delete(cn, 'formula-then-primary')

    series = genserie(datetime(2020, 1, 1), 'D', 3)
    tsh.update(engine, series, 'formula-then-primary', 'Babar')

    series = tsh.get(engine, 'formula-then-primary')
    assert_df("""
2020-01-01    0.0
2020-01-02    1.0
2020-01-03    2.0
""", series)


def test_manual_overrides(engine, tsh):
    # start testing manual overrides
    ts_begin = genserie(datetime(2010, 1, 1), 'D', 5, [2.])
    ts_begin.loc['2010-01-04'] = -1
    tsh.update(engine, ts_begin, 'ts_mixte', 'test')

    # -1 represents bogus upstream data
    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04   -1.0
2010-01-05    2.0
""", tsh.get(engine, 'ts_mixte'))

    # test marker for first inserstion
    _, marker = tsh.get_ts_marker(engine, 'ts_mixte')
    assert False == marker.any()

    # refresh all the period + 1 extra data point
    ts_more = genserie(datetime(2010, 1, 2), 'D', 5, [2])
    ts_more.loc['2010-01-04'] = -1
    tsh.update(engine, ts_more, 'ts_mixte', 'test')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04   -1.0
2010-01-05    2.0
2010-01-06    2.0
""", tsh.get(engine, 'ts_mixte'))

    # just append an extra data point
    # with no intersection with the previous ts
    ts_one_more = genserie(datetime(2010, 1, 7), 'D', 1, [2])
    tsh.update(engine, ts_one_more, 'ts_mixte', 'test')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04   -1.0
2010-01-05    2.0
2010-01-06    2.0
2010-01-07    2.0
""", tsh.get(engine, 'ts_mixte'))

    assert tsh.supervision_status(engine, 'ts_mixte') == 'unsupervised'

    # edit the bogus upstream data: -1 -> 3
    # also edit the next value
    ts_manual = genserie(datetime(2010, 1, 4), 'D', 2, [3])
    tsh.update(engine, ts_manual, 'ts_mixte', 'test', manual=True)
    assert tsh.supervision_status(engine, 'ts_mixte') == 'supervised'

    tsh.get_ts_marker(engine, 'ts_mixte')
    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    3.0
2010-01-05    3.0
2010-01-06    2.0
2010-01-07    2.0
""", tsh.get(engine, 'ts_mixte'))

    # refetch upstream: the fixed value override must remain in place
    assert -1 == ts_begin['2010-01-04']
    tsh.update(engine, ts_begin, 'ts_mixte', 'test')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    3.0
2010-01-05    3.0
2010-01-06    2.0
2010-01-07    2.0
""", tsh.get(engine, 'ts_mixte'))

    # upstream provider fixed its bogus value: the manual override
    # should be replaced by the new provider value
    ts_begin_amend = ts_begin.copy()
    ts_begin_amend.iloc[3] = 2
    tsh.update(engine, ts_begin_amend, 'ts_mixte', 'test')
    ts, marker = tsh.get_ts_marker(engine, 'ts_mixte')

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
    tsh.update(engine, ts_edit, 'ts_mixte', 'test', manual=True)
    assert 2 == tsh.get(engine, 'ts_mixte')['2010-01-04']  # still
    ts, marker = tsh.get_ts_marker(engine, 'ts_mixte')

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
    tsh.update(engine, ts_edit, 'ts_mixte', 'test', manual=True)
    assert 4 == tsh.get(engine, 'ts_mixte')['2010-01-04']  # still

    ts_auto_resend_the_same = pd.Series([2], index=drange)
    tsh.update(engine, ts_auto_resend_the_same, 'ts_mixte', 'test')
    assert 4 == tsh.get(engine, 'ts_mixte')['2010-01-04']  # still

    ts_auto_fix_value = pd.Series([7], index=drange)
    tsh.update(engine, ts_auto_fix_value, 'ts_mixte', 'test')
    assert 7 == tsh.get(engine, 'ts_mixte')['2010-01-04']  # still

    # test the marker logic
    # which helps put nice colour cues in the excel sheet
    # get_ts_marker returns a ts and its manual override mask
    # test we get a proper ts
    ts_auto, _ = tsh.get_ts_marker(engine, 'ts_mixte')

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
    tsh.update(engine, ts_manual, 'ts_mixte', 'test', manual=True)

    ts_manual = genserie(datetime(2010, 1, 9), 'D', 1, [3])
    tsh.update(engine, ts_manual, 'ts_mixte', 'test', manual=True)
    tsh.update(engine, ts_auto, 'ts_mixte', 'test')

    upstream_fix = pd.Series([2.5], index=[datetime(2010, 1, 5)])
    tsh.update(engine, upstream_fix, 'ts_mixte', 'test')

    # we had three manual overrides, but upstream fixed one of its values
    tip_ts, tip_marker = tsh.get_ts_marker(engine, 'ts_mixte')

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
    tsh.update(engine, ts_manual, 'ts_mixte', 'test', manual=True)
    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    7.0
2010-01-05    2.5
2010-01-06    3.0
2010-01-07    2.0
2010-01-09    4.0
""", tsh.get(engine, 'ts_mixte'))


def test_first_manual(engine, tsh):
    ts_begin = genserie(datetime(2010, 1, 1), 'D', 10)
    tsh.update(engine, ts_begin, 'ts_only', 'test', manual=True)

    assert_df("""
2010-01-01    0.0
2010-01-02    1.0
2010-01-03    2.0
2010-01-04    3.0
2010-01-05    4.0
2010-01-06    5.0
2010-01-07    6.0
2010-01-08    7.0
2010-01-09    8.0
2010-01-10    9.0
""", tsh.get(engine, 'ts_only'))

    tsh.update(engine, ts_begin, 'ts_only', 'test', manual=True)
    assert_df("""
2010-01-01    0.0
2010-01-02    1.0
2010-01-03    2.0
2010-01-04    3.0
2010-01-05    4.0
2010-01-06    5.0
2010-01-07    6.0
2010-01-08    7.0
2010-01-09    8.0
2010-01-10    9.0
""", tsh.get(engine, 'ts_only'))

    ts_slight_variation = ts_begin.copy()
    ts_slight_variation.iloc[3] = 0
    ts_slight_variation.iloc[6] = 0
    tsh.update(engine, ts_slight_variation, 'ts_only', 'test')
    tsh.get(engine, 'ts_only').to_string().strip()

    # should be a noop
    tsh.update(engine, ts_slight_variation, 'ts_only', 'test', manual=True)
    _, marker = tsh.get_ts_marker(engine, 'ts_only')

    assert_df("""
2010-01-01    False
2010-01-02    False
2010-01-03    False
2010-01-04    False
2010-01-05    False
2010-01-06    False
2010-01-07    False
2010-01-08    False
2010-01-09    False
2010-01-10    False
""", marker)


def test_more_manual(engine, tsh):
    ts = genserie(datetime(2015, 1, 1), 'D', 5)
    tsh.update(engine, ts, 'ts_exp1', 'test')

    ts_man = genserie(datetime(2015, 1, 3), 'D', 3, -1)
    ts_man.iloc[-1] = np.nan
    # erasing of the laste value for the date 5/1/2015
    tsh.update(engine, ts_man, 'ts_exp1', 'test', manual=True)

    ts_get = tsh.get(engine, 'ts_exp1')

    assert_df("""
2015-01-01    0.0
2015-01-02    1.0
2015-01-03   -3.0
2015-01-04   -3.0""", ts_get)

    ts_marker, marker = tsh.get_ts_marker(engine, 'ts_exp1')
    assert ts_marker.equals(ts_get)
    assert_df("""
2015-01-01    False
2015-01-02    False
2015-01-03     True
2015-01-04     True
2015-01-05     True""", marker)


def test_revision_date(engine, tsh):
    ts = genserie(datetime(2010, 1, 4), 'D', 4, [1], name='truc')
    tsh.update(
        engine, ts, 'ts_through_time', 'test',
        insertion_date=pd.Timestamp(datetime(2015, 1, 1, 15, 43, 23), tz='UTC')
    )

    ts = genserie(datetime(2010, 1, 4), 'D', 4, [2], name='truc')
    tsh.update(
        engine, ts, 'ts_through_time', 'test',
        insertion_date=pd.Timestamp(datetime(2015, 1, 2, 15, 43, 23), tz='UTC')
    )

    ts = genserie(datetime(2010, 1, 4), 'D', 4, [3], name='truc')
    tsh.update(
        engine, ts, 'ts_through_time', 'test',
        insertion_date=pd.Timestamp(datetime(2015, 1, 3, 15, 43, 23), tz='UTC')
    )

    ts = tsh.get(engine, 'ts_through_time')

    assert_df("""
2010-01-04    3.0
2010-01-05    3.0
2010-01-06    3.0
2010-01-07    3.0
""", ts)

    ts = tsh.get(engine, 'ts_through_time',
                 revision_date=datetime(2015, 1, 2, 18, 43, 23))

    assert_df("""
2010-01-04    2.0
2010-01-05    2.0
2010-01-06    2.0
2010-01-07    2.0
""", ts)

    ts = tsh.get(engine, 'ts_through_time',
                 revision_date=datetime(2015, 1, 1, 18, 43, 23))

    assert_df("""
2010-01-04    1.0
2010-01-05    1.0
2010-01-06    1.0
2010-01-07    1.0
""", ts)

    ts = tsh.get(engine, 'ts_through_time',
                 revision_date=datetime(2014, 1, 1, 18, 43, 23))

    assert len(ts) == 0


def test_before_first_insertion(engine, tsh):
    tsh.update(engine, genserie(datetime(2010, 1, 1), 'D', 11), 'ts_shtroumpf', 'test')

    # test get_marker with an unknown series vs a serie  displayed with
    # a revision date before the first insertion
    result = tsh.get_ts_marker(engine, 'unknown_ts')
    assert (None, None) == result

    a, b = tsh.get_ts_marker(engine, 'ts_shtroumpf', revision_date=datetime(1970, 1, 1))
    assert len(a) == len(b) == 0

    result1 = tsh.get_many(engine, 'unknown_ts')
    result2 = tsh.get_many(engine, 'ts_shtroumpf', revision_date=datetime(1970, 1, 1))

    assert len(result1) == len(result2)


def test_get_many(engine, tsh):
    ts_base = genserie(datetime(2010, 1, 1), 'D', 3, [1])
    tsh.update(engine, ts_base, 'base', 'test')

    tsh.register_formula(
        engine, 'scalarprod',
        '(* 2 (series "base"))'
    )

    v, m, o = tsh.get_many(engine, 'scalarprod')
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
        tsh.update(engine, ts_base * idx, 'comp1', 'test',
                   insertion_date=idate)
        tsh.update(engine, ts_base * idx, 'comp2', 'test',
                   insertion_date=idate)

    tsh.register_formula(
        engine, 'repusum',
        '(add (series "comp1") (series "comp2"))'
    )

    tsh.register_formula(
        engine, 'repuprio',
        '(priority (series "comp1") (series "comp2"))'
    )

    lastsum, _, _ = tsh.get_many(engine, 'repusum')
    pastsum, _, _ = tsh.get_many(engine, 'repusum',
                                 revision_date=datetime(2015, 1, 2, 18))

    lastprio, _, _ = tsh.get_many(engine, 'repuprio')
    pastprio, _, _ = tsh.get_many(engine, 'repuprio',
                                  revision_date=datetime(2015, 1, 2, 18))

    assert_df("""
2010-01-01    4.0
2010-01-02    4.0
2010-01-03    4.0""", lastsum)

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0""", pastsum)

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0""", lastprio)

    assert_df("""
2010-01-01    1.0
2010-01-02    1.0
2010-01-03    1.0""", pastprio)


def test_origin(engine, tsh):
    ts_real = genserie(datetime(2010, 1, 1), 'D', 10, [1])
    ts_nomination = genserie(datetime(2010, 1, 1), 'D', 12, [2])
    ts_forecast = genserie(datetime(2010, 1, 1), 'D', 20, [3])

    tsh.update(engine, ts_real, 'realised', 'test')
    tsh.update(engine, ts_nomination, 'nominated', 'test')
    tsh.update(engine, ts_forecast, 'forecasted', 'test')

    tsh.register_formula(
        engine, 'serie5',
        '(priority (series "realised") (series "nominated") (series "forecasted"))'
    )

    values, _, origin = tsh.get_many(engine, 'serie5')

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

    tsh.register_formula(
        engine, 'serie6',
        '(priority '
        ' (series "realised" #:prune 1)'
        ' (series "nominated" #:prune 1)'
        ' (series "forecasted" #:prune 0))'
    )

    values, _, origin = tsh.get_many(engine, 'serie6')

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

    tsh.register_formula(
        engine, 'serie7',
        '(priority '
        ' (series "realised" #:prune 1)'
        ' (series "nominated" #:prune 3)'
        ' (series "forecasted" #:prune 0))'
    )

    values, _, origin = tsh.get_many(engine, 'serie7')

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


def test_na_and_delete(engine, tsh):
    ts_repushed = genserie(datetime(2010, 1, 1), 'D', 11)
    ts_repushed[0:3] = np.nan
    tsh.update(engine, ts_repushed, 'ts_repushed', 'test')
    diff = tsh.update(engine, ts_repushed, 'ts_repushed', 'test')
    assert len(diff) == 0


def test_exotic_name(engine, tsh):
    ts = genserie(datetime(2010, 1, 1), 'D', 11)
    tsh.update(engine, ts, 'ts-with_dash', 'test')
    tsh.get(engine, 'ts-with_dash')


def test_historic_delta(engine, tsh):
    tictac = False
    for insertion_date in pd.date_range(start=datetime(2015, 1, 1),
                                        end=datetime(2015, 1, 2),
                                        freq='H'):
        ts = genserie(start=insertion_date, freq='H', repeat=6)
        tsh.update(engine, ts, 'republication', 'test',
                   insertion_date=pd.Timestamp(insertion_date, tz='UTC'),
                   manual=tictac)
        tictac = not tictac

    ts = tsh.staircase(engine, 'republication', delta=timedelta(hours=3))
    assert_df("""
2015-01-01 03:00:00    3.0
2015-01-01 04:00:00    3.0
2015-01-01 05:00:00    3.0
2015-01-01 06:00:00    3.0
2015-01-01 07:00:00    3.0
2015-01-01 08:00:00    3.0
2015-01-01 09:00:00    3.0
2015-01-01 10:00:00    3.0
2015-01-01 11:00:00    3.0
2015-01-01 12:00:00    3.0
2015-01-01 13:00:00    3.0
2015-01-01 14:00:00    3.0
2015-01-01 15:00:00    3.0
2015-01-01 16:00:00    3.0
2015-01-01 17:00:00    3.0
2015-01-01 18:00:00    3.0
2015-01-01 19:00:00    3.0
2015-01-01 20:00:00    3.0
2015-01-01 21:00:00    3.0
2015-01-01 22:00:00    3.0
2015-01-01 23:00:00    3.0
2015-01-02 00:00:00    3.0
2015-01-02 01:00:00    3.0
2015-01-02 02:00:00    3.0
2015-01-02 03:00:00    3.0
2015-01-02 04:00:00    4.0
2015-01-02 05:00:00    5.0
""", ts)


def test_staircase_formula(engine, tsh):
    dr = pd.date_range(
        start=datetime(2015, 1, 1),
        end=datetime(2015, 1, 2),
        freq='H'
    )
    for insertion_date in dr:
        ts1 = genserie(start=insertion_date, freq='H', repeat=6)
        ts2 = ts1 + 1
        tsh.update(
            engine, ts1, 'rep1', 'test',
            insertion_date=pd.Timestamp(insertion_date, tz='UTC')
        )
        tsh.update(
            engine, ts2, 'rep2', 'test',
            insertion_date=pd.Timestamp(insertion_date, tz='UTC')
        )

    ingest_formulas(tsh, engine, DATADIR / 'formula_definitions.csv')

    d = timedelta(hours=3)
    start = datetime(2015, 1, 2)

    r1 = tsh.staircase(engine, 'rep1', delta=d, from_value_date=start)
    r2 = tsh.staircase(engine, 'rep2', delta=d, from_value_date=start)
    f1 = tsh.staircase(engine, 'excelalias4', delta=d, from_value_date=start)
    f2 = tsh.staircase(engine, 'excelaliassum1', delta=d, from_value_date=start)

    assert_df("""
2015-01-02 00:00:00    3.0
2015-01-02 01:00:00    3.0
2015-01-02 02:00:00    3.0
2015-01-02 03:00:00    3.0
2015-01-02 04:00:00    4.0
2015-01-02 05:00:00    5.0
""", r1)

    assert_df("""
2015-01-02 00:00:00    4.0
2015-01-02 01:00:00    4.0
2015-01-02 02:00:00    4.0
2015-01-02 03:00:00    4.0
2015-01-02 04:00:00    5.0
2015-01-02 05:00:00    6.0
""", r2)

    assert_df("""
2015-01-02 00:00:00    24.0
2015-01-02 01:00:00    24.0
2015-01-02 02:00:00    24.0
2015-01-02 03:00:00    24.0
2015-01-02 04:00:00    32.0
2015-01-02 05:00:00    40.0
""", f1)

    assert_df("""
2015-01-02 00:00:00    47.0
2015-01-02 01:00:00    47.0
2015-01-02 02:00:00    47.0
2015-01-02 03:00:00    47.0
2015-01-02 04:00:00    59.0
2015-01-02 05:00:00    71.0
""", f2)


    r1, _, _ = tsh.get_many(engine, 'rep1', delta=d, from_value_date=start)
    r2, _, _ = tsh.get_many(engine, 'rep2', delta=d, from_value_date=start)
    f1, _, _ = tsh.get_many(engine, 'excelalias4', delta=d, from_value_date=start)
    f2, _, _ = tsh.get_many(engine, 'excelaliassum1', delta=d, from_value_date=start)

    assert_df("""
2015-01-02 00:00:00    3.0
2015-01-02 01:00:00    3.0
2015-01-02 02:00:00    3.0
2015-01-02 03:00:00    3.0
2015-01-02 04:00:00    4.0
2015-01-02 05:00:00    5.0
""", r1)

    assert_df("""
2015-01-02 00:00:00    4.0
2015-01-02 01:00:00    4.0
2015-01-02 02:00:00    4.0
2015-01-02 03:00:00    4.0
2015-01-02 04:00:00    5.0
2015-01-02 05:00:00    6.0
""", r2)

    assert_df("""
2015-01-02 00:00:00    24.0
2015-01-02 01:00:00    24.0
2015-01-02 02:00:00    24.0
2015-01-02 03:00:00    24.0
2015-01-02 04:00:00    32.0
2015-01-02 05:00:00    40.0
""", f1)

    assert_df("""
2015-01-02 00:00:00    47.0
2015-01-02 01:00:00    47.0
2015-01-02 02:00:00    47.0
2015-01-02 03:00:00    47.0
2015-01-02 04:00:00    59.0
2015-01-02 05:00:00    71.0
""", f2)


def test_sanitize_names(engine, tsh):
    saturn_name = 'name space(),[] & Cie'
    sanitized_name = tsh._sanitize(saturn_name)
    assert 'namespace&Cie' == sanitized_name
    tsh.update(engine, genserie(datetime(2015, 1, 1), 'D', 2), saturn_name, 'test')
    assert 2 == len(tsh.get(engine, 'namespace&Cie'))


def test_formula_metadata(engine, tsh):
    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2020, 1, 1), freq='D', periods=3)
    )

    tsh.update(
        engine,
        ts,
        'unsupervised',
        'Babar'
    )

    tsh.update(
        engine,
        ts,
        'handcrafted',
        'Celeste',
        manual=True
    )

    tsh.register_formula(
        engine,
        'combined',
        '(add (series "unsupervised") (series "handcrafted"))'
    )
    meta = tsh.metadata(
        engine,
        'combined'
    )
    assert meta == {
        'index_dtype': '|M8[ns]',
        'index_type': 'datetime64[ns, UTC]',
        'tzaware': True,
        'value_dtype': '<f8',
        'value_type': 'float64'
    }


def test_types():
    # prune the types registered from other modules/plugins
    # we want to only show the ones provided by the current package
    opnames = set(
        ('priority-origin',)
    )
    types = {
        name: ftype
        for name, ftype in json.loads(jsontypes()).items()
        if name in opnames
    }
    assert {
        'priority-origin': {'return': 'Series', 'serieslist': 'Series'}
    } == types
