from functools import partial

import pandas as pd
import pytest
from tshistory.testutil import assert_df

from tshistory_refinery.scrap import Scrap, Scrapers


@pytest.fixture(scope='session')
def cleanup_series(tsa):
    seriesinfos = list(tsa.catalog().values())[0]
    for seriesinfo in seriesinfos:
        seriesname = seriesinfo[0]
        tsa.delete(seriesname)


# We are testing 2 situations:
# - function with parameters and a timeseries in output /
# - function without any paramaters and with a dataframe in output

def get_timeseries_from_website1(parameter1, fromdate, todate):
    ts = pd.Series([1, 2, 3])
    ts.index = pd.date_range(
        start=pd.to_datetime(fromdate),
        end=pd.to_datetime(todate),
        periods=len(ts.values)
    )
    return ts, {'hello': 'world'}


def get_dataframe_from_website2():
    df = pd.DataFrame(
        data={
            'col1': [1, 2, 3],
            'col2': [3, 4, 5],
            'col3': [4, 5, 6]
        },
        index=pd.date_range(
            start=pd.Timestamp('2022-07-01'),
            end=pd.to_datetime('2022-07-03')
        )
    )
    return df, {'col1': {'hello': 'world'}}


SCRAPERS_test = Scrapers(
    Scrap(
        names='series.from.website1',
        func=partial(get_timeseries_from_website1, 'kikou'),
        schedrule='0 0 * * * *',
        fromdate='(date "2022-1-1")',
        todate='(date "2022-1-3")',
        initialdate='(date "2001-1-1")'
    ),
    Scrap(
        names={
            'series.1.from.website2': 'col1',
            'series.2.from.website2': 'col2',
            'series.3.from.website2': 'col3'
        },
        func=get_dataframe_from_website2,
        schedrule='0 0 13,15 * * *',
        precious=True
    )
)


# we test class methods here
def test_find_scrap_by_hash():
    hash = list(SCRAPERS_test.scrapers.values())[0].hash()
    scrap = SCRAPERS_test.find_by_hash(hash)
    assert scrap.names == 'series.from.website1'


def test_check_seriesname():
    seriesname_ok = 'series.1.from.website2'
    assert SCRAPERS_test.seriesname_exists(seriesname_ok)
    seriesname_notok = 'babar'
    assert not SCRAPERS_test.seriesname_exists(seriesname_notok)


def test_get_inventory():
    assert SCRAPERS_test.seriesnames_inventory() == {
        'series.1.from.website2': '936d1bf7198d582cee8e30bbb2450b4da88e9cd8',
        'series.2.from.website2': '936d1bf7198d582cee8e30bbb2450b4da88e9cd8',
        'series.3.from.website2': '936d1bf7198d582cee8e30bbb2450b4da88e9cd8',
        'series.from.website1': 'c2b9a19a28fd39a30d252a24185baa774dc95b88'
    }


def test_update_timeseries(engine, tsa):
    hash = list(SCRAPERS_test.scrapers.values())[0].hash()
    scrap = SCRAPERS_test.find_by_hash(hash)
    scrap.update(tsa)
    assert_df("""
2022-01-01    1.0
2022-01-02    2.0
2022-01-03    3.0
""", tsa.get('series.from.website1'))


def test_update_dataframe(engine, tsa):
    hash = list(SCRAPERS_test.scrapers.values())[1].hash()
    scrap = SCRAPERS_test.find_by_hash(hash)
    scrap.update(tsa)
    assert_df("""
2022-07-01    4.0
2022-07-02    5.0
2022-07-03    6.0
""", tsa.get('series.3.from.website2'))
