import io
from pathlib import Path

import pandas as pd

from tshistory.testutil import genserie
from tshistory_formula.registry import func, metadata
from tshistory_refinery import cache


DATADIR = Path(__file__).parent / 'data'


@func('cronos')
def cronos(uid: str, fromdate: pd.Timestamp, todate: pd.Timestamp) -> pd.Series:
    pass


@metadata('cronos')
def cronos_metadata(cn, tsh, tree):
    return {
        f'cronos:{tree[1]}': {
            'tzaware': True,
            'source': 'singularity-cronos',
            'index_type': 'datetime64[ns, UTC]',
            'value_type': 'float64',
            'index_dtype': '|M8[ns]',
            'value_dtype': '<f8'
        }
    }


def test_formula_form_base(engine, client, tsh):
    with engine.begin() as cn:
        cn.execute('delete from tsh.registry')

    ts = genserie('2019-1-1', 'D', 3)
    tsh.update(engine, ts, 'crude-a', 'Babar')

    user_file = DATADIR / 'goodformula.csv'
    # the user is pushing its own formulas
    response = client.post(
        '/updateformulas',
        upload_files=[
            ('new_formulas.csv',
             user_file.name,
             user_file.read_bytes(),
             'text/csv')
        ]
    )
    assert response.status_code == 200
    assert response.json == {
        'warnings': {},
        'errors': {
            'missing': [
                'crude-b',
                'crude-c',
                'gas-a',
                'gas-b',
                'gas-c'
            ]
        }
    }

    # really do it
    for name in ('crude-b', 'crude-b', 'crude-c', 'gas-a', 'gas-b', 'gas-c'):
        tsh.update(engine, ts, name, 'Babar')

    posted = client.post(
        '/updateformulas',
        {'reallydoit': True},
        upload_files=[
            ('new_formulas.csv',
             user_file.name,
             user_file.read_bytes(),
             'text/csv')
        ]
    )
    # the user is downloading the current formulaes
    response = client.get('/downloadformulas')
    formula_inserted = pd.read_csv(user_file)
    formula_downloaded = pd.read_csv(io.StringIO(response.text))
    assert set(formula_inserted['text']) == set(formula_downloaded['text'])

    assert tsh.internal_metadata(engine, 'arith2')['tzaware'] is False

    # We reinsert the donwloaded formulaes and check that everything is kept in the process
    response = client.post(
        '/updateformulas',
        {'reallydoit': True},
        upload_files=[
            ('new_formulas.csv',
             'formulareinserted.csv',
             formula_downloaded.to_csv().encode(),
             'text/csv')
        ]
    )

    # confirmation
    response = client.get('/downloadformulas')

    # finaly
    formula_roundtripped = pd.read_csv(io.StringIO(response.text))
    assert formula_roundtripped.equals(formula_downloaded)

    # bogus formulas
    user_file = DATADIR / 'badformula.csv'
    formula_inserted = pd.read_csv(user_file)
    # the user is pushing its own formulaes
    response = client.post(
        '/updateformulas',
        upload_files=[
            ('new_formulas.csv',
             user_file.name,
             user_file.read_bytes(),
             'text/csv')
        ]
    )
    assert response.json == {
        'errors': {
            'syntax': ['syntax']
        },
        'warnings': {
            'existing': ['prio1']
        }
    }

    response = client.get('/formulas')
    assert response.status_code == 200

    meta = tsh.internal_metadata(engine, 'arith2')
    assert 'supervision_status' not in meta


def test_formula_form_metadata(engine, client, tsh, remote):
    with engine.begin() as cn:
        cn.execute('delete from tsh.registry')
        cn.execute('delete from remote.registry')
        cn.execute('delete from tsh.cache_policy')

    remote.register_formula(
        'remote-formula',
        '(cronos "yyy" (date "2020-1-1") (date "2021-1-1"))'
    )
    assert not tsh.exists(engine, 'remote-formula')
    assert not tsh.exists(engine, 'remote')

    user_file = DATADIR / 'remoteautoformula.csv'
    client.post(
        '/updateformulas',
        {'reallydoit': True},
        upload_files=[
            ('new_formulas.csv',
             user_file.name,
             user_file.read_bytes(),
             'text/csv')
        ]
    )
    # the user is downloading the current formulaes
    response = client.get('/downloadformulas')
    formula_inserted = pd.read_csv(user_file)
    formula_downloaded = pd.read_csv(io.StringIO(response.text))
    assert formula_inserted['name'].isin(formula_downloaded['name']).all()

    assert tsh.exists(engine, 'remote')
    assert tsh.internal_metadata(engine, 'remote') == {
        'contenthash': '3f27b94c07e759f9dfb331def431c2bc7278c27a',
        'formula': '(series "remote-formula")',
        'tzaware': True,
        'index_type': 'datetime64[ns, UTC]',
        'value_type': 'float64',
        'index_dtype': '|M8[ns]',
        'value_dtype': '<f8'
    }


def test_get_policies(client, engine):
    res = client.get('/policies')
    assert res.json == []

    cache.new_policy(
        engine,
        'pol-1',
        initial_revdate='(date "2020-1-1")',
        look_before='(shifted (today) #:days 15)',
        look_after='(shifted (today) #:days -10)',
        revdate_rule='0 1 * * *',
        schedule_rule='0 8-18 * * *',
    )

    res = client.get('/policies')
    assert res.json == [
        {'active': False,
         'initial_revdate': '(date "2020-1-1")',
         'look_after': '(shifted (today) #:days -10)',
         'look_before': '(shifted (today) #:days 15)',
         'name': 'pol-1',
         'ready': False,
         'revdate_rule': '0 1 * * *',
         'schedule_rule': '0 8-18 * * *'}
    ]


    ts = pd.Series(
        [1., 2., 3.],
        index=pd.date_range(
            pd.Timestamp('2022-1-1'),
            freq='D', periods=3
        )
    )

    from tshistory_formula.tsio import timeseries
    tsh = timeseries()

    tsh.update(engine, ts, 'for-pol-1-ground', 'Babar')
    tsh.register_formula(
        engine,
        'for-pol-1',
        '(series "for-pol-1-ground")'
    )

    cache.set_policy(
        engine,
        'pol-1',
        'for-pol-1',
        namespace=tsh.namespace
    )

    assert engine.execute(
        'select count(*) from tsh.cache_policy_series'
    ).scalar() == 1

    resp = client.delete('/delete-policy/pol-1')
    assert resp.status_code == 204

    assert engine.execute(
        'select count(*) from tsh.cache_policy_series'
    ).scalar() == 0
    assert engine.execute(
        'select count(*) from tsh.cache_policy'
    ).scalar() == 0


def test_create_policies(client, engine):
    res = client.get('/policies')
    assert res.json == []

    res = client.put_json('/create-policy', {
        'name': 'web-pol',
        'initial_revdate': '(date "2020-1-1")',
        'look_after': '(shifted (today) #:days -10)',
        'look_before': '(shifted (today) #:days 15)',
        'revdate_rule': '0 1 * * *',
        'schedule_rule': '0 8-18 * * *'
    })
    assert res.status_code == 201

    res = client.get('/policies')
    assert res.json == [
        {'active': False,
         'initial_revdate': '(date "2020-1-1")',
         'look_after': '(shifted (today) #:days -10)',
         'look_before': '(shifted (today) #:days 15)',
         'name': 'web-pol',
         'ready': False,
         'revdate_rule': '0 1 * * *',
         'schedule_rule': '0 8-18 * * *'
         }
    ]

    res = client.put_json('/create-policy', {
        'name': 'web-pol',
    })
    assert res.status_code == 400
    assert res.text == 'Missing fields'

    # with a bogus field
    res = client.put_json('/create-policy', {
        'name': 'web-pol-bis',
        'initial_revdate': 'BOGUS',
        'look_after': '(shifted (today) #:days -10)',
        'look_before': '(shifted (today) #:days 15)',
        'revdate_rule': '0 1 * * *',
        'schedule_rule': '0 8-18 * * *'
    })
    assert res.status_code == 400
    assert res.text == "Bad inputs for the cache policy: {'initial_revdate': 'BOGUS'}"


def test_cacheable_formulas(client, tsh, engine):
    with engine.begin() as cn:
        cn.execute('delete from tsh.registry')
        cn.execute('delete from tsh.cache_policy')

    res = client.get('/cacheable-formulas')
    assert res.json == []

    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(pd.Timestamp('2022-1-1', tz='UTC'), periods=3, freq='D')
    )

    tsh.update(
        engine,
        ts,
        'cacheable-base',
        'Babar'
    )

    tsh.register_formula(
        engine,
        'i-am-cacheable',
        '(series "cacheable-base")',
    )
    tsh.register_formula(
        engine,
        'i-am-broken',
        '(series "no-there")',
        reject_unknown=False
    )

    res = client.get('/cacheable-formulas')
    assert res.json == ['i-am-broken', 'i-am-cacheable']

    res = client.put_json('/create-policy', {
        'name': 'test-cacheable',
        'initial_revdate': '(date "2020-1-1")',
        'look_after': '(shifted (today) #:days -10)',
        'look_before': '(shifted (today) #:days 15)',
        'revdate_rule': '0 1 * * *',
        'schedule_rule': '0 8-18 * * *'
    })
    assert res.status_code == 201

    res = client.put_json('/set-series-policy', {
        'policyname': 'test-cacheable',
        'seriesname': 'i-am-cacheable'
    })
    assert res.status_code == 201

    res = client.get('/cacheable-formulas')
    assert res.json == ['i-am-broken']

    res = client.get('/policy-series/test-cacheable')
    assert res.json == ['i-am-cacheable']

    # unset cache policy
    res = client.put_json('/unset-series-policy', {
        'name': 'i-am-cacheable'
    })
    assert res.status_code == 204

    res = client.get('/cacheable-formulas')
    assert res.json == ['i-am-broken', 'i-am-cacheable']

    res = client.get('/policy-series/test-cacheable')
    assert res.json == []


def test_validate_policy(client):
    res = client.put_json('/validate-policy', {
        'initial_revdate': 'BOGUS',
        'look_after': '(shifted (today) #:days -10)',
        'look_before': '(shifted (today) #:days 15)',
        'revdate_rule': '0 1 * * *',
        'schedule_rule': '0 8-18 * * *'
    })
    assert res.status_code == 200
    assert res.json == {'initial_revdate': 'BOGUS'}


def test_schedule_policy(client, tsh, engine):
    with engine.begin() as cn:
        cn.execute('delete from tsh.registry')
        cn.execute('delete from tsh.cache_policy')

    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(pd.Timestamp('2022-1-1', tz='UTC'), periods=3, freq='D')
    )

    tsh.update(
        engine,
        ts,
        'schedulable-base',
        'Babar'
    )

    tsh.register_formula(
        engine,
        'schedule-me',
        '(series "schedulable-base")',
    )

    res = client.put_json('/create-policy', {
        'name': 'test-schedule',
        'initial_revdate': '(date "2020-1-1")',
        'look_after': '(shifted (today) #:days -10)',
        'look_before': '(shifted (today) #:days 15)',
        'revdate_rule': '0 1 * * *',
        'schedule_rule': '0 8-18 * * *'
    })
    assert res.status_code == 201

    res = client.get('/scheduled-policy', {
        'name': 'test-schedule'
    })
    assert not res.json

    res = client.put_json('/set-series-policy', {
        'policyname': 'test-schedule',
        'seriesname': 'schedule-me'
    })
    assert res.status_code == 201
    res = client.get('/scheduled-policy', {
        'name': 'test-schedule'
    })
    assert not res.json

    res = client.put_json('/schedule-policy', {
        'name': 'test-schedule'
    })
    assert res.status_code == 201

    res = client.get('/scheduled-policy', params={
        'name': 'test-schedule'
    })
    assert res.json

    res = client.put_json('/schedule-policy', {
        'name': 'test-schedule'
    })
    assert res.text == 'nothing changed'

    res = client.put_json('/unschedule-policy', {
        'name': 'test-schedule'
    })
    assert res.status_code == 201
    res = client.put_json('/unschedule-policy', {
        'name': 'test-schedule'
    })
    assert res.text  == 'nothing changed'

    res = client.get('/scheduled-policy', params={
        'name': 'test-schedule'
    })
    assert not res.json


def test_edit_policy(client, engine):
    with engine.begin() as cn:
        cn.execute('delete from tsh.registry')
        cn.execute('delete from tsh.cache_policy')

    res = client.put_json('/create-policy', {
        'name': 'test-edit',
        'initial_revdate': '(date "2010-1-1")',
        'look_after': '(shifted (today) #:days -10)',
        'look_before': '(shifted (today) #:days 15)',
        'revdate_rule': '0 1 * * *',
        'schedule_rule': '0 8-18 * * *'
    })
    assert res.status_code == 201

    res = client.put_json('/edit-policy', {
        'name': 'test-edit',
        'initial_revdate': '(date "2012-1-1")',
        'look_after': '(shifted (today) #:days -15)',
        'look_before': '(shifted (today) #:days 20)',
        'revdate_rule': '10 1 * * *',
        'schedule_rule': '10 8-18 * * *'
    })
    assert res.status_code == 200

    res = client.get('/policies')
    assert res.json == [
        {'active': False,
         'initial_revdate': '(date "2012-1-1")',
         'look_after': '(shifted (today) #:days -15)',
         'look_before': '(shifted (today) #:days 20)',
         'name': 'test-edit',
         'ready': False,
         'revdate_rule': '10 1 * * *',
         'schedule_rule': '10 8-18 * * *'
         }
    ]
