import io
from pathlib import Path

import pandas as pd

from tshistory.testutil import assert_df, genserie
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
        cn.execute('delete from tsh.formula')

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

    uploaded = client.post(
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

    assert tsh.metadata(engine, 'arith2')['tzaware'] == False

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

    meta = tsh.metadata(engine, 'arith2')
    assert 'supervision_status' not in meta



def test_formula_form_metadata(engine, client, tsh, remote):
    with engine.begin() as cn:
        cn.execute('delete from tsh.formula')
        cn.execute('delete from remote.formula')
        cn.execute('delete from tsh.cache_policy')

    remote.register_formula(
        'remote-formula',
        '(cronos "yyy" (date "2020-1-1") (date "2021-1-1"))'
    )
    assert not tsh.exists(engine, 'remote-formula')
    assert not tsh.exists(engine, 'remote')

    user_file = DATADIR / 'remoteautoformula.csv'
    uploaded = client.post(
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
    assert tsh.metadata(engine, 'remote') == {
        'tzaware': True,
        'index_type': 'datetime64[ns, UTC]',
        'value_type': 'float64',
        'index_dtype': '|M8[ns]',
        'value_dtype': '<f8'
    }


def test_policies(client, engine):
    res = client.get('/policies')
    assert res.json == []

    cache.new_policy(
        engine,
        'pol-1',
        initial_revdate='(date "2020-1-1")',
        from_date='(date "2010-1-1")',
        look_before='(shifted (today) #:days 15)',
        look_after='(shifted (today) #:days -10)',
        revdate_rule='0 1 * * *',
        schedule_rule='0 8-18 * * *',
    )

    res = client.get('/policies')
    assert res.json == [
        {'from_date': '(date "2010-1-1")',
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
