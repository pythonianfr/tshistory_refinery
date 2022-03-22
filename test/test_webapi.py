import io
from pathlib import Path

import pandas as pd

from tshistory.testutil import assert_df, genserie
from tshistory_formula.registry import func, metadata

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


def test_policies(client):
    res = client.get('/policies')
    assert res.json == []
    assert res.json() == []

