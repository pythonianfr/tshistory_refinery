import io
from pathlib import Path

import pandas as pd

from tshistory.testutil import assert_df, genserie
from tshistory_formula.registry import func

DATADIR = Path(__file__).parent / 'data'


def test_formula_form(engine, client, tsh):
    @func('cronos')
    def cronos(uid: str, fromdate: pd.Timestamp, todate: pd.Timestamp) -> pd.Series:
        pass

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
