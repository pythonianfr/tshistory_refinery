import pytest
import io
import json

import pandas as pd
import numpy as np
from pathlib import Path

from flask import url_for
from bs4 import BeautifulSoup

from tshistory.util import tojson, fromjson

import tshistory_refinery.webapi

DATADIR = Path(__file__).parent / 'data'

def assert_df(expected, df):
    assert expected.strip() == df.to_string().strip()


def genserie(start, freq, repeat, initval=None, tz=None, name=None):
    if initval is None:
        values = range(repeat)
    else:
        values = [initval] * repeat
    return pd.Series(values,
                     name=name,
                     index=pd.date_range(start=start,
                                         freq=freq,
                                         periods=repeat,
                                         tz=tz))


def test_formula_form(engine, client, tsh):
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
