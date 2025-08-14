"""test index migrations for tshistory_refinery with hypothesis"""

from hypothesis import given, settings, strategies as st

from tshistory import dbdiag
from tshistory.migrate import do_fix_indexes
from tshistory.testutil import create_index_issues
from tshistory_refinery.helper import EXPECTED_INDEXES


@given(
    to_duplicate_idx=st.sets(st.integers(0, 4), max_size=5),
    to_drop_idx=st.sets(st.integers(0, 4), max_size=5),
    to_misname_idx=st.sets(st.integers(0, 4), max_size=5),
)
@settings(max_examples=20, deadline=10000)
def test_migration_fix_refinery_indexes(engine, to_duplicate_idx, to_drop_idx, to_misname_idx):
    """test that migration handles any combination of refinery index issues"""
    
    # skip if the sets overlap (can't duplicate and drop the same index)
    if to_duplicate_idx & to_drop_idx:
        return
    
    # get expected indexes for refinery
    refinery_indexes = {
        k: (v[0].format(ns='tsh'), v[1])
        for k, v in EXPECTED_INDEXES.items()
    }
    
    # convert indices to actual index tuples
    all_indexes = list(refinery_indexes.keys())
    to_duplicate = [all_indexes[i] for i in to_duplicate_idx]
    to_drop = [all_indexes[i] for i in to_drop_idx]
    to_misname = [all_indexes[i] for i in to_misname_idx if i not in to_drop_idx]
    
    # create the issues
    create_index_issues(engine, 'tsh', to_duplicate, to_drop, to_misname, refinery_indexes)
    
    # check issues using refinery indexes specifically
    actual = dbdiag.get_actual_indexes(engine, 'tsh')
    issues = dbdiag.find_issues(refinery_indexes, actual)
    issues_report = dbdiag.format_report(issues)
    
    # should have issues if we created any
    if to_duplicate or to_drop or to_misname:
        assert '✓ All indexes are correct!' not in issues_report
    
    # run the migration with refinery indexes
    do_fix_indexes(engine, 'tsh', interactive=False, indexes=refinery_indexes)
    
    # property 1: all issues should be fixed
    actual = dbdiag.get_actual_indexes(engine, 'tsh')
    issues = dbdiag.find_issues(refinery_indexes, actual)
    fixed_report = dbdiag.format_report(issues)
    assert '✓ All indexes are correct!' in fixed_report
    
    # property 2: migration should be idempotent
    do_fix_indexes(engine, 'tsh', interactive=False, indexes=refinery_indexes)
    actual = dbdiag.get_actual_indexes(engine, 'tsh')
    issues = dbdiag.find_issues(refinery_indexes, actual)
    idempotent_report = dbdiag.format_report(issues)
    assert idempotent_report == fixed_report
    
    # property 3: all expected indexes should exist with correct names
    actual = dbdiag.get_actual_indexes(engine, 'tsh')
    
    for (table, columns), (expected_name, _) in refinery_indexes.items():
        matching = [idx for idx in actual
                   if idx['table'] == table and idx['columns'] == columns]
        assert len(matching) == 1
        assert matching[0]['name'] == expected_name