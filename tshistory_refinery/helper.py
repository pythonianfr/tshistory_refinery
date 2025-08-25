import warnings

from inireader import reader
from tshistory.api import timeseries


# Expected indexes for tshistory_refinery cache tables
# Format: (table, columns) -> (index_name_template, index_type)
# Use {ns} as placeholder for namespace
EXPECTED_INDEXES = {
    ('cache_policy_sched', ('prepared_task_id',)): ('{ns}_cache_policy_sched_prepared_task_id_idx', 'btree'),
    ('cache_policy_series', ('cache_policy_id',)): ('{ns}_cache_policy_series_cache_policy_id_idx', 'btree'),
}


def config():
    return reader('refinery.cfg')


def readsources(config):
    sources = {}
    for name, source in config['sources'].items():
        uri, ns = source.split(',')
        sources[name] = (uri.strip(), ns.strip())
    return sources


def apimaker(config):
    warnings.warn(
        'The `apimaker` function is deprecated. '
        'You mayt want to move your source definitions from `refinery.cfg` '
        'and put them into `tshistory.cfg`. Then, using tshistory.api.timeseries '
        'will just work.',
        DeprecationWarning
    )
    from tshistory_refinery.tsio import timeseries as tshclass
    dburi = config['db']['uri']
    sources = readsources(config)

    return timeseries(
        dburi,
        handler=tshclass,
        sources=sources
    )


# topological sort of formulas

def comparator(tsh, engine):
    """ produces a `cmp` function to order series by dependents """

    def compare(n1, n2):
        d1 = tsh.dependents(engine, n1)
        d2 = tsh.dependents(engine, n2)
        # base case: if any has no dep we are done
        if not len(d1) and not len(d2):
            return 0
        if not len(d1):
            return 1
        if not len(d2):
            return -1
        # general case
        assert not (n1 in d2 and n2 in d1)
        if n1 in d2:
            return 1
        elif n2 in d1:
            return -1
        # no dependency
        return 0

    return compare


def reduce_frequency(tempo, idates):
    if not len(tempo):
        return tempo

    assert len(idates)
    new_tempo = []
    for cdate in tempo:
        if len([idate for idate in idates if idate <= cdate]):
            new_tempo.append(cdate)
            idates = [idate for idate in idates if idate > cdate]

    return new_tempo
