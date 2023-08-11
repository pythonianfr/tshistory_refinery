import warnings

from inireader import reader
from tshistory.api import timeseries


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
            return -1 if n1 < n2 else 0 if n1 == n2 else 1
        if not len(d1):
            return -1
        if not len(d2):
            return 1
        # general case
        assert not (n1 in d2 and n2 in d1)
        if n1 in d2:
            return -1
        elif n2 in d1:
            return 1
        # no dependency
        return -1 if n1 < n2 else 0 if n1 == n2 else 1

    return compare


def reduce_frequency(tempo, idates):
    assert len(tempo)
    assert len(idates)
    new_tempo = []
    for cdate in tempo:
        if len([idate for idate in idates if idate <= cdate]):
            new_tempo.append(cdate)
            idates = [idate for idate in idates if idate > cdate]

    return new_tempo
