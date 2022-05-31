import socket

from inireader import reader
from sqlalchemy import create_engine
from tshistory.api import timeseries


NTHREAD = 16


def config():
    return reader('refinery.cfg')


def spawn_engine(dburi):
    return create_engine(dburi, pool_size=NTHREAD)


def readsources(config):
    sources = []
    for _name, source in config['sources'].items():
        uri, ns = source.split(',')
        sources.append((uri.strip(), ns.strip()))
    return sources


def apimaker(config):
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
    """ produces a `cmp` function to order series by dependants """

    def compare(n1, n2):
        d1 = tsh.dependants(engine, n1)
        d2 = tsh.dependants(engine, n2)
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
