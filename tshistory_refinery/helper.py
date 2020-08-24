import socket

from inireader import reader
from sqlalchemy import create_engine
from tshistory.api import timeseries

from tshistory_refinery.tsio import timeseries as tshclass


NTHREAD = 16


def config():
    return reader('refinery.ini')


def spawn_engine(dburi):
    return create_engine(dburi, pool_size=NTHREAD)


def readsources(config):
    sources = []
    for _name, source in config['sources'].items():
        uri, ns = source.split(',')
        sources.append((uri.strip(), ns.strip()))
    return sources


def apimaker(config):
    dburi = config['db']['uri']
    sources = readsources(config)

    return timeseries(
        dburi,
        handler=tshclass,
        sources=sources
    )


def host():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 1))
    return s.getsockname()[0]

