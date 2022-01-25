from tshistory.util import tx
from tshistory.tsio import timeseries as basets
from tshistory_xl.tsio import timeseries as xlts

from tshistory_refinery import cache


class timeseries(xlts):

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.cache = basets(namespace='{}-cache'.format(self.namespace))

    @tx
    def get(self, cn, name, **kw):
        if self.type(cn, name) != 'formula':
            return super().get(cn, name, **kw)

        ready = cache.ready(cn, name)
        if ready is not None and ready:
            return self.cache.get(cn, name, **kw)

        return super().get(cn, name, **kw)
