from datetime import timedelta

import pandas as pd
from psyl import lisp
from sqlhelp import select

from tshistory.util import (
    compatible_date,
    patch,
    threadpool,
    tx
)
from tshistory.tsio import timeseries as basets
from tshistory_xl.tsio import timeseries as xlts

from tshistory_refinery import cache
from tshistory_refinery import api  # trigger registration


class name_stopper:
    __slots__ = 'cn', 'tsh', 'names'

    def __init__(self, cn, tsh, stopnames=()):
        self.cn = cn
        self.tsh = tsh
        self.names = stopnames

    def __contains__(self, name):
        if name in self.names:
            return True
        tsh = self.tsh
        if tsh.type(self.cn, name) == 'formula':
            if tsh.cache.exists(self.cn, name):
                return True
        return False


class timeseries(xlts):

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.cache = basets(namespace='{}-cache'.format(self.namespace))

    def _expanded_formula(self, cn, formula, stopnames=(), qargs=None):
        # stopnames dynamic lookup for series that have a cache
        # (we won't expand them since we can litterally stop at them)
        if not qargs or (not qargs.get('live') and not qargs.get('nocache')):
            stopnames = name_stopper(cn, self, stopnames)
        return super()._expanded_formula(
            cn, formula,
            stopnames=stopnames,
            qargs=qargs
        )

    @tx
    def get(self, cn, name, nocache=False, live=False, **kw):
        if self.type(cn, name) != 'formula':
            return super().get(cn, name, **kw)

        if nocache or not self.cache.exists(cn, name):
            return super().get(cn, name, nocache=nocache, live=live, **kw)

        ready = cache.series_policy_ready(cn, name, namespace=self.namespace)
        if not ready:
            return super().get(cn, name, nocache=nocache, live=live, **kw)
        print(f'GET DATA FROM CACHE FOR {name}')
        idates = self.cache.insertion_dates(
            cn, name,
            from_insertion_date=kw.get('revision_date')
        )
        cached = self.cache.get(cn, name, **kw)
        if len(cached) and live:
            tzaware = self.metadata(cn, name)['tzaware']
            # save for later use
            fvd = kw.pop('from_value_date', None)
            tvd = kw.pop('to_value_date', None)
            if fvd:
                fvd = compatible_date(tzaware, fvd)
            if tvd:
                tvd = compatible_date(tzaware, tvd)

            # now, compute boundaries for the live query
            # using the cache policy
            policy = cache.series_policy(cn, name, namespace=self.namespace)
            now = (
                kw.get('revision_date') or
                (idates and idates[-1]) or
                pd.Timestamp.utcnow()
            )
            # we use the look before date span from the cache policy
            kw['from_value_date'] = cache.eval_moment(
                policy['look_before'],
                {'now': now}
            )
            # and the max of the look after / query for the right boundary
            la = cache.eval_moment(
                policy['look_after'],
                {'now': now}
            )
            if tvd:
                # let's honor the to_value_date part of the query of provided
                # for the live part
                la = compatible_date(tzaware, la)
                la = max(tvd, la)
            kw['to_value_date'] = la
            livets = super().get(cn, name, nocache=nocache, live=live, **kw)
            return patch(cached, livets).loc[fvd:tvd]

        return cached


    @tx
    def insertion_dates(self, cn, name,
                        from_insertion_date=None,
                        to_insertion_date=None,
                        nocache=False,
                        **kw):
        if self.type(cn, name) != 'formula':
            return super().insertion_dates(
                cn, name,
                from_insertion_date=from_insertion_date,
                to_insertion_date=to_insertion_date,
                **kw
            )


        if not nocache and self.cache.exists(cn, name):
            ready = cache.series_policy_ready(cn, name, namespace=self.namespace)
            if ready is not None and ready:
                return self.cache.insertion_dates(
                    cn, name,
                    from_insertion_date=from_insertion_date,
                    to_insertion_date=to_insertion_date,
                    **kw
                )

        return super().insertion_dates(
            cn, name,
            from_insertion_date=from_insertion_date,
            to_insertion_date=to_insertion_date,
            **kw
        )

    @tx
    def history(self, cn, name,
                nocache=False,
                **kw):
        if self.type(cn, name) != 'formula':
            return super().history(
                cn, name, **kw
            )

        if not nocache and self.cache.exists(cn, name):
            ready = cache.series_policy_ready(cn, name, namespace=self.namespace)
            if ready is not None and ready:
                return self.cache.history(cn, name, **kw)

        return super().history(
            cn, name, **kw
        )

    @tx
    def rename(self, cn, oldname, newname):
        if self.type(cn, oldname) == 'formula':
            self.cache.rename(cn, oldname, newname)

        return super().rename(cn, oldname, newname)

    @tx
    def delete(self, cn, name):
        if self.type(cn, name) == 'formula':
            self.cache.delete(cn, name)

        return super().delete(cn, name)

    @tx
    def invalidate_cache(self, cn, name):
        self.cache.delete(cn, name)

    @tx
    def unset_cache_policy(self, cn, name):
        cache.unset_policy(cn, name, namespace=self.namespace)
        self.cache.delete(cn, name)

    @tx
    def cacheable_formulas(self, cn, unlinked=True):
        q = select(
            'f.name'
        ).table(
            f'"{self.namespace}".formula as f'
        )
        if unlinked:
            q.where(
                f'not exists '
                f' (select 1 '
                f'  from "{self.namespace}".cache_policy_series as p'
                f'  where p.series_id = f.id'
                f')'
            )

        return [
            name for name, in q.do(cn).fetchall()
        ]

    @tx
    def register_formula(self, cn, name, formula, reject_unknown=True):
        prevch = self.content_hash(cn, name)
        super().register_formula(
            cn, name, formula,
            reject_unknown=reject_unknown
        )
        if prevch != self.content_hash(cn, name):
            self.invalidate_cache(cn, name)
            for name in self.dependants(cn, name):
                self.invalidate_cache(cn, name)
