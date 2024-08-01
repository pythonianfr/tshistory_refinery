import pandas as pd
from sqlhelp import select

from tshistory.util import (
    compatible_date,
    patch,
    tx
)
from tshistory.tsio import timeseries as basets
from tshistory_xl.tsio import timeseries as xlts

from tshistory_refinery import cache
from tshistory_refinery import api  # trigger registration  # noqa: F401


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


def utcnow():
    return pd.Timestamp.utcnow()


def infer_freq(idates):
    assert len(idates) > 1
    index = pd.Series(idates)
    deltas = (index - index.shift(1)).dropna()
    freq = deltas.median()

    conform_intervals = sum(deltas == freq)
    return freq, conform_intervals / len(deltas)



class timeseries(xlts):
    index = 3

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.cache = basets(namespace=f'{self.namespace}-cache')

    def _expanded_formula(self, cn, formula, stopnames=(), level=-1,
                          display=True, remote=True, qargs=None):
        # stopnames dynamic lookup for series that have a cache
        # (we won't expand them since we can litterally stop at them)
        if qargs is not None and (not qargs.get('live') and not qargs.get('nocache')):
            stopnames = name_stopper(cn, self, stopnames)
        return super()._expanded_formula(
            cn, formula,
            stopnames=stopnames,
            level=level,
            display=display,
            remote=remote,
            qargs=qargs
        )

    @tx
    def get(self, cn, name, nocache=False, live=False, **kw):
        if self.type(cn, name) != 'formula':
            return super().get(cn, name, **kw)

        if nocache or not self.cache.exists(cn, name):
            return super().get(cn, name, nocache=nocache, live=live, **kw)

        # there is a cache and we want hard to use it ...
        # what if it is stale or old or just initially building ?
        # we try an heuristics based on regularity of the cache insertion dates
        # -> if they are regular and we are missing at least two revisions
        # we do a live query ...

        # asking all cache idates is not a too expensive operation at
        # this point
        cacheidates = self.cache.insertion_dates(cn, name)
        if len(cacheidates) > 1:
            freq, _ = infer_freq(cacheidates)
            now = utcnow()
            lag = now - cacheidates[-1]
            live = lag / freq > 2

        cached = self.cache.get(cn, name, **kw)
        if len(cached):
            if live:
                return self._get_live(cn, name, cached, cacheidates, kw)

            return cached

        # cached is empty -- here we see if we are asked some old uncached
        # revision and serve it if available

        revdate = kw.get('revision_date')
        if revdate is None or revdate >= self.cache.first_insertion_date(cn, name):
            return cached

        return super().get(cn, name, nocache=nocache, live=live, **kw)

    def _get_live(self, cn, name, cached, idates, kw):
        tzaware = self.tzaware(cn, name)

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
            utcnow()
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
            # let's honor the to_value_date part of the query provided
            # for the live part
            la = compatible_date(tzaware, la)
            la = max(tvd, la)
        kw['to_value_date'] = la
        livets = super().get(cn, name, live=True, **kw)
        return patch(cached, livets).loc[fvd:tvd]

    @tx
    def insertion_dates(self, cn, name,
                        from_insertion_date=None,
                        to_insertion_date=None,
                        from_value_date=None,
                        to_value_date=None,
                        nocache=False,
                        **kw):
        if self.type(cn, name) != 'formula':
            return super().insertion_dates(
                cn, name,
                from_insertion_date=from_insertion_date,
                to_insertion_date=to_insertion_date,
                from_value_date=from_value_date,
                to_value_date=to_value_date,
                **kw
            )

        if not nocache and self.cache.exists(cn, name):
            idates = self.cache.insertion_dates(
                cn, name,
                from_insertion_date=from_insertion_date,
                to_insertion_date=to_insertion_date,
                from_value_date=from_value_date,
                to_value_date=to_value_date,
                **kw
            )
            # some casuistry to complete idates to the left
            # using the uncached formula
            if not idates:
                # no choice but to delegate to the non-cached world
                return super().insertion_dates(
                    cn, name,
                    from_insertion_date=from_insertion_date,
                    to_insertion_date=to_insertion_date,
                    from_value_date=from_value_date,
                    to_value_date=to_value_date,
                    nocache=True,
                    **kw
                )

            if from_insertion_date and from_insertion_date >= idates[0]:
                # nothing more to collect
                return idates

            # complete to the left (with help of the non-cached world)
            leftidates = super().insertion_dates(
                cn, name,
                from_insertion_date=from_insertion_date,
                to_insertion_date=idates[0],
                from_value_date=from_value_date,
                to_value_date=to_value_date,
                nocache=True,
                **kw
            )
            if leftidates:
                # avoid a duplicate
                return sorted(set(leftidates + idates))

            return idates

        # the nocache argument must be carried
        # upstream because it is perfectly possible
        # to hit another cached formula there
        # and we want to propagate this all the way up
        return super().insertion_dates(
            cn, name,
            from_insertion_date=from_insertion_date,
            to_insertion_date=to_insertion_date,
            from_value_date=from_value_date,
            to_value_date=to_value_date,
            # explained in the above comment
            nocache=nocache,
            **kw
        )

    @tx
    def rename(self, cn, oldname, newname, propagate=True):
        if self.type(cn, oldname) == 'formula':
            self.cache.rename(cn, oldname, newname, propagate=propagate)

        return super().rename(cn, oldname, newname, propagate=propagate)

    @tx
    def delete(self, cn, name):
        if self.type(cn, name) == 'formula':
            self.cache.delete(cn, name)

        return super().delete(cn, name)

    @tx
    def invalidate_cache(self, cn, name):
        if self.cache.exists(cn, name):
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
            f'"{self.namespace}".registry as f'
        ).where(
            'f.internal_metadata->\'formula\' is not null'
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
            for name in self.dependents(cn, name):
                self.invalidate_cache(cn, name)
