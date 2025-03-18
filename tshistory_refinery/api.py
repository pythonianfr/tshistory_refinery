from typing import List

from rework import api as rapi
from tshistory.util import (
    extend,
    threadpool
)
from tshistory.api import (
    altsources,
    mainsource
)

from tshistory_refinery import cache


NONETYPE = type(None)


@extend(mainsource)
def new_cache_policy(
        self,
        name: str,
        initial_revdate: str,
        look_before: str,
        look_after: str,
        revdate_rule: str,
        schedule_rule: str) -> NONETYPE:
    """Create a cache policy."""

    with self.engine.begin() as cn:
        cache.new_policy(
            cn,
            name,
            initial_revdate,
            look_before,
            look_after,
            revdate_rule,
            schedule_rule,
            namespace=self.tsh.namespace
        )


@extend(mainsource)
def edit_cache_policy(
        self,
        name: str,
        initial_revdate: str,
        look_before: str,
        look_after: str,
        revdate_rule: str,
        schedule_rule: str) -> NONETYPE:
    """Modify an existing cache policy (by name)."""

    with self.engine.begin() as cn:
        cache.edit_policy(
            cn,
            name,
            initial_revdate,
            look_before,
            look_after,
            revdate_rule,
            schedule_rule,
            namespace=self.tsh.namespace
        )


@extend(mainsource)
def delete_cache_policy(self, name: str) -> NONETYPE:
    """Delete a cache policy (by name)."""
    with self.engine.begin() as cn:
        cache.delete_policy(
            cn,
            name,
            namespace=self.tsh.namespace
        )


@extend(mainsource)
def set_cache_policy(
        self,
        policyname: str,
        seriesnames: List[str]) -> NONETYPE:
    """Associate series with a cache policy."""
    with self.engine.begin() as cn:
        for name in seriesnames:
            cache.set_policy(
                cn,
                policyname,
                name,
                namespace=self.tsh.namespace
            )


@extend(mainsource)
def unset_cache_policy(self, seriesnames: List[str]) -> NONETYPE:
    """Dis-associate series from a cache policy."""
    with self.engine.begin() as cn:
        for name in seriesnames:
            cache.unset_policy(
                cn,
                name,
                namespace=self.tsh.namespace
            )
            self.tsh.cache.delete(cn, name)


@extend(mainsource)
def cache_free_series(self, allsources: bool=True):
    """List the series that are available for association with a cache
    policy."""
    with self.engine.begin() as cn:
        freeset = {
            self._instancename(): self.tsh.cacheable_formulas(cn)
        }
    freeset.update(self.othersources.cache_free_series(False))
    return freeset


@extend(altsources)
def cache_free_series(self, allsources=True):  # noqa: F811
    freeset = []
    pool = threadpool(len(self.sources))
    def getfree(source):
        try:
            freeset.append(
                source.tsa.cache_free_series(allsources)
            )
        except:
            import traceback as tb; tb.print_exc()
            print(f'source {source} temporarily unavailable')

    pool(getfree, [(s,) for s in self.sources])
    all = {}
    for c in freeset:
        all.update(c)
    return all


@extend(mainsource)
def cache_policies(self):
    """Return a list of cache policies names."""
    with self.engine.begin() as cn:
        return [
            name for name, in cn.execute(
                'select name from tsh.cache_policy'
            ).fetchall()
        ]


@extend(mainsource)
def cache_policy_series(self, policyname: str):
    """Return the list of series associated with a cache policy."""
    with self.engine.begin() as cn:
        return cache.policy_series(cn, policyname)


@extend(mainsource)
def cache_series_policy(self, seriesname: str):
    """Return the cache policy of a series."""
    with self.engine.begin() as cn:
        return cache.series_policy(cn, seriesname)


@extend(mainsource)
def has_cache(self, seriesname: str):
    """Predicate to verify is a series formula has a cache."""
    with self.engine.begin() as cn:
        return self.tsh.cache.exists(cn, seriesname)


@extend(mainsource)
def delete_cache(self, seriesname: str):
    """Purge the cache of a formula."""
    with self.engine.begin() as cn:
        return self.tsh.invalidate_cache(cn, seriesname)


@extend(mainsource)
def refresh_series_policy_now(self, policyname: str):
    with self.engine.begin() as cn:
        return rapi.schedule(
            cn,
            'refresh_formula_cache_now',
            domain='timeseries',
            inputdata={'policy': policyname}
        ).tid
