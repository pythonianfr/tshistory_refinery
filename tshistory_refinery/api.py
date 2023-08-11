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

    cache.new_policy(
        self.engine,
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

    cache.edit_policy(
        self.engine,
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
    cache.delete_policy(
        self.engine,
        name,
        namespace=self.tsh.namespace
    )


@extend(mainsource)
def set_cache_policy(
        self,
        policyname: str,
        seriesnames: List[str]) -> NONETYPE:
    """Associate series with a cache policy."""
    for name in seriesnames:
        cache.set_policy(
            self.engine,
            policyname,
            name,
            namespace=self.tsh.namespace
        )


@extend(mainsource)
def unset_cache_policy(self, seriesnames: List[str]) -> NONETYPE:
    """Dis-associate series from a cache policy."""
    for name in seriesnames:
        cache.unset_policy(
            self.engine,
            name,
            namespace=self.tsh.namespace
        )
        self.tsh.cache.delete(self.engine, name)


@extend(mainsource)
def cache_free_series(self, allsources: bool=True):
    """List the series that are available for association with a cache
    policy."""
    freeset = {
        self._instancename(): self.tsh.cacheable_formulas(self.engine)
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
    return [
        name for name, in self.engine.execute(
            'select name from tsh.cache_policy'
        ).fetchall()
    ]


@extend(mainsource)
def cache_policy_series(self, policyname: str):
    """Return the list of series associated with a cache policy."""
    return cache.policy_series(self.engine, policyname)


@extend(mainsource)
def cache_series_policy(self, seriesname: str):
    """Return the cache policy of a series."""
    return cache.series_policy(self.engine, seriesname)


@extend(mainsource)
def has_cache(self, seriesname: str):
    """Predicate to verify is a series formula has a cache."""
    return self.tsh.cache.exists(self.engine, seriesname)


@extend(mainsource)
def delete_cache(self, seriesname: str):
    """Purge the cache of a formula."""
    return self.tsh.invalidate_cache(self.engine, seriesname)


@extend(mainsource)
def refresh_series_policy_now(self, policyname: str):
    return rapi.schedule(
        self.engine,
        'refresh_formula_cache_now',
        domain='timeseries',
        inputdata={'policy': policyname}
    ).tid
