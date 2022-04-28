from typing import List

from tshistory.util import extend
from tshistory.api import mainsource

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

    cache.new_policy(
        self.engine,
        name,
        initial_revdate,
        look_before,
        look_after,
        revdate_rule,
        schedule_rule
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

    cache.edit_policy(
        self.engine,
        name,
        initial_revdate,
        look_before,
        look_after,
        revdate_rule,
        schedule_rule
    )


@extend(mainsource)
def delete_cache_policy(self, name: str) -> NONETYPE:

    cache.delete_policy(
        self.engine,
        name
    )


@extend(mainsource)
def set_cache_policy(
        self,
        policyname: str,
        seriesnames: List[str]) -> NONETYPE:

    for name in seriesnames:
        cache.set_policy(
            self.engine,
            policyname,
            name
        )


@extend(mainsource)
def unset_cache_policy(self, seriesnames: List[str]) -> NONETYPE:

    for name in seriesnames:
        cache.unset_policy(self.engine, name)


@extend(mainsource)
def cache_free_series(self):
    return self.tsh.cacheable_formulas(self.engine)


@extend(mainsource)
def cache_policies(self):
    return [
        name for name, in self.engine.execute(
            'select name from tsh.cache_policy'
        ).fetchall()
    ]


@extend(mainsource)
def cache_policy_series(self, policyname: str):
    return cache.policy_series(self.engine, policyname)
