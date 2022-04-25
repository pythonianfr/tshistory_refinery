from tshistory.util import extend
from tshistory.api import mainsource

from tshistory_refinery import cache


NONETYPE = type(None)


@extend(mainsource)
def new_cache_policy(
        self,
        name: str,
        initial_revdate: str,
        from_date: str,
        look_before: str,
        look_after: str,
        revdate_rule: str,
        schedule_rule: str) -> NONETYPE:

    cache.new_policy(
        self.engine,
        name,
        initial_revdate,
        from_date,
        look_before,
        look_after,
        revdate_rule,
        schedule_rule
    )

