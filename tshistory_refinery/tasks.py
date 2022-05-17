from functools import cmp_to_key

from rework.api import task
import rework.io as rio

from tshistory_refinery import (
    cache,
    helper
)


@task(
    domain='timeseries',
    inputs=(
        rio.string('policy', required=True),
        rio.number('initial', required=True)
    )
)
def refresh_formula_cache(task):
    tsa = helper.apimaker(
        helper.config()
    )
    names = cache.policy_series(
        tsa.engine,
        task.input['policy'],
        namespace=tsa.tsh.namespace
    )

    with task.capturelogs(std=True):
        print(f'refreshing series: {names}')
        # sort series by dependency order
        # we want the leafs to be computed
        tsh = tsa.tsh
        engine = tsa.engine

        unames = set()
        # put the uncached serie at the end
        for name in names:
            if not tsh.cache.exists(engine, name):
                unames.add(name)

        names = [
            name for name in names
            if name not in unames
        ]

        cmp = helper.comparator(tsh, engine)
        names.sort(key=cmp_to_key(cmp))

        unames = list(unames)
        unames.sort(key=cmp_to_key(cmp))

        # first batch (potentially just a refresh if not an initial run)
        print('first batch')
        for name in names:
            print('refresh', name)
            with engine.begin() as cn:
                if tsh.live_content_hash(cn, name) != tsh.content_hash(cn, name):
                    tsh.invalidate_cache(cn, name)

            cache.refresh(engine, tsa, name, initial=task.input['initial'])

        # second batch (potentially re-filling invalidated caches)
        print('second batch')
        for name in unames:
            print('refresh', name)
            cache.refresh(engine, tsa, name, initial=task.input['initial'])

        cache.set_ready(engine, task.input['policy'])
