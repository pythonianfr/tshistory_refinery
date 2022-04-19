from functools import cmp_to_key

from rework.api import task
from rework.io import string

from tshistory_refinery import (
    cache,
    helper
)


@task(
    domain='timeseries',
    inputs=(string('policy', required=True),)
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

        cmp = helper.comparator(tsh, engine)
        names.sort(key=cmp_to_key(cmp))

        for name in names:
            print('refresh', name)
            with engine.begin() as cn:
                if tsh.live_content_hash(cn, name) != tsh.content_hash(cn, name):
                    tsh.invalidate_cache(cn, name)

            cache.refresh(engine, tsa, name)
