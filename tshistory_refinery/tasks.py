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
        task.inputs['policy'],
        namespace=tsa.tsh.namespace
    )

    for name in names:
        cache.refresh(tsa.engine, tsa, name)
