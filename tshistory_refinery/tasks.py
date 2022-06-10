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
    policy = task.input['policy']
    initial = task.input['initial']

    with task.capturelogs(std=True):
        cache.refresh_policy(tsa, policy, initial)


@task(
    domain='timeseries',
    inputs=(
        rio.string('policy', required=True),
    )
)
def refresh_formula_cache_now(task):
    tsa = helper.apimaker(
        helper.config()
    )
    policy = task.input['policy']

    with task.capturelogs(std=True):
        cache.refresh_policy_now(tsa, policy)
