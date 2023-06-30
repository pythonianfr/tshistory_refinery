import json

from rework.api import task
import rework.io as rio

from tshistory.util import objects
from tshistory_refinery import (
    cache,
    helper,
    scrap
)


# cache

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


# scrap

@task(domain='scrapers',
      inputs=(
          rio.string('seriesname'),
          rio.string('identifier'),
          rio.string('initialdate', required=True),
          rio.number('reset', required=True)
      )
)
def fetch_history(task):
    tsa = helper.apimaker(
        {
            'db': {'uri': str(task.engine.url)},
            'sources': {}
        }
    )
    with task.capturelogs(std=True):
        inputs = task.input
        if 'seriesname' not in inputs:
            inputs['seriesname'] = None
        if 'identifier' not in inputs:
            inputs['identifier'] = None

        scraper = scrap.Scrapers()
        for scrapr in objects('scrapers'):
            scraper.merge(scrapr)

        scraper.fetch_history(
            tsa,
            inputs['seriesname'],
            inputs['identifier'],
            inputs['initialdate'],
            inputs['reset']
        )


@task(domain='scrapers',
      inputs=(
          rio.string('seriesname'),
          rio.string('identifier'),
          rio.string('fromdate'),
          rio.string('todate')
      ),
      outputs=(
          rio.string('func'),
          rio.string('ts_status')
      )
)
def refresh(task):
    tsa = helper.apimaker(
        {
            'db': {'uri': str(task.engine.url)},
            'sources': {}
        }
    )
    with task.capturelogs(std=True):
        inputs = task.input
        if 'fromdate' not in inputs:
            inputs['fromdate'] = None
        if 'todate' not in inputs:
            inputs['todate'] = None
        if 'seriesname' not in inputs:
            inputs['seriesname'] = None
        if 'identifier' not in inputs:
            inputs['identifier'] = None

        scraper = scrap.Scrapers()
        for scrapr in objects('scrapers'):
            scraper.merge(scrapr)

        func_str = scraper.find_func_str(
            identifier=inputs['identifier'],
            seriesname=inputs['seriesname']
        )

        try:
            # the scraping part may fail
            status = scraper.refresh(
                tsa,
                inputs['seriesname'],
                inputs['identifier'],
                inputs['fromdate'],
                inputs['todate']
            )
        except:
            task.save_output(
                {
                    'func' : func_str,
                    'ts_status': json.dumps({
                        inputs['seriesname']: 'failed'
                    })
                }
            )
            raise

        task.save_output(
            {
                'func' : func_str,
                'ts_status': json.dumps(status)
            }
        )
