import json

from rework.api import task
import rework.io as rio

from tshistory.api import timeseries
from tshistory.util import (
    objects,
    replicate_series,
    replicate_basket,
)
from tshistory_refinery import (
    cache,
    scrap
)


# cache

@task(
    domain='timeseries',
    inputs=(
        rio.string('policy', required=True),
    )
)
def refresh_formula_cache(task):
    tsa = timeseries()
    policy = task.input['policy']

    with task.capturelogs(std=True):
        cache.refresh_policy(tsa, policy)


@task(
    domain='timeseries',
    inputs=(
        rio.string('policy', required=True),
    )
)
def refresh_formula_cache_now(task):
    tsa = timeseries()
    policy = task.input['policy']

    with task.capturelogs(std=True):
        cache.refresh_policy_now(tsa, policy)


@task(inputs=(
    rio.string('url_refinery_origin'),
    rio.string('seriesname_origin'),
    rio.string('seriesname_target'),
    rio.moment('from_insertion_date'),
    rio.string('insertion_freq_offset')
    )
)
def replicate_series_from_refinery(task):
    tsa = timeseries()
    with task.capturelogs(std=True):
        inputs = task.input
        tsa_origin = timeseries(inputs['url_refinery_origin'])
        replicate_series(
            tsa_origin,
            tsa,
            inputs['seriesname_origin'],
            inputs.get('seriesname_target', None),
            from_insertion_date=inputs.get('from_insertion_date', None),
            insertion_freq_offset=inputs.get('insertion_freq_offset', None),
        )


@task(inputs=(
    rio.string('url_refinery_origin'),
    rio.string('basket_name'),
    rio.string('prefix'),
    rio.string('suffix'),
    rio.moment('from_insertion_date'),
    rio.string('insertion_freq_offset')
)
)
def replicate_from_basket(task):
    tsa = timeseries()
    with task.capturelogs(std=True):
        inputs = task.input
        tsa_origin = timeseries(inputs['url_refinery_origin'])
        replicate_basket(
            tsa_origin=tsa_origin,
            tsa_target=tsa,
            basket_name=inputs['basket_name'],
            from_insertion_date=inputs.get('from_insertion_date', None),
            insertion_freq_offset=inputs.get('insertion_freq_offset', None),
            prefix=inputs.get('prefix', ''),
            suffix=inputs.get('suffix', ''),
        )


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
    tsa = timeseries()
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
    tsa = timeseries()
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


# migration

@task(
    domain='default',
    inputs=(
        rio.number('cpus', required=True),
    )
)
def migrate_diffs(task):
    from tshistory.migrate import migrate_add_diffstart_diffend

    cpus = task.input['cpus']

    engine = task.engine
    with task.capturelogs(std=True):
        migrate_add_diffstart_diffend(engine, 'tsh', False, True, cpus=cpus)
        migrate_add_diffstart_diffend(engine, 'tsh.group', False, True, cpus=cpus)
        migrate_add_diffstart_diffend(engine, 'tsh-upstream', False, True, cpus=cpus)
        migrate_add_diffstart_diffend(engine, 'tsh-formula-patch', False, True, cpus=cpus)
        migrate_add_diffstart_diffend(engine, 'tsh-cache', False, True, cpus=cpus)
