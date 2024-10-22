Create a time series information system
=======================================

In the basics chapter, we've seen how to use a number of essential
building blocks.

Building on this knowledge, we want to address a more ambitious goal:
how to write a coherent, well curated time series information system.

We will start from the acquisition of time series data. There are some
challenges there and we will address the most common.

Once we have a complete "primary data" set, made of raw time series,
we will start the process of building the **refined** series that we
can use:

* as input for models

* as input for high-level analysis (e.g. fundamental economic
  analysis)

* as a basis for complex dashboards and decision support systems

At the end of this chapter, you will master all fundamental aspects of
building a complete system.


Pipelines : starting with injecting data
----------------------------------------

Data acquisition is the first step.

Let's start with a simple example, in which we will get forecasts for
renewable production in France. We will use a ready made open source
client to get data from the `ENTSO-E <https://www.entsoe.eu/>`_ data
repository. ENTSO-E itself exposes data for free in the context of the
UE power markets.

.. code-block:: python
   :linenos:

    import pandas as pd
    from entsoe.exceptions import NoMatchingDataError
    from entsoe import EntsoePandasClient
    from tshistory.api import timeseries

    def get_res_generation_forecast_entsoe():
        fromdate = pd.to_datetime('today') - pd.Timedelta(1, 'd')
        todate = pd.to_datetime('today') + pd.Timedelta(7, 'd')
        try:
            df = EntsoePandasClient(api_key='apikey').query_wind_and_solar_forecast(
                'FR',
                start=fromdate.tz_localize('Europe/Paris'),
                end=todate.tz_localize('Europe/Paris'),
            )
        except NoMatchingDataError:
            return

        name_map = {
            'Solar': 'power.prod.solar.fr.mwh.entsoe.fcst.h',
            'Wind Onshore': 'power.prod.windonshore.fr.mwh.entsoe.fcst.h'
        }

        tsa = timeseries()
        for col, name in name_map.items():
            tsa.update(name, df[col], 'entsoe-scraper')


Such a scraping function, put into a Python script, could then be
scheduled to be run at periodic moments of the day to continuously
feed the time series repository; for instance using `cron` on Linux or
`at` on Windows.

This is a bare-bones solution, which unsurprinsinly is largely used in
the industry. Some prefer to use sophisticated and refined task
schedulers such as Apache AirFlow, for the additional capacities it
provides. Later in this document we will show how to use the Refinery
integrated task manager to set up powerful data acquisition pipelines.

Now, let's back to our scraping function and examine what it does in
detail.

Writing and using scrapers for primary series
---------------------------------------------

Building formulas on top to get curated series
----------------------------------------------

Introduce the task manager
--------------------------

Feeding a model with series, getting the output in the system, with the task manager
------------------------------------------------------------------------------------

Building a basic dashboard
--------------------------


