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
schedulers such as `Apache AirFlow <https://airflow.apache.org/>`_,
for the additional capacities it provides. Later in this document we
will show how to use the Refinery integrated task manager to set up
powerful data acquisition pipelines.

Now, let's back to our scraping function and examine what it does in
detail.

.. code:: python

        fromdate = pd.to_datetime('today') - pd.Timedelta(1, 'd')
        todate = pd.to_datetime('today') + pd.Timedelta(7, 'd')

Here we start by defining the time window for the data to be
fetched. Since it is about forecasts, a window from yesterday to next
week is reasonnable.

.. code:: python

        try:
            df = EntsoePandasClient(api_key='apikey').query_wind_and_solar_forecast(
                'FR',
                start=fromdate.tz_localize('Europe/Paris'),
                end=todate.tz_localize('Europe/Paris'),
            )
        except NoMatchingDataError:
            return

Next, we fetch the data using the ENTSO-E client dedicated method. We
provide the `zone` and the time window. On error we exit immediately.

.. code:: python

        name_map = {
            'Solar': 'power.prod.solar.fr.mwh.entsoe.fcst.h',
            'Wind Onshore': 'power.prod.windonshore.fr.mwh.entsoe.fcst.h'
        }

The resulting data frame may have several columns. We *know* (by
reading the documentation) that in our case (zone `FR`) we will have
two columns. We construct a mapping from these column names to the
series names we want to have. It is very important to have a good
naming convention (in our case we indicate: nature of the flux, type
of production, zone, provenance of the data, nature of the data
- forecast is not observed nor a nomination - and finally the
expected granularity).

.. code:: python

        tsa = timeseries()
        for col, name in name_map.items():
            tsa.update(name, df[col], 'entsoe-scraper')

We create the time series refinery api object and then do an update of
the series.

This is one of the simplest thing we can do to acquire data.

.. note::

   This however is a bit ad-hoc and lacks separation of concerns:

   * acquisition and injection happens in the same place

   * the horizon/time window handling is hard-coded

   * there is no handling of metadata, and we won't know easily the
     provenance of the acquired data

   * we hard-coded the zone and time window

   In the chapter on advanced topics we will propose a more structured
   way to organize this and address the issues listed above.


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


