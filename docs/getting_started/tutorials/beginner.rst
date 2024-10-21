Basics
========

General setup reminder
-----------------------

**Setup your machine**

Let's pip-install the client:

.. code:: bash

   $ pip install tshistory_refinery

And save the config file ``tshistory.cfg`` in the home
directory, containing:

.. code:: ini

   [auth]
   refinery.uri = https://refinery.<myrefinery>.pythonian.fr/api
   refinery.login = <username>
   refinery.password = <password>


**The naming convention : a pillar for your refinery**

First and foremost, users of the refinery need to gather and define
together the naming convention. This is an important aspect of the
**data governance**.

This convention concerns all the series (primary and formulas)
available in the data referential.

Every person of the team needs to know it very well. They will
define new data following this convention and find quickly and
efficiently the data they need.

Here are some tips:

* define the separator of your keywords. Let's choose a dot for our
  example "."

* gather all the types of keywords you could need. Let's assume we
  work with fruits transactions, here is our list:

    * type of aliment
    * type of fruit
    * country of origin
    * country of selling
    * type of transaction (sell/buy)
    * unit (currency/weight)
    * farmer
    * buyer
    * granularity

* choose the most intuitive order of keywords:
  **aliment.fruit.transaction.farmer.buyer.from_country.to_country.unit.granularity**

example :
*fruit.banana.buy.misterfield.superfrenchshop.civ.fr.eur/t.d*

.. note::

   Some of these aspects probably belong to the **metadata** of the
   series. However a good name goes a long way when it comes to making
   a data referential easily usable for everyone. So encoding some

**Initialize timeseries client**

.. code:: python

    from tshistory.api import timeseries
    tsa = timeseries()


How to create / update / read "primary" time series
---------------------------------------------------

**Create a series**

.. code:: python

    import pandas as pd

    ts = pd.Series(
        data=[1, 2, 3],
        index=pd.date_range(start="2024-11-01", freq="D", periods=3)
    )
    seriesname = "fruit.banana.buy.misterfield.superfrenchshop.civ.fr.eur/t.d"

    tsa.update(seriesname, ts, author="fruit@lover")

**Get your series**

>>> ts = tsa.get(seriesname)
>>> ts
2024-11-01    1.0
2024-11-02    2.0
2024-11-03    3.0
Name: fruit.banana.buy.misterfield.superfrenchshop.civ.fr.eur/t.d, dtype: float64

**Update your series**

Overwrite existing values and add one for the 4th of November:

.. code:: python

    ts = pd.Series(
        data=[5, 6, 7, 10],
        index=pd.date_range(start="2024-11-01", freq="D", periods=4)
    )
    seriesname = "fruit.banana.buy.misterfield.superfrenchshop.civ.fr.eur/t.d"

    tsa.update(seriesname, ts, author="fruit@lover")

Get the new version of the timeseries:

>>> ts = tsa.get(seriesname)
>>> ts
2024-11-01     5.0
2024-11-02     6.0
2024-11-03     7.0
2024-11-04    10.0
Freq: D, Name: fruit.banana.buy.misterfield.superfrenchshop.civ.fr.eur/t.d, dtype: float64

.. note::
    The update is as important as get.

    Let's explain its semantics:

    - its input series is used to patch the existing series state
    - existing points with unchanged values will just be dropped
    - new points will be added
    - modified points will be updated
    - points with nan values will erase existing points
    - if there is no difference between the inserted points and the ones in base, no revision is created
    - it returns a series of the points that have been either added, update or erased


How to Exists / Rename / Delete time series
-------------------------------------------

>>> tsa.exists(seriesname)
True
>>> tsa.rename(seriesname, 'new.name')
>>> tsa.exists(seriesname)
False
>>> tsa.delete('new.name')
>>> tsa.exists('new.name')
False


Going further: playing with revisions
-------------------------------------

Now let's assume that we work with a forecast

.. code:: python

    # create a forecast series
    insertion_dates = pd.date_range(
        start=pd.Timestamp('2024-11-01T09:00+00:00'),
        end=pd.Timestamp('2024-11-04T09:00+00:00'),
        freq='D'
    )
    i=0
    for insertion_date in insertion_dates:
        ts = pd.Series(
            [1, 2, 3],
            index = pd.date_range(start=insertion_date.date(), periods=3, freq='D')
        )
        tsa.update(
            'my_morning_forecast',
            ts+i,
            'sensei',
            insertion_date=insertion_date
        )
        i=+1

Let's use the get operator. It will give us the last version of each timestamp.

>>> tsa.get("my_morning_forecast")
2024-11-01    1.0
2024-11-02    2.0
2024-11-03    2.0
2024-11-04    2.0
2024-11-05    3.0
2024-11-06    4.0
Name: my_morning_forecast, dtype: float64

Now, let's investigate our forecast versions.

>>> tsa.history("my_morning_forecast")
{
    Timestamp('2024-11-01 09:00:00+0000', tz='UTC'):
        2024-11-01    1.0
        2024-11-02    2.0
        2024-11-03    3.0
        Name: my_morning_forecast, dtype: float64,
    Timestamp('2024-11-02 09:00:00+0000', tz='UTC'):
        2024-11-01    1.0
        2024-11-02    2.0
        2024-11-03    3.0
        2024-11-04    4.0
        Name: my_morning_forecast, dtype: float64,
    Timestamp('2024-11-03 09:00:00+0000', tz='UTC'):
        2024-11-01    1.0
        2024-11-02    2.0
        2024-11-03    2.0
        2024-11-04    3.0
        2024-11-05    4.0
        Name: my_morning_forecast, dtype: float64,
    Timestamp('2024-11-04 09:00:00+0000', tz='UTC'):
        2024-11-01    1.0
        2024-11-02    2.0
        2024-11-03    2.0
        2024-11-04    2.0
        2024-11-05    3.0
        2024-11-06    4.0
        Name: my_morning_forecast, dtype: float64
}

If we want to see only the differential :

>>> tsa.history("my_morning_forecast", diffmode=True)
{
    Timestamp('2024-11-01 09:00:00+0000', tz='UTC'):
        2024-11-01    1.0
        2024-11-02    2.0
        2024-11-03    3.0
        Name: my_morning_forecast, dtype: float64,
    Timestamp('2024-11-02 09:00:00+0000', tz='UTC'):
        2024-11-04    4.0
        Name: my_morning_forecast, dtype: float64,
    Timestamp('2024-11-03 09:00:00+0000', tz='UTC'):
        2024-11-03    2.0
        2024-11-04    3.0
        2024-11-05    4.0
        Name: my_morning_forecast, dtype: float64,
    Timestamp('2024-11-04 09:00:00+0000', tz='UTC'):
        2024-11-04    2.0
        2024-11-05    3.0
        2024-11-06    4.0
        Name: my_morning_forecast, dtype: float64
}

Let's assume we want the forecast of the 3rd of Nov at 12am, after the morning run :

>>> tsa.get("my_morning_forecast", revision_date=pd.Timestamp("2024-11-03T12:00:00+00:00"))
2024-11-01    1.0
2024-11-02    2.0
2024-11-03    2.0
2024-11-04    3.0
2024-11-05    4.0
Name: my_morning_forecast, dtype: float64


Adding and fetching metadata
----------------------------

.. code:: python

    tsa.update_metadata(
        "my_morning_forecast",
        {
            'fruit': 'orange',
            'buyer': 'superfrenchshop',
            'seasonal': 0
            'calibration': '2024-10-01'
        }
    )

>>> tsa.metadata("my_morning_forecast")
{
    'buyer': 'superfrenchshop',
    'fruit': 'orange',
    'seasonal': 0,
    'calibration': '2024-10-01'
}

.. note::
    Metadata is a very important tool to structure the time series referential.
    We insisted on the naming convention in the beginning and indeed good names should be good keys and embed a number of metadata aspects of the series.
    But it is not always enough.

    Metadata, at the Python API level, will be managed with flat Python dictionaries.


Using find with metadata
------------------------

Using formulas : create a formula
---------------------------------

Similarities and differences with primary series
------------------------------------------------

Using the formula editor
------------------------

Seeing all the formulas
-----------------------

