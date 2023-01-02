Installation
==============================================

Just pip-install it:

.. code:: bash

   $ pip install tshistory_refinery

Create first a postgresql database:

.. code:: bash

   $ createdb my_time_series

Then initialize the database schema:

.. code:: bash

   $ tsh init-db postgresql:///my_time_series --no-dry-run

Last item: we need a configuration file ``refinery.cfg`` in the home
directory, containing:

.. code:: ini

   [db]
   uri = postgresql:///my_time_series


Introduction
============

Purpose
-------

``tshistory`` is targetted at applications using time series where
`backtesting <https://en.wikipedia.org/wiki/Backtesting>`__ and
`cross-validation <https://en.wikipedia.org/wiki/Cross-validation_(statistics)>`__
are an essential feature.

It provides exhaustivity and efficiency of the storage, with a simple
Python api.

It can be used as a building block for machine learning, model
optimization and validation, both for inputs and outputs.

Principles
----------

There are many ways to represent timeseries in a relational database,
and ``tshistory`` provides two things:

-  a base python API which abstracts away the underlying storage

-  a postgres model, which emphasizes the compact storage of successive
   states of series

The core idea of tshistory is to handle successive versions of
timeseries as they grow in time, allowing to get older states of any
series.

Getting started tutorials
==============================================

Starting with a fresh database
------------------------------

You need a postgresql database. You can create one like this:

.. code:: shell

    createdb mydb

Then, initialize the ``tshistory`` tables, like this:

.. code:: python

    tsh init-db postgresql://me:password@localhost/mydb

From this you’re ready to go !

Creating a series
-----------------

However here’s a simple example:

.. code:: python

    >>> import pandas as pd
    >>> from tshistory.api import timeseries
    >>>
    >>> tsa = timeseries('postgres://me:password@localhost/mydb')
    >>>
    >>> series = pd.Series([1, 2, 3],
    ...                    pd.date_range(start=pd.Timestamp(2017, 1, 1),
    ...                                  freq='D', periods=3))
    # db insertion
    >>> tsa.update('my_series', series, 'babar@pythonian.fr')
    ...
    2017-01-01    1.0
    2017-01-02    2.0
    2017-01-03    3.0
    Freq: D, Name: my_series, dtype: float64

    # note how our integers got turned into floats
    # (there are no provisions to handle integer series as of today)

    # retrieval
    >>> tsa.get('my_series')
    ...
    2017-01-01    1.0
    2017-01-02    2.0
    2017-01-03    3.0
    Name: my_series, dtype: float64

Note that we generally adopt the convention to name the time series api
object ``tsa``.

Updating a series
-----------------

This is good. Now, let’s insert more:

.. code:: python

    >>> series = pd.Series([2, 7, 8, 9],
    ...                    pd.date_range(start=pd.Timestamp(2017, 1, 2),
    ...                                  freq='D', periods=4))
    # db insertion
    >>> tsa.update('my_series', series, 'babar@pythonian.fr')
    ...
    2017-01-03    7.0
    2017-01-04    8.0
    2017-01-05    9.0
    Name: my_series, dtype: float64

    # you get back the *new information* you put inside
    # and this is why the `2` doesn't appear (it was already put
    # there in the first step)

    # db retrieval
    >>> tsa.get('my_series')
    ...
   2017-01-01    1.0
   2017-01-02    2.0
   2017-01-03    7.0
   2017-01-04    8.0
   2017-01-05    9.0
   Name: my_series, dtype: float64

It is important to note that the third value was *replaced*, and the two
last values were just *appended*. As noted the point at ``2017-1-2``
wasn’t a new information so it was just ignored.

Retrieving history
------------------

We can access the whole history (or parts of it) in one call:

.. code:: python

    >>> history = tsa.history('my_series')
    ...
    >>>
    >>> for idate, series in history.items(): # it's a dict
    ...     print('insertion date:', idate)
    ...     print(series)
    ...
    insertion date: 2018-09-26 17:10:36.988920+02:00
    2017-01-01    1.0
    2017-01-02    2.0
    2017-01-03    3.0
    Name: my_series, dtype: float64
    insertion date: 2018-09-26 17:12:54.508252+02:00
    2017-01-01    1.0
    2017-01-02    2.0
    2017-01-03    7.0
    2017-01-04    8.0
    2017-01-05    9.0
    Name: my_series, dtype: float64

Note how this shows the full serie state for each insertion date. Also
the insertion date is timzeone aware.

Specific versions of a series can be retrieved individually using the
``get`` method as follows:

.. code:: python

    >>> tsa.get('my_series', revision_date=pd.Timestamp('2018-09-26 17:11+02:00'))
    ...
    2017-01-01    1.0
    2017-01-02    2.0
    2017-01-03    3.0
    Name: my_series, dtype: float64
    >>>
    >>> tsa.get('my_series', revision_date=pd.Timestamp('2018-09-26 17:14+02:00'))
    ...
    2017-01-01    1.0
    2017-01-02    2.0
    2017-01-03    7.0
    2017-01-04    8.0
    2017-01-05    9.0
    Name: my_series, dtype: float64

It is possible to retrieve only the differences between successive
insertions:

.. code:: python

    >>> diffs = tsa.history('my_series', diffmode=True)
    ...
    >>> for idate, series in diffs.items():
    ...   print('insertion date:', idate)
    ...   print(series)
    ...
    insertion date: 2018-09-26 17:10:36.988920+02:00
    2017-01-01    1.0
    2017-01-02    2.0
    2017-01-03    3.0
    Name: my_series, dtype: float64
    insertion date: 2018-09-26 17:12:54.508252+02:00
    2017-01-03    7.0
    2017-01-04    8.0
    2017-01-05    9.0
    Name: my_series, dtype: float64

You can see a series metadata:

.. code:: python

    >>> tsa.update_metadata('series', {'foo': 42})
    >>> tsa.metadata('series')
    {foo: 42}