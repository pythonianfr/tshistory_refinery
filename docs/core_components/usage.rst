Timeseries Store Usage
======================

Table of Contents
-----------------

- `Creating a series`_
- `Updating a series`_
- `Working with versions`_
- `Point and version erasure`_
- `Retrieving history`_
- `Working with metadata`_
- `Replacing a series entirely`_
- `Checking series existence`_
- `Renaming a series`_
- `Deleting a series`_
- `Finding series`_
- `Getting series information`_
- `Working with logs`_
- `Staircase operations`_
- `Time Series Operations API`_

Creating a series
-----------------

Here’s a simple example:

.. code:: python

    >>> import pandas as pd
    >>> from tshistory.api import timeseries
    >>>
    >>> tsa = timeseries('postgresql://me:password@localhost/mydb')
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

The ``update`` method is the fundamental operation for time series management,
designed for incrementally updating series as new data arrives over time.

This is good. Now, let's insert more:

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

You get back the *new information* you put inside and this is why the
`2` doesn't appear (it was already put  there in the first step).

.. code:: python

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
wasn't a new information so it was just ignored.

Working with versions
---------------------

The ``insertion_dates`` method is one of the three fundamental API points
(along with ``get`` and ``update``). Every update creates a new version of
the series, and this method returns when each version was created:

.. code:: python

    >>> tsa.insertion_dates('my_series')
    [pd.Timestamp('2018-09-26 17:10:36.988920+02:00'),
     pd.Timestamp('2018-09-26 17:12:54.508252+02:00')]

    >>> # get insertions within a date range
    >>> tsa.insertion_dates('my_series',
    ...                     from_insertion_date=pd.Timestamp('2018-09-26 17:11:00+02:00'))
    [pd.Timestamp('2018-09-26 17:12:54.508252+02:00')]

These timestamps identify the versions of your series and are what you use
with ``get`` to retrieve any past state.

Point and version erasure
--------------------------

Point erasure with NaN
~~~~~~~~~~~~~~~~~~~~~~~

You can erase specific points in a series by updating with NaN values:

.. code:: python

    >>> # erase the point at 2017-01-02
    >>> erasure = pd.Series([np.nan], index=[pd.Timestamp('2017-01-02')])
    >>> tsa.update('my_series', erasure, 'cleanup@example.com', keepnans=True)

    >>> # by default, erased points are not shown
    >>> tsa.get('my_series')
    2017-01-01    1.0
    2017-01-03    7.0
    2017-01-04    8.0
    2017-01-05    9.0
    Name: my_series, dtype: float64

    >>> # use keepnans=True to see erased points
    >>> tsa.get('my_series', keepnans=True)
    2017-01-01    1.0
    2017-01-02    NaN
    2017-01-03    7.0
    2017-01-04    8.0
    2017-01-05    9.0
    Name: my_series, dtype: float64

Version erasure with strip
~~~~~~~~~~~~~~~~~~~~~~~~~~

**WARNING: This is a DESTRUCTIVE operation that should only be used as a LAST RESORT.**

The ``strip`` method permanently removes all versions after a given insertion date:

.. code:: python

    >>> # check existing versions
    >>> tsa.insertion_dates('my_series')
    [pd.Timestamp('2018-09-26 17:10:36.988920+02:00'),
     pd.Timestamp('2018-09-26 17:12:54.508252+02:00'),
     pd.Timestamp('2018-09-26 17:15:00.000000+02:00')]

    >>> # DANGER: permanently remove versions after 17:12
    >>> tsa.strip('my_series', pd.Timestamp('2018-09-26 17:12:00+02:00'))

    >>> # versions are gone forever
    >>> tsa.insertion_dates('my_series')
    [pd.Timestamp('2018-09-26 17:10:36.988920+02:00')]

This operation cannot be undone. Use with extreme caution.

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
``get`` method with the ``revision_date`` parameter (using timestamps
obtained from ``insertion_dates``):

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

Working with metadata
---------------------

Series can have metadata attached to help document and organize them:

.. code:: python

    >>> tsa.update_metadata('temperature_sensor', {
    ...     'unit': 'celsius',
    ...     'location': 'building_a',
    ...     'sensor_type': 'PT100',
    ...     'frequency': 'hourly'
    ... })

    >>> tsa.metadata('temperature_sensor')
    {'unit': 'celsius', 'location': 'building_a', 'sensor_type': 'PT100', 'frequency': 'hourly'}

    >>> # update metadata (merges with existing)
    >>> tsa.update_metadata('temperature_sensor', {'calibrated': '2023-01-15'})
    >>> tsa.metadata('temperature_sensor')
    {'unit': 'celsius', 'location': 'building_a', 'sensor_type': 'PT100',
     'frequency': 'hourly', 'calibrated': '2023-01-15'}

    >>> # replace all metadata
    >>> tsa.replace_metadata('temperature_sensor', {'unit': 'fahrenheit', 'status': 'active'})
    >>> tsa.metadata('temperature_sensor')
    {'unit': 'fahrenheit', 'status': 'active'}

    >>> # view metadata history
    >>> tsa.old_metadata('temperature_sensor')
    [(pd.Timestamp('2023-01-01 10:00:00+00:00'),
      {'unit': 'celsius', 'location': 'building_a', 'sensor_type': 'PT100', 'frequency': 'hourly'}),
     (pd.Timestamp('2023-01-02 11:00:00+00:00'),
      {'unit': 'celsius', 'location': 'building_a', 'sensor_type': 'PT100',
       'frequency': 'hourly', 'calibrated': '2023-01-15'}),
     (pd.Timestamp('2023-01-03 09:00:00+00:00'),
      {'unit': 'fahrenheit', 'status': 'active'})]

Beyond managing metadata for individual series, you can also discover what metadata
keys are used across all series in your refinery instance:

.. code:: python

    >>> # list all metadata keys in use
    >>> tsa.list_metadata_keys()
    ['calibrated', 'frequency', 'location', 'sensor_type', 'status', 'unit']

Replacing a series entirely
----------------------------

In specific circumstances, you may need to completely replace a series - for
example, when you are working with forecast data and you need only the last updated forecast. The ``replace``
method provides this capability:

.. code:: python

    >>> # create initial series
    >>> series = pd.Series([10, 20, 30],
    ...                    pd.date_range(start=pd.Timestamp(2025, 1, 2),
    ...                                  freq='D', periods=3))
    >>> tsa.update('stock_levels_forecast', series, 'operator@example.com', insertion_date=pd.Timestamp(2025,1,1))

    >>> # later, replace the entire series with a new forecast
    >>> new_series = pd.Series([70, 50, 60],
    ...                        pd.date_range(start=pd.Timestamp(2025, 1, 3),
    ...                                      freq='D', periods=3))
    >>> tsa.replace('stock_levels_forecast', new_series, 'admin@example.com', insertion_date=pd.Timestamp(2025,1,2))

    >>> tsa.get('stock_levels_forecast')
    2025-01-03    70.0
    2025-01-04    50.0
    2025-01-05    60.0
    Freq: D, Name: stock_levels_forecast, dtype: float64

The ``replace`` method completely overwrites the series with new data,
removing any points not present in the new series.

.. note::

    It's important to note that ``replace`` preserves the complete version history.
    The replace operation appears as a new insertion date in the series history:

    .. code:: python

        >>> tsa.insertion_dates('stock_levels_forecast')
        [pd.Timestamp('2025-01-01 00:00:00+0000', tz='UTC'),  # original update
        pd.Timestamp('2025-01-02 00:00:00+0000', tz='UTC')]  # replace operation

        >>> # history shows both the original and replaced versions
        >>> history = tsa.history('stock_levels_forecast')
        >>> for idate, series in history.items():
        ...     print(f'insertion date: {idate}')
        ...     print(series)
        ...
        insertion date: 2025-01-01 00:00:00+00:00
        2025-01-02    10.0
        2025-01-03    20.0
        2025-01-04    30.0
        Name: stock_levels_forecast, dtype: float64
        insertion date: 2025-01-02 00:00:00+00:00
        2025-01-03    70.0
        2025-01-04    50.0
        2025-01-05    60.0
        Name: stock_levels_forecast, dtype: float64

    This means you can always retrieve previous states of the series before the
    replace operation using ``revision_date``.

Checking series existence
--------------------------

To check if a series exists:

.. code:: python

    >>> tsa.exists('my_series')
    True
    >>> tsa.exists('non_existent')
    False

Renaming a series
-----------------

To rename a series:

.. code:: python

    >>> tsa.rename('old_name', 'new_name')
    >>> tsa.exists('old_name')
    False
    >>> tsa.exists('new_name')
    True

Deleting a series
-----------------

To remove a series from the database:

.. code:: python

    >>> tsa.delete('my_series')
    >>> tsa.get('my_series')  # returns None

Finding series
--------------

To find series in the database:

.. code:: python

    >>> # find all series
    >>> tsa.find()
    ['my_series', 'temperature_fr', 'calculated_avg', 'temperature_paris']

    >>> # find with metadata
    >>> results = tsa.find('(by.name "temperature")', meta=True)
    >>> results
    ['temperature_fr', 'temperature_paris']
    >>> # access directly the metadata of found series
    >>> results[0].meta
    {'unit': 'celsius', 'location': 'fr'}

See the :ref:`search_language` documentation for comprehensive query capabilities.

The older ``catalog()`` method is still available but returns everything at once
from all sources in a slightly cumbersome structure:

.. code:: python

    >>> tsa.catalog()
    {'local': [('my_series', 'primary'), ('temperature_fr', 'primary'), ('temperature_paris', 'primary')],
     'remote': [('calculated_avg', 'formula')]}

The ``find()`` API is generally preferred for its flexibility.

Getting series information
---------------------------

To get detailed information about a series:

.. code:: python

    >>> tsa.type('my_series')
    'primary'

    >>> tsa.interval('my_series')
    (pd.Timestamp('2025-01-01'), pd.Timestamp('2025-01-05'))

    >>> tsa.source('my_series')
    'local'

    >>> # get inferred frequency
    >>> tsa.inferred_freq('my_series')
    'D'  # daily frequency

    >>> # get various informations with internal_metadata.
    >>> # tzawareness, value type (float, string) and supervision_status
    >>> tsa.internal_metadata('my_series')
    {'left': '2025-01-01T00:00:00',
    'right': '2025-01-05T00:00:00',
    'tzaware': False,
    'tablename': 'my_series',
    'index_type': 'datetime64[ns]',
    'value_type': 'float64',
    'index_dtype': '<M8[ns]',
    'value_dtype': '<f8',
    'supervision_status': 'unsupervised'}

Working with logs
-----------------

To see the history of operations on a series:

.. code:: python

    >>> tsa.log('my_series', limit=5)
    [{'date': pd.Timestamp('2018-09-26 17:10:36.988920+02:00'),
      'author': 'babar@pythonian.fr',
      'meta': {},
      'rev': 1},
     {'date': pd.Timestamp('2018-09-26 17:12:54.508252+02:00'),
      'author': 'babar@pythonian.fr',
      'meta': {},
      'rev': 2}]

Staircase operations
--------------------

The staircase operations are specialized methods for forecast backtesting and
time-consistent analysis. They reconstruct series as they were available at
specific lead times, which is essential for evaluating forecast accuracy 
without look-ahead bias.

The ``staircase`` method shows what data was available at ``(value_date - delta)``.
For each value date, it looks back delta time to find what was known at that
historical moment:

.. code:: python

    >>> staircase = tsa.staircase('forecast_series', delta=pd.Timedelta(days=1))

The ``block_staircase`` method is more sophisticated. It rebuilds a series from
successive blocks of history taken at regular revision intervals. Each block
corresponds to data from a specific revision date. This is useful for analyzing
how forecasts evolve over time with a consistent publication schedule.

For example, with daily revisions at 10am and a 24-hour maturity offset, the
method assembles blocks where each day's values come from the revision published
24 hours before:

.. code:: python

    >>> bsc = tsa.block_staircase(
    ...     name='forecast_series',
    ...     from_value_date=pd.Timestamp('2020-01-03', tz='utc'),
    ...     to_value_date=pd.Timestamp('2020-01-05', tz='utc'),
    ...     revision_freq={'days': 1},
    ...     revision_time={'hour': 10},
    ...     revision_tz='UTC',
    ...     maturity_offset={'hours': 24},
    ...     maturity_time={'hour': 4}
    ... )

The result is a series where different time periods come from different revisions,
allowing you to see how the forecast performed with a consistent lead time across
the entire period.

Time Series Operations API
---------------------------

The time series API provides comprehensive methods for managing time series data:

.. autoclass:: tshistory.api.mainsource
    :noindex:
    :member-order: bysource
    :members: get, update, replace, delete, exists, rename, insertion_dates, history, strip, log, metadata, internal_metadata, update_metadata, replace_metadata, old_metadata, list_metadata_keys, type, source, interval, inferred_freq, find, catalog, staircase, block_staircase
