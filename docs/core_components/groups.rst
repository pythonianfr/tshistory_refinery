.. _Groups:

Groups
======

Groups in tshistory are collections of related time series that share
the same time index and are managed as a unit. They are particularly
useful for handling scenarized time series or multivariate time series
data where multiple series need to be kept in sync.

They come in two flavors: primary groups (stored) and formula groups.

Here's how creating a primary group looks like:

.. code-block:: python

 import pandas as pd
 from tshistory.api import timeseries

 tsa = timeseries()

 group_data = pd.DataFrame({
     'temperature': [20, 21, 22],
     'pressure': [1013, 1014, 1015],
     'humidity': [60, 65, 70]
 }, index=pd.date_range('2025-01-01', periods=3, freq='D'))

 tsa.group_replace('weather_station_fr', group_data, 'operator')


And this is a formula group:

.. code-block:: python

 import pandas as pd
 from tshistory.api import timeseries

 tsa = timeseries('postgresql://user:pass@host/db')

 tsa.register_group_formula(
     'weather_station_form',
     '(group-add (group "weather_station_fr") (group "weather_station_be"))'
 )

.. note::

  Groups in tshistory have a **fixed schema** once created: you can't
  change the **columns** nor, like with time series, fundamental
  attributes such as tz-awareness.

  All ``group_update`` and ``group_replace`` operations must match the
  exact column structure of the original group.

  This constraint ensures:

  - **Data Integrity**: Prevents accidental schema changes that could
    break downstream consumers
  - **Version Consistency**: All historical versions of a group
    maintain the same structure


Group Operations API
--------------------

The groups feature provides several API methods for managing grouped
time series:

.. autoclass:: tshistory_formula.api.mainsource
    :noindex:
    :member-order: bysource
    :members: group_get, group_replace, group_delete, group_exists, group_type, group_source, group_insertion_dates, group_log, group_history, group_catalog, group_metadata, group_internal_metadata, update_group_metadata, replace_group_metadata, group_find

Formula Group Operations
------------------------

These methods manage computed groups using the formula language:

.. autoclass:: tshistory.api.mainsource
    :noindex:
    :member-order: bysource
    :members: register_group_formula, group_formula, group_eval_formula, register_formula_bindings, bindings_for


Best Practices
---------------

1. **Plan Your Schema**: Design your group schema before creation, considering future needs
2. **Use Descriptive Names**: Choose clear, descriptive names for both groups and columns

Common Use Cases
----------------

Groups are ideal for:

- **Ensemble Forecasts**: Scenarized stochastic weather scenarios

They may (depending on various factors) be interesting with:

- **Financial Data**: OHLC (Open, High, Low, Close) price data
- **IoT Sensors**: Multiple sensor readings from the same device
- **Economic Indicators**: Related economic metrics that should be kept in sync

In many cases, it will be more convenient to handle data acquisition
as individual time series, and then create a group from them (using
the *from-from-series* formulaic operator).
