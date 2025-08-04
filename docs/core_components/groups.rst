.. _Groups:

Groups
======

Groups in tshistory are collections of related time series that share
the same time index and are managed as a unit. They are particularly
useful for handling multivariate time series data where multiple
series need to be kept in sync.

.. highlight:: python
.. code-block:: python

 from tshistory.api import timeseries
 tsa = timeseries('postgresql://user:pass@host/db')

 # Create a group with multiple series
 import pandas as pd
 group_data = pd.DataFrame({
     'temperature': [20, 21, 22],
     'pressure': [1013, 1014, 1015],
     'humidity': [60, 65, 70]
 }, index=pd.date_range('2024-01-01', periods=3, freq='D'))

 tsa.group_replace('weather_station', group_data, 'operator')

Group Schema Constraints
-------------------------

.. note::
  Groups in tshistory have a **fixed schema** once
  created. This is a fundamental constraint that affects all group
  operations.

Key Rules
^^^^^^^^^

1. **Column Set is Immutable**: Once a group is created, its column names and types cannot be changed
2. **No Adding Columns**: You cannot add new series to an existing group
3. **No Removing Columns**: You cannot remove series from an existing group
4. **Schema Validation**: All ``group_update`` and ``group_replace`` operations must match the exact column structure of the original group

Why This Constraint Exists
^^^^^^^^^^^^^^^^^^^^^^^^^^^

This constraint ensures:

- **Data Integrity**: Prevents accidental schema changes that could break downstream consumers
- **Version Consistency**: All historical versions of a group maintain the same structure


Group Operations API
--------------------

The groups feature provides several API methods for managing grouped
time series:

.. autoclass:: tshistory.api.mainsource
    :noindex:
    :member-order: bysource
    :members: group_get, group_replace, group_delete, group_exists, group_type, group_source, group_insertion_dates, group_log, group_history, group_catalog, group_metadata, group_internal_metadata, update_group_metadata, replace_group_metadata, group_find

Best Practices
---------------

1. **Plan Your Schema**: Design your group schema before creation, considering future needs
2. **Use Descriptive Names**: Choose clear, descriptive names for both groups and columns
3. **Version Control**: When schema changes are needed, use versioned group names (e.g., ``group_v1``, ``group_v2``)

Common Use Cases
----------------

Groups are ideal for:

- **Financial Data**: OHLC (Open, High, Low, Close) price data
- **IoT Sensors**: Multiple sensor readings from the same device
- **Economic Indicators**: Related economic metrics that should be kept in sync
- **Weather Stations**: Multiple weather measurements from the same location
- **Ensemble Forecasts**: 
- **System Metrics**: CPU, memory, disk usage from the same server

Example: Financial OHLC Data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

 import pandas as pd
 import numpy as np

 # Create OHLC data
 dates = pd.date_range('2024-01-01', periods=100, freq='D')
 ohlc_data = pd.DataFrame({
     'open': np.random.randn(100).cumsum() + 100,
     'high': np.random.randn(100).cumsum() + 102,
     'low': np.random.randn(100).cumsum() + 98,
     'close': np.random.randn(100).cumsum() + 100
 }, index=dates)

 # Store as group
 tsa.group_replace('stock_AAPL_ohlc', ohlc_data, 'market_data')

 # Retrieve the group
 ohlc = tsa.group_get('stock_AAPL_ohlc')

 # Get metadata
 meta = tsa.group_metadata('stock_AAPL_ohlc')

 # Search for OHLC groups
 ohlc_groups = tsa.group_find('(by.name "ohlc")')
