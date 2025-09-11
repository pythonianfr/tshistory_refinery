.. _Groups:

Groups
======

Table of Contents
-----------------

- `Introduction and Stored Groups`_
- `Group Formulas`_
- `Common Use Cases`_
- `Group API Reference`_

Introduction and Stored Groups
-------------------------------

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


Group Formulas
--------------

Group formulas enable powerful computed groups using the formula
language. Like series formulas, they are evaluated on-demand and
inherit versioning from their components.

Group Formula Operators
~~~~~~~~~~~~~~~~~~~~~~~~

The formula language provides specialized operators for working with
groups:

**group**

Retrieves a group from storage or formula, similar to the ``series``
operator:

.. code:: scheme

    (group "weather_station_fr")

**group-add**

Performs element-wise addition of multiple groups. All groups must
have compatible indexes:

.. code:: scheme

    ;; add weather data from multiple stations
    (group-add (group "station_paris")
               (group "station_lyon")
               (group "station_marseille"))

This operator aligns the groups by their time index and adds
corresponding columns.

**group-add-series**

Adds a series to every column of a group. Useful for adjustments or
calibrations:

.. code:: scheme

    ;; convert temperatures from kelvin to celsius
    (group-add-series (group "temperatures_kelvin")
                      (series "kelvin_to_celsius_offset"))

The series is broadcast to all scenarios in the group.

**bind and group-from-series**

Constructs a new group by binding multiple series together as named
scenarios:

.. code:: scheme

    ;; create scenarios from individual series
    (group-from-series
        (bind "optimistic" (series "forecast_high"))
        (bind "baseline" (series "forecast_mid"))
        (bind "pessimistic" (series "forecast_low")))

Each ``bind`` creates a named column in the resulting group. This is
particularly useful for creating scenario-based groups from individual
forecast series.

Creating and Using Group Formulas
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Register a group formula using the API:

.. code:: python

    >>> tsa.register_group_formula(
    ...     'eu_temperatures',
    ...     '(group-add (group "france_temps") '
    ...     '           (group "germany_temps") '
    ...     '           (group "spain_temps"))'
    ... )

Once registered, use it like any other group:

.. code:: python

    >>> df = tsa.group_get('eu_temperatures')
    >>> print(df.head())
                          low    mid    high
    2024-01-01 00:00:00  48.5   52.2   55.8
    2024-01-02 00:00:00  49.1   52.7   56.4

Group Formula Metadata
~~~~~~~~~~~~~~~~~~~~~~

Group formulas can have metadata like primary groups:

.. code:: python

    >>> tsa.update_group_metadata('eu_temperatures', {
    ...     'unit': 'celsius',
    ...     'frequency': 'daily',
    ...     'scenarios': 'temperature forecasts'
    ... })

Formula-Specific Methods
~~~~~~~~~~~~~~~~~~~~~~~~

Several methods are specific to group formulas:

.. code:: python

    >>> # get the formula definition
    >>> formula = tsa.group_formula('eu_temperatures')
    >>> print(formula)
    '(group-add (group "france_temps") ...)'

    >>> # get expanded formula (resolving nested formulas)
    >>> expanded = tsa.group_formula('eu_temperatures', expanded=True)

    >>> # test a formula without registering
    >>> result = tsa.group_eval_formula(
    ...     '(group-add (group "test1") (group "test2"))'
    ... )

Formula Bindings: Creating Groups from Series Formulas
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The bindings system is a powerful mechanism that transforms series
formulas into group formulas by replacing selected series references
with groups. This allows you to apply the same calculation logic
across multiple scenarios simultaneously.

**Core Concept**

Given a series formula that combines multiple series, you can "bind"
some of those series to groups. The formula then evaluates column-wise
across the bound groups, producing a group as output.

**The Family Concept**

A "family" groups together series/groups that play equivalent roles in
the formula. Key rules:

- All groups within a family must have the same number of columns (scenarios)
- The formula is evaluated column-by-column across families
- Column 1 of each group in a family is used together, then column 2, etc.

**Example: Weather Scenario Modeling**

Consider a formula that combines temperature and wind data with
adjustments:

.. code:: python

    # original series formula
    tsa.register_formula(
        'weather_index',
        '(add (mul (series "temp_base") 0.7) '
        '     (mul (series "wind_base") 0.3) '
        '     (series "seasonal_adjustment"))'
    )

Now create groups for different weather scenarios:

.. code:: python

    # temperature scenarios (3 scenarios: cold, normal, warm)
    temp_scenarios = pd.DataFrame({
        'cold': [5, 6, 7],
        'normal': [15, 16, 17],
        'warm': [25, 26, 27]
    }, index=dates)
    tsa.group_replace('temp_scenarios', temp_scenarios, 'operator')

    # wind scenarios (must also have 3 scenarios to match)
    wind_scenarios = pd.DataFrame({
        'calm': [5, 5, 5],
        'moderate': [15, 15, 15],
        'strong': [30, 30, 30]
    }, index=dates)
    tsa.group_replace('wind_scenarios', wind_scenarios, 'operator')

Bind the formula to create a group:

.. code:: python

    # define the binding
    binding = pd.DataFrame([
        ['temp_base', 'temp_scenarios', 'weather'],
        ['wind_base', 'wind_scenarios', 'weather'],
        # seasonal_adjustment remains a regular series
    ], columns=['series', 'group', 'family'])

    # register the bound group
    tsa.register_formula_bindings(
        'weather_index_scenarios',  # new group name
        'weather_index',            # source formula
        binding
    )

Result:

.. code:: python

    >>> result = tsa.group_get('weather_index_scenarios')
    >>> print(result.columns)
    ['scenario_1', 'scenario_2', 'scenario_3']

    # each column computed as:
    # scenario_1: temp_scenarios['cold'] * 0.7 + wind_scenarios['calm'] * 0.3 + seasonal_adjustment
    # scenario_2: temp_scenarios['normal'] * 0.7 + wind_scenarios['moderate'] * 0.3 + seasonal_adjustment
    # scenario_3: temp_scenarios['warm'] * 0.7 + wind_scenarios['strong'] * 0.3 + seasonal_adjustment

**Multiple Families Example**

Families are useful when you want different binding strategies for
different parts of the formula:

.. code:: python

    # formula with different types of inputs
    tsa.register_formula(
        'complex_calc',
        '(add (series "regional_data") '
        '     (mul (series "global_factor") (series "local_factor")))'
    )

    # binding with two families
    binding = pd.DataFrame([
        ['regional_data', 'regions_group', 'regions'],      # 5 regions
        ['local_factor', 'local_scenarios', 'scenarios'],   # 3 scenarios
        # global_factor remains unbound (same for all combinations)
    ], columns=['series', 'group', 'family'])

This would create a group with 15 columns (5 regions × 3 scenarios),
exploring all combinations.

**Key Points**

- Unbound series in the formula remain as series (broadcast to all columns)
- All groups in the same family must have identical column counts
- The binding creates a "bound" type group that dynamically evaluates the formula
- Use ``bindings_for(name)`` to retrieve the binding configuration for a group

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
the *group-from-series* formulaic operator).

Group API Reference
-------------------

Primary Group Operations
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: tshistory_formula.api.mainsource
    :noindex:
    :member-order: bysource
    :members: group_get, group_replace, group_delete, group_exists, group_type, group_source, group_insertion_dates, group_log, group_history, group_catalog, group_metadata, group_internal_metadata, update_group_metadata, replace_group_metadata, group_find

Formula Group Operations
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: tshistory_formula.api.mainsource
    :noindex:
    :member-order: bysource
    :members: register_group_formula, group_formula, group_eval_formula, register_formula_bindings, bindings_for
