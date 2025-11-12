.. _search_language:

Search Query Language Reference
===============================

The Refinery search system uses a powerful query language to
find time series and groups based on metadata, names, and other
properties. This guide provides comprehensive documentation with
extensive examples.

Table of Contents
-----------------

- `Quick Start`_
- `Query Syntax`_
- `Basic Search Operators`_
- `Logical Operators`_
- `Formula-Specific Operators`_
- `Tree and Cache Operators`_

Quick Start
-----------

Your First Search
~~~~~~~~~~~~~~~~~

.. code-block:: python

    # find all series containing "temperature" in their name
    results = tsa.find('(by.name "temperature")')
    assert results == ['room_temperature', 'outside_temperature', 'water_temperature']

    # find series by metadata
    results = tsa.find('(by.metaitem "sensor_type" "humidity")', meta=True)
    assert results[0].meta == {'sensor_type': 'humidity', 'unit': 'percent', 'location': 'building_a'}

    # combine conditions
    results = tsa.find('(by.and (by.name "daily") (by.metaitem "region" "europe"))', limit=50)
    assert len(results) <= 50


.. note::

   As the examples above show, results look like strings - which they
   are ! But they carry .meta and imeta attributes which, when queried
   with meta=True, get populated.


.. warning::

   By default, ``find()`` returns **only timezone-aware series**. If you need to search for timezone-naive series, you must explicitly use the ``by.not`` operator combined with ``by.tzaware``:

   .. code-block:: python

       # find only timezone-naive series
       naive_series = tsa.find('(by.not (by.tzaware))')

       # combine with other conditions to find naive series
       naive_temp = tsa.find('(by.and (by.name "temperature") (by.not (by.tzaware)))')



More find API options
~~~~~~~~~~~~~~~~~~~~~

The find method offers additional capabilities for more advanced use
cases. You can retrieve all series using either ``find()`` with no
parameters or the explicit ``(by.everything)`` operator.

In federated setups with multiple data sources, the ``sources``
parameter lets you filter results by origin. The result objects also
carry useful attributes: ``.source`` indicates where the data comes
from (local or remote) and ``.kind`` shows whether it's a primary
(stored) or formula (computed) series.

.. code-block:: python

    # get all series - two equivalent ways
    allseries = tsa.find()  # no parameters
    allseries = tsa.find('(by.everything)')  # explicit

    # filter by source in federated setups
    localonly = tsa.find('(by.everything)', sources=['local'])
    assert all(s.source == 'local' for s in localonly)

    remote_series = tsa.find('(by.name "sales")', sources=['remote'])
    assert all(s.source == 'remote' for s in remote_series)

    # examine result attributes
    results = tsa.find('(by.name "temperature")')
    assert results[0].source in ('local', 'remote')
    assert results[0].kind in ('primary', 'formula')

Finding Groups
~~~~~~~~~~~~~~

The same search capabilities are available for groups through the
``group_find`` method. It supports all the same operators and
parameters as ``find``, but searches within the groups namespace
instead of series.

.. code-block:: python

    # find groups by name
    groups = tsa.group_find('(by.name "weather")')
    assert groups == ['weather_station_paris', 'weather_forecast_lyon']

.. note::

   To create actual groups (DataFrames) from **series** search results, you have two approaches:
   
   .. code-block:: python
   
       # approach 1: using group-from-series formula operator
       tsa.register_group_formula(
           'sensor_group',
           '''(group-from-series 
               (bind "sensor1" (series "temperature_sensor_1"))
               (bind "sensor2" (series "temperature_sensor_2"))
               (bind "sensor3" (series "temperature_sensor_3")))'''
       )
       
       # approach 2: python code to build dataframe from search results  
       results = tsa.find('(by.metaitem "type" "sensor")')
       df = pd.DataFrame({
           name: tsa.get(name) 
           for name in results
       })

Saving Searches with Baskets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Baskets allow you to save frequently used search queries for reuse. This is particularly useful for complex queries or when the same set of series needs to be referenced multiple times. Baskets work for both series and groups independently - they maintain separate namespaces controlled by the ``group`` parameter.

.. code-block:: python

    # register a basket for series
    tsa.register_basket('energy_sensors', '(by.and (by.metaitem "type" "sensor") (by.metaitem "category" "energy"))')
    
    # register a basket for groups  
    tsa.register_basket('weather_groups', '(by.name "weather")', group=True)
    
    # list all series baskets
    series_baskets = tsa.list_baskets()
    assert 'energy_sensors' in series_baskets
    
    # execute a basket query (supports same parameters as find)
    results = tsa.basket('energy_sensors', limit=10, meta=True)
    assert results[0].meta['type'] == 'sensor'
    
    # use the basket in formulas
    formula = '(add (findseries (by.basket "energy_sensors")))'


Query Syntax
------------

Structure
~~~~~~~~~

.. code-block:: scheme

    ;; Basic structure
    (operator "argument")

    ;; With multiple arguments
    (operator "arg1" "arg2")

    ;; Nested queries
    (by.and (by.name "sales") (by.metaitem "region" "north"))

Basic Syntax Rules
~~~~~~~~~~~~~~~~~~

1. **Parentheses**: Every query is wrapped in parentheses ``(operator args...)``
2. **Quotes**: All string values use double quotes ``"value"``
3. **Nesting**: Queries can be nested for complex logic
4. **Case Sensitivity**: 
   - Name searches (``by.name``) are **case-insensitive**
   - Metadata keys and values (``by.metakey``, ``by.metaitem``) are **case-sensitive**
   - Formula content searches (``by.formulacontents``) are **case-sensitive**


Basic Search Operators
----------------------

Name-based Search: ``by.name``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Searches for series whose names contain the specified substring
(case-insensitive):

.. code-block:: scheme

    ;; Basic name search
    (by.name "temperature")  ;; Matches: "room_temperature", "temperature_sensor"


Metadata Key Search: ``by.metakey``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Finds series that have a specific metadata key (regardless of the
value):

.. code-block:: scheme

    ;; Basic metadata key search
    (by.metakey "region")    ;; All series that have a "region" metadata field

Everything Operator: ``by.everything``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Returns all series without any filtering:

.. code-block:: scheme

    (by.everything)  ;; Returns all series


Timezone-Aware Filter: ``by.tzaware``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Filters for timezone-aware series:

.. code-block:: scheme

    (by.tzaware)  ;; Only timezone-aware series


Metadata Value Search: ``by.metaitem``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Finds series where a specific metadata key has a specific value. The
point of examples here is also to illustrate use of the series
matadata:

.. code-block:: scheme

    ;; Basic metadata value search
    (by.metaitem "region" "europe")          ;; Series in Europe region
    (by.metaitem "data_type" "financial")    ;; Financial data

    ;; Quality and status
    (by.metaitem "quality" "high")           ;; High quality data
    (by.metaitem "confidence" "reliable")    ;; Reliable data


Internal Metadata Search: ``by.internal-metaitem``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Searches internal metadata (system-managed metadata):

.. code-block:: scheme

    (by.internal-metaitem "supervision_status" "supervised")  ;; Find supervised series


Value Comparison Search
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Finds series where a metadata value meets a numerical condition.

.. warning::

   It must be noted that while these filters can be used by ``.find``
   and in baskets, they are also available in the formula language, as
   input parameters of the ``findseries`` operator. In the later case,
   the notation can be different. This is the case for values
   filtering.

   In any case, the Web interface provides structured editors that can
   help you discover the available operators in the relevant context
   (find / basket / findseries) and will guide you write correct
   filters.

.. code-block:: scheme

    ;; Direct comparison operators (for use with find)
    (< "temperature" 25)          ;; Temperature metadata < 25
    (<= "confidence" 0.8)         ;; Confidence <= 80%
    (> "threshold" 100)           ;; Threshold > 100
    (>= "count" 10)              ;; Count >= 10
    (= "status" "active")        ;; Status equals "active"


Logical Operators
-----------------

AND Operator: ``by.and``
~~~~~~~~~~~~~~~~~~~~~~~~~

Combines multiple conditions where ALL must be true:

.. code-block:: scheme

    (by.and (by.name "temperature") (by.metaitem "region" "europe"))


OR Operator: ``by.or``
~~~~~~~~~~~~~~~~~~~~~~

Combines multiple conditions where ANY can be true:

.. code-block:: scheme

    (by.or (by.metaitem "region" "europe") (by.metaitem "region" "asia"))

    ;; Multiple regions
    (by.or (by.metaitem "country" "france")
            (by.metaitem "country" "germany")
            (by.metaitem "country" "italy"))


NOT Operator: ``by.not``
~~~~~~~~~~~~~~~~~~~~~~~~

Excludes series matching the specified condition:

.. code-block:: scheme

    (by.not (by.metaitem "status" "deprecated")) ;; Exclude deprecated series
    (by.not (by.name "test"))                    ;; Exclude test series


Formula-Specific Operators
--------------------------

Formula Detection: ``by.formula``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Finds all computed (formula) series:

.. code-block:: scheme

    ;; Find all formula series
    (by.formula)

    ;; Combine with other conditions
    (by.and (by.formula) (by.metaitem "department" "finance"))    ;; Finance formulas
    (by.and (by.formula) (by.metaitem "type" "kpi"))              ;; KPI formulas

    ;; Find non-formula (stored) series
    (by.not (by.formula))


Formula Content Search: ``by.formulacontents``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   This is in practice quite useful to find a specific formula when we
   don't know its name (or other properties) but we have an idea of
   what it does.

Searches within the actual formula expressions:

.. code-block:: scheme

    ;; Find formulas using specific functions
    (by.formulacontents "resample")          ;; Formulas with resampling
    (by.formulacontents "priority")          ;; Formulas using priority operator

    ;; Find formulas referencing specific series
    (by.formulacontents "stock_price")       ;; Formulas using directly stock_price series

    ;; Advanced operations
    (by.formulacontents "findseries")        ;; Dynamic series discovery formulas
    (by.formulacontents "constant")          ;; Formulas creating constants


Tree and Cache Operators
------------------------

Tree Path Filters
~~~~~~~~~~~~~~~~~

Filter series based on their organization in tree paths:

.. code-block:: scheme

    (by.without-path)           ;; Series not in any tree path
    (by.at-path "/energy/solar") ;; Series at specific path (and children)

Cache Policy Filters
~~~~~~~~~~~~~~~~~~~~

Filter by cache policy (refinery-specific):

.. code-block:: scheme

    (by.cache)                    ;; Has any cache policy
    (by.cachepolicy "daily_cache") ;; Has specific cache policy
