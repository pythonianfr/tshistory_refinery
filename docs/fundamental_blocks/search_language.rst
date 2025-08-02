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
- `Advanced Patterns`_
- `Best Practices`_

Quick Start
-----------

Your First Search
~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Find all series containing "temperature" in their name
    results = tsa.find('(by.name "temperature")')

    # Find series by metadata
    results = tsa.find('(by.metaitem "sensor_type" "humidity")')

    # Combine conditions
    results = tsa.find('(by.and (by.name "daily") (by.metaitem "region" "europe"))')

Basic Syntax Rules
~~~~~~~~~~~~~~~~~~

1. **Parentheses**: Every query is wrapped in parentheses ``(operator args...)``
2. **Quotes**: All string values use double quotes ``"value"``
3. **Nesting**: Queries can be nested for complex logic
4. **Case Sensitive**: All operators and values are case-sensitive

Query Syntax
------------

Structure
~~~~~~~~~

.. code-block:: scheme

    ;; Basic structure
    '(operator "argument")'

    ;; With multiple arguments
    '(operator "arg1" "arg2")'

    ;; Nested queries
    '(by.and (by.name "sales") (by.metaitem "region" "north"))'


Basic Search Operators
----------------------

Name-based Search: ``by.name``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Searches for series whose names contain the specified substring
(case-insensitive):

.. code-block:: scheme

    ;; Basic name search
    '(by.name "temperature")'  ;; Matches: "room_temperature", "temperature_sensor"


Metadata Key Search: ``by.metakey``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Finds series that have a specific metadata key (regardless of the
value):

.. code-block:: scheme

    ;; Basic metadata key search
    '(by.metakey "region")'    ;; All series that have a "region" metadata field

Metadata Value Search: ``by.metaitem``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Finds series where a specific metadata key has a specific value. The
point of examples here is also to illustrate use of the series
matadata:

.. code-block:: scheme

    ;; Basic metadata value search
    '(by.metaitem "region" "europe")'          ;; Series in Europe region
    '(by.metaitem "data_type" "financial")'    ;; Financial data

    ;; Quality and status
    '(by.metaitem "quality" "high")'           ;; High quality data
    '(by.metaitem "confidence" "reliable")'    ;; Reliable data


Value Comparison Search: ``by.value``
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

    ;; Pure search filters (for use with ``find``)
    '(> "temperature" 25)'          ;; Temperature metadata > 25
    '(>= "confidence" 0.8)'         ;; Confidence >= 80%
    '(< "threshold" 100)'           ;; Threshold < 100

    ;; In the formula language  (for use with ``findseries``)
    '(by.value "temperature" ">" 25)'     ;; Temperature metadata > 25
    '(by.value "confidence" ">=" 0.8)'    ;; Confidence >= 80%
    '(by.value "threshold" "<" 100)'      ;; Threshold < 100


Logical Operators
-----------------

AND Operator: ``by.and``
~~~~~~~~~~~~~~~~~~~~~~~~~

Combines multiple conditions where ALL must be true:

.. code-block:: scheme

    '(by.and (by.name "temperature") (by.metaitem "region" "europe"))'


OR Operator: ``by.or``
~~~~~~~~~~~~~~~~~~~~~~

Combines multiple conditions where ANY can be true:

.. code-block:: scheme

    '(by.or (by.metaitem "region" "europe") (by.metaitem "region" "asia"))'

    ;; Multiple regions
    '(by.or (by.metaitem "country" "france")
            (by.metaitem "country" "germany")
            (by.metaitem "country" "italy"))'


NOT Operator: ``by.not``
~~~~~~~~~~~~~~~~~~~~~~~~

Excludes series matching the specified condition:

.. code-block:: scheme

    '(by.not (by.metaitem "status" "deprecated"))' ;; Exclude deprecated series
    '(by.not (by.name "test"))'                    ;; Exclude test series


Formula-Specific Operators
--------------------------

Formula Detection: ``by.formula``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Finds all computed (formula) series:

.. code-block:: scheme

    ;; Find all formula series
    '(by.formula)'

    ;; Combine with other conditions
    '(by.and (by.formula) (by.metaitem "department" "finance"))'    # Finance formulas
    '(by.and (by.formula) (by.metaitem "type" "kpi"))'              # KPI formulas

    # Find non-formula (stored) series
    '(by.not (by.formula))'


Formula Content Search: ``by.formulacontents``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   This is in practice quite useful to find a specific formula when we
   don't know its name (or other properties) but we have an idea of
   what it does.

Searches within the actual formula expressions:

.. code-block:: scheme

    ;; Find formulas using specific functions
    '(by.formulacontents "resample")'          ;; Formulas with resampling
    '(by.formulacontents "priority")'          ;; Formulas using priority operator

    ;; Find formulas referencing specific series
    '(by.formulacontents "stock_price")'       ;; Formulas using directly stock_price series

    ;; Advanced operations
    '(by.formulacontents "findseries")'        ;; Dynamic series discovery formulas
    '(by.formulacontents "constant")'          ;; Formulas creating constants


Advanced Patterns
-----------------

Complex Business Logic
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Find series for quarterly business review
    '(by.and (by.metaitem "frequency" "quarterly")
             (by.or (by.metaitem "category" "revenue")
                    (by.metaitem "category" "costs")
                    (by.metaitem "category" "kpi"))
             (by.metaitem "department" "finance")
             (by.not (by.metaitem "status" "draft")))'

    # Find anomaly detection candidates
    '(by.and (by.metaitem "frequency" "daily")
             (by.value "volatility" ">" 0.1)
             (by.metaitem "business_critical" "true")
             (by.not (by.metaitem "anomaly_detection" "enabled")))'

    # Find series for machine learning pipeline
    '(by.and (by.value "data_points" ">" 1000)
             (by.value "completeness" ">" 0.95)
             (by.metaitem "data_type" "numerical")
             (by.not (by.metaitem "contains_pii" "true"))
             (by.metaitem "ml_approved" "true"))'


Best Practices
--------------

Error Handling
~~~~~~~~~~~~~~

.. code-block:: python

    # Always handle empty results
    results = tsa.find('(by.metaitem "nonexistent" "value")')
    if not results:
        print("No series found matching criteria")

    # Validate query syntax before using
    try:
        results = tsa.find('(by.and (by.name "test"))')  # Missing closing paren in complex query
    except SyntaxError as e:
        print(f"Query syntax error: {e}")

Testing Queries
~~~~~~~~~~~~~~~

.. code-block:: python

    # Start with simple queries and build complexity
    # Step 1: Test basic condition
    basic_results = tsa.find('(by.metaitem "type" "sensor")')

    # Step 2: Add location filter
    location_results = tsa.find('(by.and (by.metaitem "type" "sensor")
                                         (by.metaitem "location" "building_a"))')

    # Step 3: Add status filter
    final_results = tsa.find('(by.and (by.metaitem "type" "sensor")
                                      (by.metaitem "location" "building_a")
                                      (by.metaitem "status" "active"))')

    # Compare result counts to ensure filters work as expected
    print(f"All sensors: {len(basic_results)}")
    print(f"Building A sensors: {len(location_results)}")
    print(f"Active Building A sensors: {len(final_results)}")

This comprehensive guide covers all aspects of the Refinery search
query language. Use these patterns as building blocks to create
sophisticated queries for data discovery and analysis.
