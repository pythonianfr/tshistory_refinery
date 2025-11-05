Best Practices
==============

This guide provides best practices for working effectively with the
Timeseries Refinery, based on production experience and proven
patterns.

Naming Conventions
------------------

**Series Naming Guidelines**

Follow a consistent hierarchical structure using dots as separators:

.. code:: text

    domain.category.subcategory.source.location.unit.frequency

Examples:

* ``energy.electricity.price.spot.france.eur_mwh.h`` - Hourly French electricity spot prices
* ``weather.temperature.air.meteo_france.paris.celsius.d`` - Daily temperature in Paris
* ``finance.fx.rate.ecb.eur_usd.rate.d`` - Daily EUR/USD exchange rate

**Guidelines:**

* Use lowercase letters and underscores for multi-word components
* Keep names descriptive but concise (aim for 6-8 components maximum)
* Place most general categories first, most specific last
* Include units and frequency when relevant
* Avoid abbreviations unless they are standard in your domain

**Metadata Naming Standards**

Use consistent metadata keys across your organization:

**Standard Keys:**

* ``source`` - Data provider or system of origin
* ``unit`` - Measurement unit
* ``frequency`` - Native data frequency
* ``geography`` - Geographic scope or location
* ``category`` - Business domain classification
* ``quality`` - Data quality indicators
* ``contact`` - Responsible person or team

**Guidelines:**

* Use snake_case for metadata keys
* Prefer established vocabularies when possible
* Document your metadata schema
* Keep values consistent (use controlled vocabularies)
* **Metadata keys should remain stable** - avoid frequently changing metadata values
* Use ``tsa.insertion_dates()`` to get timing information instead of storing it in metadata
* Per update metadata can be provided: ``tsa.update(name, series,
  author, metadata={...})`` to document what is going on in a specific
  revision

Data Governance
---------------

**Team Collaboration Guidelines**

**Establish Clear Ownership:**

* Assign data stewards for each domain or category

* Use the ``contact`` metadata field: ``tsa.update_metadata('series',
  {'contact': 'energy.team@company.com'})``
* Find series by owner: ``tsa.find('(by.metaitem "contact" "energy.team")')``
* Use supervision with ``tsa.update('series', data, 'author', manual=True)``
  to track manual interventions

**Communication Protocols:**

* All formula changes, updates, and metadata modifications are automatically logged
* Use ``tsa.history('series', diffmode=True)`` (sparingly though, it
  is an expensive api point) to see what changed between versions
* Set up ``tswatch`` alerts for critical series that stop updating
* Use the web UI's series browser to explore dependencies before changes

**Change Management Process**

**Before Making Changes:**

* Test formulas with ``tsa.eval_formula('(+ (series "a") (series "b"))')`` before registering
* Check ``tsa.formula_depth('complex_formula')`` to understand computational complexity
* Use the formula editor in the web UI for validation and testing

**Implementation:**

* Formula registration is automatically versioned: ``tsa.register_formula('name', 'new_formula')``
* Use cache policies for performance - see :ref:`getting_started/tutorials/advanced:Formulas: when to use a cache/materialized view`
* Leverage the rework task system for scheduled updates - see :ref:`getting_started/tutorials/advanced:Tasks system: how to organize and schedule tasks`
* Use the mini scraping framework to link scrapers to tasks and series (see ``scrap.py`` and ``refresh`` task)

**After Changes:**

* Use ``tsa.get('series', revision_date=timestamp)`` to compare before/after states
* Update dashboard configurations if series structure changed
* Monitor cache performance and policies

**Data Quality Standards**

**Validation Using Refinery Features:**

* Use ``tsa.supervision_status('series')`` to check if manual overrides exist
* Implement quality checks in rework tasks that run on schedule
* Use ``tsa.edited('series')`` to identify series with manual interventions
* Store quality indicators in metadata: ``{'quality': 'validated', 'source': 'verified'}``

**Audit Trail Management:**

* Every ``tsa.update()``, ``tsa.update_metadata()``, and ``tsa.replace_metadata()`` is automatically logged
* Use ``tsa.log('series')`` to see change history (and per-update metadata)
* Supervision via ``manual=True`` maintains audit trail for corrections
* ``tsa.insertion_dates('series')`` shows when data was added to the system

**Data Lineage:**

* Use ``tsa.formula('computed_series')`` to see formula definition
* ``tsa.source('series')`` identifies the database source
* Formula dependencies are tracked automatically
* Web UI provides visual dependency graphs for complex formulas

Formula Development
-------------------

**Data Medallion Architecture for Formulas**

**Bronze Layer - Raw Ingestion:**

* Direct from sources: ``energy.prices.nordpool.raw``, ``weather.meteo.paris.raw``
* No processing, preserve original structure and timestamps

**Silver Layer - Cleaned and Standardized:**

* Handle data quality issues here: missing values, duplicates, basic validation
* Resampling to standard frequencies happens here,
  example: ``(resample (series "energy.prices.raw") "H")``
* Standardized units, timezone-aware, validated
* Outlier removal, gap filling
* Business rule application: ``(slice ... #:from (date "2020-01-01"))`` for data quality cutoffs

**Gold Layer - Business Logic:**

* ``energy.daily_average_price`` - business KPIs and aggregations, ML model inputs
* ``energy.price_forecast`` - ML model outputs
* ``trading.settlement_prices`` - complex business calculations
* Cross-domain joins and enrichment

**Platinum Layer - Presentation:**

* ``dashboard.energy.price_summary`` - optimized for specific dashboards
* ``api.energy.latest_prices`` - formatted for external APIs
* User-specific views and permissions

**Formula Composition Strategies by Layer**

**Bronze → Silver Transformations:**

* Focus on data quality
* Standardization
* Basic gap filling
* Resampling for stable time granularities

**Silver → Gold Business Logic:**

* Domain calculations: ``(/ (+ (series "clean.demand") (series "clean.losses")) (series "clean.capacity"))``
* Aggregations (by geography or other domains) at different levels
* Cross-referencing: ``(priority (series "validated") (series "estimated"))``

**Gold → Platinum Optimization:**

* Performance caching for heavy calculations
* User-specific filters and permissions
* Dashboard-optimized time ranges and granularity

**Production Architecture Patterns**

**The "Source of Truth" Pattern:**

* Each business concept has ONE gold-layer source of truth
* All downstream uses reference this canonical series
* Example: ``computed.energy.official_price`` used by all trading, reporting, and billing systems

**The "Temporal Consistency" Pattern:**

* Maintain consistent time horizons across related series
* ``computed.energy.rolling_30d_average`` and ``computed.energy.rolling_30d_volatility``
* Use shared time windows: ``(rolling ... #:window "30D" #:center False)``

**The "Lineage Preservation" Pattern:**

* Embed source attribution in formula names
* ``computed.energy.price.from_nordpool_entsoe`` vs ``computed.energy.price.from_local_market``
* Completes tracking data provenance through the medallion layers

**Anti-Patterns from Production Experience**

**The "Layer Bypass" Anti-Pattern:**

* Gold formulas directly reading raw data: ``(series "messy_data.raw")``
* Skips cleaning and validation, leads to hazardous results
* Always flow through the medallion: raw → clean → computed

**The "Mixed-Layer Formula" Anti-Pattern:**

* One formula mixing concerns: cleaning + business logic + presentation formatting
* Makes debugging and maintenance difficult
* Keep each formula focused on one medallion layer's responsibilities
