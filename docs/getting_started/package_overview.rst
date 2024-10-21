Package overview
================

Purpose
-------

The "timeseries refinery" is a powerful tool to build complete time
series information systems.

It provides:

* a **versioned** time series (compact) storage system with a simple
  yet powerful API (direct in python + REST)

* an edition/supervision workflow for experts

* **computed** versioned time series using a simple yet expressive
  dedicated **formula language** (think about Excel formulas, but
  simpler and specialised for time series) with a simple API

* a cache system for the computed series

* a **central repository** of time series (stored or computed)

* rich **metadata** support and **querying/filtering** over various
  kinds of metadata

* support for **timezone aware** and naive time series

* a **data-mesh** distributed architecture allowing to connect to
  other refineries (for **collaborative work**) or other time series
  silos

* a task manager and a mini-framework to handle regular **ingestion**
  of time series from external or internal sources, and also **model
  runs**

* a tool to **detect/watch** the series lagging behind (because of
  failing sources)

* a powerful **low-code declarative dashboard** system (specially
  targetted at energy comodity markets)

* a **rich web user interface** allowing exploration of the catalog,
  visualisation and edition of series, formulas, caches, tasks,
  watchers and dashboards

It can be used as a building block for machine learning, model
optimization and validation, both for inputs and outputs.


Usages
------

The time series repository can be used from the user interface. It has
also been prominently designed with a data analyst friendly Python
level API (in addition to the standard REST API).

The refinery can be used:

* as a standalone tool

* as a framework to build your own applications on top

* as a library that you can embed in your (Python) applications

It provides several extension points through a plugin system. The API
can be extended, the formula system can receive new custom
operators/functions.

It has been designed to cooperate nicely within any existing IT
infrastructure addressing time series data needs.

Its primary purposes and design goals are:

* to give maximum **autonomy** to non-programmers working with time
  series data (analysts, data scientists), without having to beg the
  IT department

* to give very **user-friendly APIs** to manipulate data and
  computations

* to provide an extensive **audit trail** from the acquired external
  data to the final dashboard product (it makes it easy to understand
  the provenance of all the data)

* to foster good **data governance** and **collaboration** over the
  time series repository

* all that while providing an **efficient storage** system and
  **horizontal scalability**



License
-------

.. code:: RST

   The Timeseries Refinery - Timeseries management tool
   Copyright (C) 2024  Pythonian

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

`To know more on LGPL license, click here. <https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html>`_
