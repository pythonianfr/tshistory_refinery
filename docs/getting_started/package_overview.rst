Package overview
================

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

TO BE ADDED : Timeseries versioned, tzawareness, naming convention, federation

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
