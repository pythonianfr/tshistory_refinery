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

To be filled
