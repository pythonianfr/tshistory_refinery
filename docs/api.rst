API documentation
==============================================

All the api points are available through the `timeseries` object. As in:

.. highlight:: python
.. code-block:: python

 from tshistory.api import timeseries
 tsa = timeseries('http://refinery.datascience.com')
 ts = tsa.get('banana-spot-price')

The available methods are the same and behave the same wether you use
an http uri or a direct postgres uri.

The methods description below appear to belong to the `mainsource`
object, which talks directly to postgres. This is an unimportant
implementation detail.


Base Series Operations
----------------------

This constitutes the fundamental API to deal with series on an
individual basis.

.. autoclass:: tshistory.api.mainsource
    :noindex:
    :member-order: bysource
    :members: get, update, replace, exists, type, source, delete, rename, interval, metadata, internal_metadata, update_metadata, replace_metadata, insertion_dates, strip, log, history, staircase, block_staircase


Operations on series sets
-------------------------

These methods permit to enumerate all know series, find them using
sophisticated search criteria (by name, metadata key/value, source).

.. autoclass:: tshistory.api.mainsource
    :noindex:
    :member-order: bysource
    :members: find, register_basket, basket, basket_definition, list_baskets, delete_basket, catalog


Supervision
-----------

The supervision feature exposes two API points for stored series.

.. autoclass:: tshistory_supervision.api.mainsource
    :noindex:
    :member-order: bysource
    :members: edited, supervision_status


Formulas
--------

The formulas adds computed series to the system ; most previously seen
API points work with them. What does not: update and replace
(obviously, since formula are by construction a read-only) feature. In
the future it is possible that these methods will be implemented with
*override* semantics.

.. autoclass:: tshistory_formula.api.mainsource
    :noindex:
    :member-order: bysource
    :members: register_formula, eval_formula, formula, formula_depth, formula_components


..
   Excel
   -----

   The API points listed there are mostly for use by the Excel client.

   .. autoclass:: tshistory_xl.api.mainsource
       :noindex:
       :member-order: bysource
       :members: values_markers_origins,


Formula cache
-------------

The formula system allows to grow *very complicated* computed series
(by building them bottom-up), which are by default computed on the
fly. The downside can be sluggish performance as complex formulas read
hundreds of base series and does computations on them. Hence it can be
useful to put them into a "cache".

.. autoclass:: tshistory_refinery.api.mainsource
    :noindex:
    :member-order: bysource
    :members: new_cache_policy, edit_cache_policy, delete_cache_policy, set_cache_policy, unset_cache_policy, cache_free_series, cache_policies, cache_policy_series, has_cache, delete_cache, refresh_series_policy_now
