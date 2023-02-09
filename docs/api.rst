API documentation
==============================================

Common & Storage
----------------
.. autoclass:: tshistory_refinery.api.mainsource
    :noindex:
    :member-order: bysource
    :members: get, update, replace, exists, insertion_dates, history, catalog, find, metadata, update_metadata

Supervision
-----------
.. autoclass:: tshistory_supervision.api.mainsource
    :noindex:
    :member-order: bysource
    :members: edited, supervision_status

Formulas
--------
.. autoclass:: tshistory_formula.api.mainsource
    :noindex:
    :member-order: bysource
    :members: register_formula, eval_formula, formula, formula_components

Excel
-----
.. autoclass:: tshistory_xl.api.mainsource
    :noindex:
    :member-order: bysource
    :members: values_markers_origins,

Refinery
--------
.. autoclass:: tshistory_refinery.api.mainsource
    :noindex:
    :member-order: bysource
    :members: new_cache_policy, edit_cache_policy, delete_cache_policy, set_cache_policy, unset_cache_policy, cache_free_series, cache_policies, cache_policy_series, has_cache, delete_cache, refresh_series_policy_now


