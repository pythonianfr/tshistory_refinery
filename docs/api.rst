API documentation
==============================================

Common & Storage
----------------
.. autoclass:: tshistory_refinery.api.mainsource
    :noindex:
    :members: get, update, replace, exists, insertion_dates, history, catalog, find, metadata, update_metadata

Supervision
-----------
.. autoclass:: tshistory_supervision.api.mainsource
    :noindex:
    :members: edited, supervision_status

Formulas
--------
.. autoclass:: tshistory_formula.api.mainsource
    :noindex:
    :members: register_formula, eval_formula, formula, formula_components

xl
---
.. autoclass:: tshistory_xl.api.mainsource
    :noindex:
    :members: values_markers_origins,

