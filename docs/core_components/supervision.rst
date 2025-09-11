Supervision
===========

The supervision mechanism enables data quality management when working with 
external data sources. As an analyst, you can correct erroneous values while
the system maintains the original data and ensures that provider corrections
automatically flow through.

The supervision workflow
------------------------

Consider a typical scenario: you receive daily market data from a provider.
Sometimes this data contains errors that need immediate correction for your
reports. The supervision system allows you to:

1. Apply manual corrections to fix errors immediately
2. Continue receiving updates from the provider
3. Have provider corrections automatically replace your manual fixes
4. Maintain full audit trail of all changes

Making manual corrections
-------------------------

Manual corrections can be applied in three ways:

.. code:: python

    >>> corrected_values = pd.Series([95.5, 96.0], 
    ...                              index=[pd.Timestamp('2023-03-15'),
    ...                                     pd.Timestamp('2023-03-16')])
    >>> tsa.update('market-prices', corrected_values, 'analyst@corp.com', manual=True)

The same manual corrections can be made through the web interface (tsview) or 
the Excel client - both automatically use manual=True when you edit values.

Your corrections appear immediately in the data. When the provider later sends
corrected data through the normal update process, their fixes automatically 
replace your manual corrections.

Viewing data provenance
-----------------------

The ``.edited`` method shows you which values are original versus manually edited:

.. code:: python

    >>> series, markers = tsa.edited('market-prices')

The returned series contains the current values, while markers is True where 
data was manually edited and False for provider data.

Understanding supervision status
--------------------------------

Every series has a supervision status in its metadata:

- ``unsupervised``: series updated only through normal updates (never with manual=True)
- ``hand-crafted``: series created and maintained entirely with manual updates (always with manual=True, whether from Python, Excel client, or web UI)
- ``supervised``: series containing both provider updates and manual corrections

This helps you quickly identify which series have manual interventions.

Supervision API Reference
-------------------------

.. autoclass:: tshistory_supervision.api.mainsource
    :noindex:
    :member-order: bysource
    :members: edited, supervision_status
