Supervision
===========

Purpose
-------

This component provides a supervision mechanism over
`tshistory <https://hg.sr.ht/~pythonian/tshistory>`__.

The central use case is as follows:

-  time series from third party providers are stored as is

-  from time to time, a data analyst will want to override (bogus)
   values, and need to know what values were overriden

-  when upstream fix its bogus values the fixed values must override the
   data analyst fixes

-  the complete series history must retain information on the
   supervision activity

Usage
-----

The basic `tshistory <https://hg.sr.ht/~pythonian/tshistory>`__ usage is
described on its own documentation.

An override is made as follows:

.. code:: python

    >>> tsa.update('my-series', series, 'analyst@corp.com', manual=True)

This also works with the ``replace`` api call.

Inserted values will show up in the next ``.get`` call.

Specific API calls exist to provide a standard workflow:

-  ``.edited`` returns a couple *series* and *markers* where *series* is
   like the output of a standard ``.get`` call and *markers* a boolean
   series indicating whether any data point is from *upstream* or a
   manual edition.

The series metadata grows a new *supervision-status* attribute, whose
value can be one of ``unsupervised``, ``supervised`` and
``hand-crafted``.

Hand-crafted is for series that are entirely made of ``manual`` updates.
