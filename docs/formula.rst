Formulas
==============================================

Purpose
-------

This `tshistory <https://hg.sr.ht/~pythonian/tshistory>`__ component
provides a formula language to build computed series.

Formulas are defined using a simple lisp-like syntax, using a
pre-defined function library.

Formulas are read-only series (you can’t ``update`` or ``replace``
values).

They also have an history, which is built, time stamps wise, using the
union of all constituent time stamps, and value wise, by applying the
formula.

Because of this the ``staircase`` operator is available on formulae.
Some ``staircase`` operations can have a very fast implementation if the
formula obeys commutativity rules.

Operators
---------

General Syntax
~~~~~~~~~~~~~~

Formulas are expressed in a lisp-like syntax using ``operators``,
positional (mandatory) parameters and keyword (optional) parameters.

The general form is:

``(<operator> <param1> ... <paramN> #:<keyword1> <value1> ... #:<keywordN> <valueN>)``

Here are a couple examples:

-  ``(add (series "wallonie") (series "bruxelles") (series "flandres"))``

Here we see the two fundamental ``add`` and ``series`` operators at
work.

This would form a new synthetic series out of three base series (which
can be either raw series or formulas themselves).

Some notes:

-  operator names can contain dashes or arbitrary caracters

-  literal values can be: ``3`` (integer), ``5.2`` (float), ``"hello"``
   (string) and ``#t`` or ``#f`` (true or false)

Pre-defined operators
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: tshistory_formula.funcs
    :noindex:
    :members:
    :exclude-members: aggregate_by_doy, compute_bounds, doy_aggregation, doy_scope_shift_transform, get_boundaries

Registering new operators
-------------------------

This is a fundamental need. Operators are fixed python functions exposed
through a lispy syntax. Applications need a variety of fancy operators.

declaring a new operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One just needs to decorate a python with the ``func`` decorator:

.. code:: python

     from tshistory_formula.registry import func

     @func('identity')
     def identity(series):
         return series

The operator will be known to the outer world by the name given to
``@func``, not the python function name (which can be arbitrary).

This is enough to get a working transformation operator. However
operators built to construct series rather than just transform
pre-existing series are more complicated.

custom series operator
~~~~~~~~~~~~~~~~~~~~~~~

We start with an example, a ``shifted`` operator that gets a series with
shifted from_value_date/to_value_date boundaries by a constant ``delta``
amount.

We would use it like this: ``(shifted "shiftme" #:days -1)``

As we can see the standard ``series`` operator won’t work there, that is
applying a shift operator (``(shift (series "shiftme"))``) *after* the
call to series is too late. The from/to implicit parameters have already
been handled by ``series`` itself and there is nothing left to *shift*.

Hence ``shifted`` must be understood as an alternative to ``series``
itself. Here is a possible implementation:

.. code:: python

     from tshistory_formula.registry import func, finder

     @func('shifted')
     def shifted(__interpreter__, name, days=0):
         args = __interpreter__.getargs.copy()
         fromdate = args.get('from_value_date')
         todate = args.get('to_value_date')
         if fromdate:
             args['from_value_date'] = fromdate + timedelta(days=days)
         if todate:
             args['to_value_date'] = todate + timedelta(days=days)

         return __interpreter__.get(name, args)

     @finder('shifted')
     def find_series(cn, tsh, tree):
         return {
             tree[1]: tsh.metadata(cn, tree[1])
         }

As we can see, we use a new ``finder`` protocol. But first let’s examine
how the ``shiftme`` operator is implemented.

First it takes a special ``__interpreter__`` parameter, which will
receive the formula interpreter object, providing access to an important
internal API of the evaluation process.

Indeed from the interpreter we can read the ``getargs`` attribute, which
contains a dictionary of the actual query mapping. We are specially
interested in the ``from_value_date`` and ``to_value_date`` items in our
example, but all the parameters of ``tshistory.get`` are available
there.

Once we have shifted the from/to value date parameter we again use the
interpreter to make a call to ``get`` which will in turn perform a call
to the underlying ``tshistory.get`` (which, we don’t know in advance,
may yield a primary series or another formula computed series).

Implementing the operator this way, we actually miss two important
pieces of information:

-  the system cannot determine a series is *produced* by the ``shifted``
   operator like it can with ``series``

-  and because of this it cannot know the technical metadata of the
   produced series (e.g. the ``tzaware`` attribute)

This is where the ``finder`` protocol and its decorator function comes
into play. For ``shifted`` we define a finder. It is a function that
takes the db connection (``cn``), time series protocol handler (``tsh``)
and formula syntax tree (``tree``), and must return a mapping from
series name to its metadata.

The tree is an obvious Python data structure representing a use of the
operator in a formula.

For instance because of the ``shifted`` python signature, any use will
be like that:

-  in lisp ``... (shifted "shift-me" #:hours +1) ...`` (the dots
   indicate that it can be part of a larger formula)

-  tree in python: ``['shifted', "shift-me", 'hours', 1]``

The name is always in position 1 in the list. Hence the implementation
of the shifted *finder*:

.. code:: python

         return {
             tree[1]: tsh.metadata(cn, tree[1])
         }

For the metadata we delegate the computation to the underlying series
metadata.

We might want to provide an ad-hoc metadata dictionary if we had a proxy
operator that would forward the series from an external source:

.. code:: python

     @func('proxy')
     def proxy(
             __interpreter__,
             series_uid: str,
             default_start: date,
             default_end : date) -> pd.Series:
         i = __interpreter__
         args = i.getargs.copy()
         from_value_date = args.get('from_value_date') or default_start
         to_value_date = args.get('to_value_date') or default_end

         proxy = ProxyClient()
         return proxy.get(
             series_uid,
             from_value_date,
             to_value_date,
         )

     @finder('proxy')
     def proxy(cn, tsh, tree):
         return {
             tree[1]: {
                 'index_type': 'datetime64[ns]',
                 'tzaware': False,
                 'value_type': 'float64'
             }
         }

Here, because we have no other means to know (and the proxy provides
some useful documentation), we write the metadata ourselves explicitly.

Also note how accessing the ``__interpreter__`` again is used to forward
the query arguments.

Editor Infos
~~~~~~~~~~~~~

The ``tshistory_formula`` package provides a custom callback for the
``editor`` capabilities of
`tshistory_editor <https://hg.sr.ht/~pythonian/tshistory_editor>`__.

A dedicated protocol is available to inform the editor on the way to
decompose/display a formula.

Example of such a function:

.. code:: python

    from tshistory_formula.registry import editor_info

    @editor_info
    def operator_with_series(builder, expr):
        for subexpr in expr[1:]:
            with builder.series_scope(subexpr):
                builder.buildinfo_expr(subexpr)

The exact ways to use the builder will be provided soon.


