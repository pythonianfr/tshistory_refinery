Formulas
==============================================

Purpose
-------

This `tshistory <https://hg.sr.ht/~pythonian/tshistory>`__ component
provides a formula (time series domain specific) language to build
computed time series.

Formulas are read-only series (you can’t ``update`` or ``replace``
them).

They also have versions and an history, which is built, time stamps
wise, using the union of all constituent time stamps, and value wise,
by applying the formula.

Because of this the ``staircase`` operator is available on formulae.
Some ``staircase`` operations can have a fast implementation if the
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

- ``(add (series "wallonie") (series "bruxelles") (series "flandres"))``


Here we see the two fundamental ``add`` and ``series`` operators at
work.

This would form a new synthetic series out of three base series (which
can be either raw series or formulas themselves).

- ``(round (series "foo") #:decimals 2)``

This illustrates the keywords.

Some notes:

-  operator names can contain dashes or arbitrary caracters

-  literal values can be: ``3`` (integer), ``5.2`` (float), ``"hello"``
   (string), ``#t`` or ``#f`` (true or false).


Registering new operators
-------------------------

This is a fundamental need. Operators are fixed python functions
exposed through a lispy syntax. Applications need a variety of fancy
operators.


Declaring a new operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One just needs to decorate a python function with the ``func`` decorator:

.. code:: python

     from tshistory_formula.registry import func

     @func('identity')
     def identity(series: pd.Series) -> pd.Series:
         return series

The operator will be known to the outer world by the name given to
``@func``, not the python function name (which can be arbitrary).

You *must* provide correct type annotations : the formula language is
statically typed and the typechecker will refuse to work with an
untyped operator.

This is enough to get a working *transformation* operator. However
operators built to construct series rather than just transform
pre-existing series are more complicated.


Autotrophic series operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We start with an example, a ``proxy`` operator that gets a series from
an existing time series silo (on the fly) to be served as it came from
your local installation.

We would use it like this: ``(proxy "a-name" #:parameter 42.3)``

As we can see it can look like the ``series`` operator, though its
signature might be more complicated (this will be entirely dependent
on the way to enumerate series in the silo).

Hence ``proxy`` must be understood as an alternative to ``series``
itself. Here is how the initial part would look:

.. code:: python

     from tshistory_formula.registry import func, finder, metadata, history, insertion_dates

     @func('proxy', auto=True)
     def proxy(__interpreter__,
               __from_value_date__,
               __to_value_date__,
               __revision_date__,
               name: str,
               parameter=0):

         # we assume there is some python client available
         # for the tier timeseries silo
         return silo_client.get(
             fromdate=__from_value_date__,
             todate=__to_value_date__,
             revdate=__revision_date__
         )

This is a possible implementation of the API `get` protocol.

Ths dunder methods are a mandatory part of the signature. The other
parameters (positional or keyword) are at your convenience and will be
exposed to the formula users.

We must also provide an helper for the formula system to detect the
presence of this particular kind of operator in a formula (because it
is not like other mere *transformation* operators).

Let's have it:

.. code:: python

     @finder('proxy')
     def proxy_finder(cn, tsh, tree):
         return {
             tree[1]: tree
         }

Let us explain the parameters:

* `cn` is a reference to the current database connection

* `tsh` is a reference to the internal API implementation object (and
  you will need the `cn` object to use it)

* `tree` is a representation of the formula restricted to the proxy
  operator use

When implementing a proxy-like operator, one generally won't need the
first two items. But here is an example of what the *tree* would look
like:

.. code:: python

   ['proxy, 'a-name', '#:parameter, 77]

Yes, the half-quoted `'proxy` and `'#:parameters` are not typos. These are
respectively a:

* symbol (simimlar to a variable name in Python)

* keyword (similar to a Python keyword)

In the finder return dictionary, only the key of the dictionary is
important: it should be globally unique and will be used to provide an
(internal) alias for the provided series name. For instance, in our
example, if `parameter` has an impact on the returned series identity,
it should be part of the key. Like this:

.. code:: python

     @finder('proxy')
     def proxy_finder(cn, tsh, tree):
         return {
             f'tree[1]-tree[2]': tree
         }

We also have to map the `metadata`, `insertion_dates` and the
`history` API methods.

.. code:: python

    @metadata('proxy')
    def proxy_metadata(cn, tsh, tree):
        return {
            f'proxy:{tree[1]}-{tree[2]}': {
                'tzaware': True,
                'source': 'silo-proxy',
                'index_type': 'datetime64[ns, UTC]',
                'value_type': 'float64',
                'index_dtype': '|M8[ns]',
                'value_dtype': '<f8'
            }
        }

.. code:: python

    @history('proxy')
    def proxy_history(__interpreter__,
                      from_value_date=None,
                      to_value_date=None,
                      from_insertion_date=None,
                      to_insertion_date=None):
        # write the implementation there :)


    @insertion_dates('proxy')
    def proxy_idates(__interpreter__,
                     from_value_date=None,
                     to_value_date=None,
                     from_insertion_date=None,
                     to_insertion_date=None):
        # write the implementation there :)



Pre-defined operators
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: tshistory_formula.funcs
    :noindex:
    :members:
    :exclude-members: aggregate_by_doy, compute_bounds, doy_aggregation, doy_scope_shift_transform, get_boundaries, find_last_values, linear_insert_date, resample_adjust, resample_transform
