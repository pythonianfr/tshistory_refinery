===============
Installation
===============

The different ways to install (pypi, docker)
--------------------------------------------

To be filled

Installing on Linux, Macos, Windows
-----------------------------------

Just pip-install it:

.. code:: bash

   $ pip install tshistory_refinery

Create first a postgresql database:

.. code:: bash

   $ createdb my_time_series

Then initialize the database schema:

.. code:: bash

   $ tsh init-db postgresql:///my_time_series --no-dry-run

Last item: we need a configuration file ``tshistory.cfg`` in the home
directory, containing:

.. code:: ini

   [dburi]
   refinery = postgresql:///my_time_series

From this point you can start a local server and start using the
refinery.

.. code:: bash

   tsh webstart


Upgrading and migrating
-----------------------

Pythonian publishes one or two major releases per year. Also a number
of smaller bug fix releases typically happen. There are two aspects to
the upgrade:

* deploying the new code version

* running the data migration (if there is one)


For a pypi installed instance, upgrading is as simple as doing a:

.. code:: bash

   pip install tshistory_refinery --upgrade


After having done that, you should run the migration, like this:

.. code:: bash

   tsh migrate <instancename>

The migrate command accepts a `--no-interactive` parameter to run it
without confirmation or question.

It can run like this:

.. code:: bash

  (base) aurelien@trantor:~$ tsh migrate refinery --no-interactive
  Running migrations for tshistory.
  Versions: from 0.20.0 to 0.20.0
  Nothing to migrate for `tshistory`, skipping.
  Running migrations for tshistory-supervision.
  Versions: from 0.13.0 to 0.13.0
  Nothing to migrate for `tshistory-supervision`, skipping.
  Running migrations for tshistory-formula.
  Versions: from 0.16.0 to 0.16.2
  Migration plan for `tshistory-formula`: ['0.16.1']
  Running migrations for tshistory-refinery.
  Versions: from 0.9.0 to 0.9.1
  Migration plan for `tshistory-refinery`: ['0.9.1']
  Running migrations for tswatch.
  Versions: from 0.1.0 to 0.3.0
  Migration plan for `tswatch`: ['0.3.0', '0.2.0']
  Running migrations for refinery-pro.
  Versions: from 0.1.0 to 0.1.0
  Nothing to migrate for `refinery-pro`, skipping.


.. warning::

   You should have a dev or preprod environment to test your migration
   against. Always make sure you have tested the migration on this
   environment, with a fresh production database dump, to make sure it
   will work smoothly. The migrations are intensely tested before
   shipping but one is never too careful.
