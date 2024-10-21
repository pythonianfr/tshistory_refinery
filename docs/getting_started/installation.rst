===============
Installation
===============

The different ways to install (pypi, docker)
--------------------------------------------

The installation can be realised by two means:

* directly from pypi (the public one, where one finds a stable - if
  old - version of the refinery, or the private one at
  `pypi.pythonian.fr`, where we make the latest fixes and major
  versions available to subscribers)

* from docker images (available to subscribers)


To get a fresh installation of the Refinery on your Python
environment, just type:

.. code:: bash

   pip install tshistory_refinery

For an upgrade:

.. code:: bash

   pip install tshistory_refinery --upgrade


To get the code from pypi.pythonian.fr, just do a:

.. code:: bash

   pip install tshistory_refinery --index-url https://pypi.pythonian.fr



Installing on Linux, Macos, Windows
-----------------------------------

Once pip-installed, you need also a working Postgres database.

On Linux, getting postgres can be made using
https://www.postgresql.org/download/linux/. On MacOs, doing a:

.. code:: bash

   brew install postgresql

For Windows: get it from https://www.postgresql.org/download/windows/.

Once installed and configured, you can create first a postgresql
database:

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

.. note::

   The `tsh webstart` command is suitable for a local development
   environement. For production purposes you probably want to use a
   wsgi container such as `gunicorn`.

   The `app` object used by wsgi containers can be found at
   `tshistory_refinery.wsgi:app`.


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
