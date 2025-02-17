===============
Installation
===============

The different ways to install (pypi, docker)
--------------------------------------------

The installation can be realised by two means:

* directly from pypi (the public one, where one finds a stable - if
  old - version of the refinery, or the private ones at
  `pypi.pythonian.fr` or `pypi-pro.pythonian.fr`, where we make the
  latest fixes and major versions available to subscribers)

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
   wsgi container such as `gunicorn` or `waitress`.

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


Installing older versions, migrating from older versions
--------------------------------------------------------

We will herein document installation and migration starting from the
`0.8.3` release, which has been available on PyPi since
september 2023.

If one wants to install this old release today (this is written in
february 2025), this will fail unless:

* the Python version is 3.9 (pandas 1.5 is still available in binary
  form for this version, for later versions, a compilation from source
  will happen, which often ends badly)

* some dependencies, direct and indirect, must be corrected (as they
  have evolved to newer, incompatible versions)

You probably want to create a `requirements.txt` file and put this
inside:

```
numpy==1.26.4
werkzeug==2.3.8
tshistory_refinery==0.8.3
tshistory==0.19.4
```

You then need to do:

```shell
$ pip install -r requirements.txt
```

Let's assume you want to upgrade from this to newer versions. We will
describe the steps to go from 0.8.3 to 0.9.0, then 0.9.1, then 0.9.2,
and lastly to the "pro" version.

.. warning::

   It is possible in theory to jump several versions at once. However
   due to bugs in the earlier versions of the migration system (up to
   0.9.1) it is advised to perform it step-wise.

So to go from `0.8.3` to `0.9.0` one should:

* adjust the `pip install` stanza with a `-i
  https://<login>:<passwd>@pypi.pythonian.fr/simple`

* also populate a requirements.txt with the following:

```
tshistory_refinery==0.9.0
tshistory==0.20.0
tsview==0.19.0
tshistory_supervision==0.13.0
tshistory_formula==0.16.1
tshistory_xl==0.8.0
rework==0.17.0
rework_ui==0.16.0
```

Lastly:

```shell
$ pip install -r requirements.txt
```

For `0.9.0` to `0.9.1`:

```
tshistory_refinery==0.9.1
tshistory==0.20.1
tsview==0.20.1
tshistory_supervision==0.13.1
tshistory_formula==0.16.2
tshistory_xl==0.8.0
rework==0.17.2
rework_ui==0.16.0
```

For `0.9.1` to `0.9.2`:

```shell
$ pip install tshistory_refinery==0.9.2 --upgrade
```

To get the `Pro` version, from `0.9.2`:

```shell
$ pip install refinery_pro==0.2.0
$ tswatch init-db
$ tsh migrate <refinery-name> --no-interactive
```
