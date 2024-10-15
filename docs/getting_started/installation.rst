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

To be filled
