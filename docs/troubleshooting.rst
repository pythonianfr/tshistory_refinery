Troubleshooting
===============

This section covers common issues and their solutions when working
with the Timeseries Refinery.

Installation Issues
-------------------

**PostgreSQL Connection Failed**

.. code:: bash

    psycopg.OperationalError: could not connect to server

* Verify PostgreSQL is running: ``sudo systemctl status postgresql``
* Check connection parameters in ``tshistory.cfg``
* Ensure database exists: ``createdb my_time_series``
* Test connection: ``psql -d my_time_series``

**Missing Dependencies**

.. code:: bash

    ImportError: No module named 'tshistory'

* Ensure all dependencies are installed: ``pip install tshistory_refinery --upgrade``
* Check Python version compatibility (>= 3.8)
* For older versions, see the installation guide for specific requirements

Runtime Issues
--------------

**Series Not Found**

* Series returns ``None`` when it doesn't exist
* Verify series exists: ``tsa.exists('my_series')``
* Check for typos in series name
* Use ``tsa.find()`` to find series
* Ensure you're connected to the correct database

**Formula Parsing Errors**

.. code:: bash

    SyntaxError: unbalanced parentheses

* Check formula syntax - parentheses must be balanced
* Verify operator names are correct
* Use the formula editor in the web UI for validation
* Check that referenced series exist

Web Interface Issues
--------------------

**Authentication Failures**

* Verify credentials in ``tshistory.cfg`` under ``[auth]`` section
* For OAuth/OpenID Connect, contact Pythonian for enterprise deployment plans
* Clear browser cache and cookies
* Check server-side authentication logs

Performance Issues
------------------

**Slow Formula Evaluation**

* Enable caching for complex formulas: ``tsa.set_cache_policy('my_formula', 'standard')``
* See :ref:`Formulas: when to use a cache/materialized view` for more details
* Check formula depth: ``tsa.formula_depth('my_formula')``
* Consider breaking complex formulas into simpler components

**High Memory Usage**

* Limit time ranges in queries
* Use streaming operations for large datasets
* Monitor Python process memory usage
* Consider upgrading hardware for large deployments

Migration Issues
----------------

**Migration Issues**

* Always backup database before migration
* Test migrations on staging environment first
* Follow the step-by-step migration guide in the installation documentation
* For complex scenarios, contact Pythonian for support

Getting Help
------------

If you encounter issues not covered here:

1. Check the server logs for detailed error messages
3. Visit the project documentation at https://refinery.docs.pythonian.fr
4. Contact support with:
   * Error message details
   * System configuration
   * Steps to reproduce the issue
   * Refinery version: ``pip show tshistory_refinery``
