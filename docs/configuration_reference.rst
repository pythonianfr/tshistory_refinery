Configuration Reference
========================

This reference guide covers all configuration options available in the
``tshistory.cfg`` configuration file.

Configuration File Locations and Discovery
-------------------------------------------

**Configuration File Search Order**

The tshistory configuration system searches for ``tshistory.cfg`` in
the following order:

1. **Environment Variable Path**: ``$TSHISTORYCFGPATH`` - if set and file exists
2. **Current Directory**: ``./tshistory.cfg``
3. **Home Directory**: ``~/tshistory.cfg``
4. **XDG Config Directory**: ``$XDG_CONFIG_HOME/tshistory.cfg`` (defaults to ``~/.config/tshistory.cfg``)

**Environment Variables:**

* ``TSHISTORYCFGPATH`` - Override default configuration file path
* ``XDG_CONFIG_HOME`` - XDG base directory specification (defaults to ``~/.config``)

**File Format:**

* INI-style configuration file
* Case-sensitive section and key names
* Comments start with ``#``


Required Sections
-----------------

[dburi] Section - Database Connections
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``[dburi]`` section is mandatory and defines database connections
for your refinery instances.

**Basic Format:**

.. code:: ini

   [dburi]
   instance_name = database_uri

**Supported URI Formats:**

* **PostgreSQL**: ``postgresql:///database_name`` (local connection)
* **PostgreSQL with credentials**: ``postgresql://user:password@host:port/database``
* **HTTP API**: ``https://refinery.example.com/api`` (remote refinery instance)

**Examples:**

.. code:: ini

   [dburi]
   # Local PostgreSQL database
   refinery = postgresql:///refinery

   # PostgreSQL with full connection details
   test_instance = postgresql://user:password@db.company.com:5432/refinery_dev

   # Remote refinery instance via HTTP API
   external = https://refinery.partner.com/api

   # Multiple instances
   local = postgresql:///refinery_local
   staging = postgresql:///refinery_staging
   prod = postgresql:///refinery_prod

**API Usage:**

* **Default connection**: ``tsa = timeseries()`` uses the first entry in the ``[dburi]`` section
* **Direct URI**: ``tsa = timeseries('postgresql:///mydb')`` bypasses configuration

**Production vs Development:**

* **Production deployments** typically use a single database URI
* **Multiple entries** are primarily useful for developers and testers who need to work with different environments
* The first entry serves as the default when no specific instance is requested

Optional Sections
-----------------

[sources] Section - Data Mesh Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``[sources]`` section configures external refinery instances for
data mesh setups, allowing your refinery to consume data from other
refineries.

**Format:**

.. code:: ini

   [sources]
   local_instance.source_name = remote_uri, namespace

**Parameters:**

* ``local_instance`` - Must match a name from the ``[dburi]`` section
* ``source_name`` - Local alias for the remote source
* ``remote_uri`` - HTTP API endpoint of the remote refinery
* ``namespace`` - Always use ``tsh`` for refinery sources

**Examples:**

.. code:: ini

   [sources]
   # Connect to external weather data
   refinery.meteo = https://refinery.meteo.company.com/api, tsh

   # Multiple external sources
   refinery.weather = https://refinery.weather.partner.com/api, tsh
   refinery.market = https://refinery.market.partner.com/api, tsh

**Usage:**

* External series become available as if they were local
* Transparent access through normal API calls
* Requires proper authentication (see ``[auth]`` section)


[auth] Section - Authentication for External Sources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``[auth]`` section provides authentication credentials for
accessing external refinery instances defined in the ``[sources]``
section.

**Authentication Methods:**

**HTTP Basic Authentication:**

.. code:: ini

   [auth]
   source_name.uri = https://external.refinery.com/api
   source_name.login = username
   source_name.password = password

**OpenID Connect - Client Credentials (M2M):**

.. code:: ini

   [auth]
   source_name.uri = https://external.refinery.com/api
   source_name.client_id = your_client_id
   source_name.client_secret = your_client_secret
   source_name.domain = openid.provider.com/realms/realm_name

**OpenID Connect - PKCE (Public Client):**

.. code:: ini

   [auth]
   source_name.uri = https://external.refinery.com/api
   source_name.pkce =
   source_name.client_id = public_client_id
   source_name.domain = openid.provider.com/realms/realm_name

**Real-World Examples:**

.. code:: ini

   [auth]
   # Basic authentication
   meteo.uri = https://refinery.meteo.pythonian.fr/api
   meteo.login = username
   meteo.password = password

   # Machine-to-Machine OAuth
   partner_api.uri = https://refinery.partner.pythonian.fr/api
   partner_api.client_id = partner_client
   partner_api.client_secret = your_client_secret
   partner_api.domain = openid.pythonian.space/realms/pythonian

   # PKCE flow (public client)
   public_api.uri = https://refinery.public.pythonian.fr/api
   public_api.pkce =
   public_api.client_id = public-client
   public_api.domain = openid.pythonian.space/realms/pythonian

**Key Points:**

* Authentication is required for most external refinery instances
* Basic auth is simpler but less secure than OpenID Connect
* PKCE flow is for public clients (no client secret)
* M2M (client credentials) flow is for server-to-server communication
* ``[auth]`` entries must correspond to sources defined in ``[sources]`` or ``[dburi]``
* ``[auth]`` is for authenticating as a **client** to external services

[server-auth] Section - Server Authentication Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``[server-auth]`` section configures authentication for the
refinery server itself (when acting as a server rather than client).

**Format:**

.. code:: ini

   [server-auth]
   client_id = your_server_client_id
   client_secret = your_server_client_secret
   domain = openid.provider.com/realms/realm_name
   authorize_uri = https://your.refinery.com/authorize
   audience = https://your.refinery.com/api

**Usage:**

* Used when your refinery instance needs to authenticate incoming requests
* Configures your refinery instance as an OAuth2 **server**
* Used when other clients need to authenticate to access your refinery
* Different from ``[auth]`` which is for authenticating to external services

.. note::
   For detailed server authentication setup, see the :ref:`security` chapter.

[dashboard] Section - External Dashboard URL Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``[dashboard]`` section defines URLs for external dashboard services and integrations.

**Important Note:**

* This section is only for **external** dashboard services
* The built-in dashboard (available at ``http://your-refinery/dashboards``) requires no configuration
* Most installations do not need this section

**Format:**

.. code:: ini

   [dashboard]
   refinery = external_dashboard_url
   dashboards = external_dashboards_service_url

**Examples:**

.. code:: ini

   [dashboard]
   # External dashboard service for the refinery instance
   refinery = http://external-dashboard.company.com

   # Dedicated external dashboards service
   dashboards = http://external-dashboards.company.com/dashboards/

**Usage:**

* Configures integration with external dashboard services (not the built-in one)
* Used by the refinery web interface to link to external dashboard systems
* Allows for integration with third-party dashboard platforms

**Key Points:**

* Optional section - only needed if integrating with external dashboard services
* The built-in dashboard system requires no configuration entries
* URLs should point to accessible external dashboard service endpoints

Examples and Use Cases
-----------------------

Basic Single Instance Setup
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The minimal configuration for a local development setup:

.. code:: ini

   [dburi]
   refinery = postgresql:///refinery

This configuration:

* Uses local PostgreSQL database named "refinery"
* No external sources or authentication
* Built-in dashboard available at ``http://localhost:5000/dashboards``

Multi-Database Development Setup
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Configuration for developers working with multiple environments:

.. code:: ini

   [dburi]
   local = postgresql:///refinery_local
   staging = postgresql:///refinery_staging
   prod = postgresql:///refinery_prod

Usage:

* ``tsa = timeseries()`` connects to ``local`` (first entry)
* ``tsa = timeseries('postgresql:///refinery_staging')`` connects to staging
* Useful for testing migrations and data comparisons

Data Mesh Configuration with Client Authentication
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Production setup consuming data from external refinery instances:

.. code:: ini

   [dburi]
   company = postgresql://user:password@db.company.com:5432/refinery

   [sources]
   company.weather = https://refinery.weather.partner.com/api, tsh
   company.market = https://refinery.market.partner.com/api, tsh

   [auth]
   # Authentication for weather source (matches [sources] entry)
   weather.uri = https://refinery.weather.partner.com/api
   weather.login = your_username
   weather.password = your_password

   # Authentication for market source (matches [sources] entry)
   market.uri = https://refinery.market.partner.com/api
   market.client_id = your_client_id
   market.client_secret = your_client_secret
   market.domain = auth.market.partner.com/realms/api

**Key Point:**

* ``[auth]`` entries must correspond to sources defined in ``[sources]`` or ``[dburi]``
* ``[auth]`` is for authenticating as a **client** to external services

Enterprise Server Authentication Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Configuration for when your refinery acts as a **server** requiring
authentication:

.. code:: ini

   [dburi]
   production = postgresql://user:password@db.internal:5432/refinery_prod

   [server-auth]
   client_id = your_server_client_id
   client_secret = your_server_client_secret
   domain = openid.yourprovider.com/realms/your_realm
   authorize_uri = https://your.refinery.com/authorize
   audience = https://your.refinery.com/api

**Key Points:**

* ``[server-auth]`` configures your refinery instance as an OAuth2 **server**
* Used when other clients need to authenticate to access your refinery
* Different from ``[auth]`` which is for authenticating to external services
* Requires OpenID Connect provider configuration
