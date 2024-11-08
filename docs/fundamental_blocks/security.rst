Securing the Refinery
=====================

Two modalities are available:

* Http `basic-auth <https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication>`_

* `OpenID Connect <https://en.wikipedia.org/wiki/OpenID#OpenID_Connect_(OIDC)>`_


Setting up  basic auth
----------------------

Most of the setup happens with your web frontend, for instance `Nginx
<https://nginx.org/en/>`_. You should consult the relevant
documentation to set up the basic auth mechanism. This will provide a
simple login + password form in front of the application.

Now, if you want to make a data-mesh configuration work in this
context, you have to provide the credentials of the target instance in
the `tshistory.cfg` configuration file.

Let's start with a sample configuration with one external source:

.. code:: ini

 [dburi]
 power = postgresql://power

 [sources]
 power.meteo = https://refinery.meteo.acme.com/api


This describes a `power` refinery that also consumes meteorogical data
from a `meteo` instance. Now if you just put http basic auth in front
of your `meteo` instance, you have to add a new section like the
following:

.. code:: ini

  [auth]
  meteo.uri = https://refinery.meteo.acme.com/api
  meteo.login = <login>
  meteo.password = <password>



