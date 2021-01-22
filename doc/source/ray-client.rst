**********
Ray Client
**********

.. note::

   This feature is still in beta and subject to changes.

===========
Basic usage
===========

While in beta, the server is available as an executable module. To start the server, run

``python -m ray.util.client.server [--host host_ip] [--port port] [--redis-address address] [--redis-password password]``

This runs ``ray.init()`` with default options and exposes the client gRPC port at ``host_ip:port`` (by default, ``0.0.0.0:50051``). Providing ``redis-address`` and ``redis-password`` will be passed into ``ray.init()`` when the server starts, allowing connection to an existing Ray cluster, as per the `cluster setup <cluster/index.html>`_ instructions.

From here, another Ray script can access that server from a networked machine with ``ray.util.connect()``

.. code-block:: python

   import ray
   import ray.util

   ray.util.connect("0.0.0.0:50051")  # replace with the appropriate host and port

   # Normal Ray code follows
   @ray.remote
   def f(x):
       return x ** x

   do_work.remote(2)
   #....

When the client disconnects, any object or actor references held by the server on behalf of the client are dropped, as if directly disconnecting from the cluster


===================
``RAY_CLIENT_MODE``
===================

Because Ray client mode affects the behavior of the Ray API, larger scripts or libraries imported before ``ray.util.connect()`` may not realize they're in client mode. This feature is being tracked with `issue #13272 <https://github.com/ray-project/ray/issues/13272>`_ but the workaround here is provided for beta users.

One option is to defer the imports from a ``main`` script that calls ``ray.util.connect()`` first. However, some older scripts or libraries might not support that.

Therefore, an environment variable is also available to force a Ray program into client mode: ``RAY_CLIENT_MODE`` An example usage:

.. code-block:: bash

   RAY_CLIENT_MODE=1 python my_ray_program.py


===================================
Programatically creating the server
===================================

For larger use-cases, it may be desirable to connect remote Ray clients to an existing Ray environment. The server can be started separately via

.. code-block:: python

   from ray.util.client.server import serve

   server = serve("0.0.0.0:50051")
   # Server does some work
   # ...
   # Time to clean up
   server.stop(0)

