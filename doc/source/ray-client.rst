**********
Ray Client
**********

.. note::

   This feature is still in beta and subject to changes.

===========
Basic usage
===========

The Ray client server is automatically started on port ``10001`` when you use ``ray start --head`` or Ray in an autoscaling cluster. The port can be changed by specifying --ray-client-server-port in the ``ray start`` command.

To start the server manually, you can run:

``python -m ray.util.client.server [--host host_ip] [--port port] [--redis-address address] [--redis-password password]``

This runs ``ray.init()`` with default options and exposes the client gRPC port at ``host_ip:port`` (by default, ``0.0.0.0:10001``). Providing ``redis-address`` and ``redis-password`` will be passed into ``ray.init()`` when the server starts, allowing connection to an existing Ray cluster, as per the `cluster setup <cluster/index.html>`_ instructions.

From here, another Ray script can access that server from a networked machine with ``ray.util.connect()``

.. code-block:: python

   import ray
   import ray.util

   ray.util.connect("<head_node_host>:10001")  # replace with the appropriate host and port

   # Normal Ray code follows
   @ray.remote
   def f(x):
       return x ** x

   do_work.remote(2)
   #....
  
When the client disconnects, any object or actor references held by the server on behalf of the client are dropped, as if directly disconnecting from the cluster.

============
Known issues
============

Because Ray client mode affects the behavior of the Ray API, larger scripts or libraries imported before ``ray.util.connect()`` may not realize they're in client mode. This feature is being tracked with `issue #13272 <https://github.com/ray-project/ray/issues/13272>`_ but the workaround here is provided for beta users.

One option is to defer the imports from a ``main`` script that calls ``ray.util.connect()`` first. However, some older scripts or libraries might not support that.

Therefore, an environment variable is also available to force a Ray program into client mode: ``RAY_CLIENT_MODE`` An example usage:

.. code-block:: bash

   RAY_CLIENT_MODE=1 python my_ray_program.py
