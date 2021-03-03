.. _ray-client:

**********
Ray Client
**********

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

=======================
Versioning requirements
=======================

Generally, the client Ray version must match the server Ray version. An error will be raised if an incompatible version is used.

Similarly, the minor Python (e.g., 3.6 vs 3.7) must match between the client and server. An error will be raised if this is not the case.
