======================================
Advanced Topics and Configurations
======================================

Ray Serve has a number of knobs and tools for you to tune for your particular workload.
All Ray Serve advanced options and topics are covered on this page aside from the
fundamentals of :doc:`deployment`. For a more hands-on take, please check out the :ref:`serve-tutorials`.

There are a number of things you'll likely want to do with your serving application including
scaling out, splitting traffic, or batching input for better performance. To do all of this,
you will create a ``BackendConfig``, a configuration object that you'll use to set
the properties of a particular backend.

.. _serve-sync-async-handles:

Sync and Async Handles
======================

Ray Serve offers two types of ``ServeHandle``. You can use the ``client.get_handle(..., sync=True|False)``
flag to toggle between them.

- When you set ``sync=True`` (the default), a synchronous handle is returned.
  Calling ``handle.remote()`` should return a Ray ObjectRef.
- When you set ``sync=False``, an asyncio based handle is returned. You need to
  Call it with ``await handle.remote()`` to return a Ray ObjectRef. To use ``await``,
  you have to run ``client.get_handle`` and ``handle.remote`` in Python asyncio event loop.

The async handle has performance advantage because it uses asyncio directly; as compared
to the sync handle, which talks to an asyncio event loop in a thread. To learn more about
the reasoning behind these, checkout our `architecture documentation <./architecture.html>`_.

Configuring HTTP Server Locations
=================================

By default, Ray Serve starts only one HTTP on the head node of the Ray cluster.
You can configure this behavior using the ``http_options={"location": ...}`` flag
in :mod:`serve.start <ray.serve.start>`:

- "HeadOnly": start one HTTP server on the head node. Serve
  assumes the head node is the node you executed serve.start
  on. This is the default.
- "EveryNode": start one HTTP server per node.
- "NoServer" or ``None``: disable HTTP server.

.. note::
   Using the "EveryNode" option, you can point a cloud load balancer to the
   instance group of Ray cluster to achieve high availability of Serve's HTTP
   proxies.

Variable HTTP Routes
====================

Ray Serve supports capturing path parameters.  For example, in a call of the form

.. code-block:: python

    client.create_endpoint("my_endpoint", backend="my_backend", route="/api/{username}")

the ``username`` parameter will be accessible in your backend code as follows:

.. code-block:: python

    def my_backend(request):
        username = request.path_params["username"]
        ...

Ray Serve uses Starlette's Router class under the hood for routing, so type
conversion for path parameters is also supported, as well as multiple path parameters.  
For example, suppose this route is used:

.. code-block:: python
    
    client.create_endpoint(
        "complex", backend="f", route="/api/{user_id:int}/{number:float}")

Then for a query to the route ``/api/123/3.14``, the ``request.path_params`` dictionary 
available in the backend will be ``{"user_id": 123, "number": 3.14}``, where ``123`` is
a Python int and ``3.14`` is a Python float.

For full details on the supported path parameters, see Starlette's
`path parameters documentation <https://www.starlette.io/routing/#path-parameters>`_.
