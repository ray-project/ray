==========================================
Calling Endpoints via HTTP and ServeHandle
==========================================

.. contents:: Calling Endpoints via HTTP and ServeHandle

Overview
========

Ray Serve endpoints can be called in two ways: from HTTP and from Python.
On this page we will show you both of these approaches and then give a tutorial
on how to integrate Ray Serve with an existing web server.

Calling Endpoints via HTTP
==========================

As described in the :doc:`tutorial`, when you create a Ray Serve endpoint, to
serve it over HTTP you just need to specify the ``route`` parameter to ``serve.create_endpoint``:

.. code-block:: python

    serve.create_endpoint("my_endpoint", backend="my_backend", route="/counter")

Below, we discuss some advanced features for customizing Ray Serve's HTTP functionality:

Configuring HTTP Server Locations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, Ray Serve starts a single HTTP server on the head node of the Ray cluster.
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
^^^^^^^^^^^^^^^^^^^^

Ray Serve supports capturing path parameters.  For example, in a call of the form

.. code-block:: python

    serve.create_endpoint("my_endpoint", backend="my_backend", route="/api/{username}")

the ``username`` parameter will be accessible in your backend code as follows:

.. code-block:: python

    def my_backend(request):
        username = request.path_params["username"]
        ...

Ray Serve uses Starlette's Router class under the hood for routing, so type
conversion for path parameters is also supported, as well as multiple path parameters.  
For example, suppose this route is used:

.. code-block:: python
    
    serve.create_endpoint(
        "complex", backend="f", route="/api/{user_id:int}/{number:float}")

Then for a query to the route ``/api/123/3.14``, the ``request.path_params`` dictionary 
available in the backend will be ``{"user_id": 123, "number": 3.14}``, where ``123`` is
a Python int and ``3.14`` is a Python float.

For full details on the supported path parameters, see Starlette's
`path parameters documentation <https://www.starlette.io/routing/#path-parameters>`_.

Custom HTTP response status codes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can return a `Starlette Response object <https://www.starlette.io/responses/>`_ from your Ray Serve backend code:

.. code-block:: python

    from starlette.responses import Response

    def f(starlette_request):
        return Response('Hello, world!', status_code=123, media_type='text/plain')
    
    serve.create_backend("hello", f)

Enabling CORS and other HTTP middlewares
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Serve supports arbitrary `Starlette middlewares <https://www.starlette.io/middleware/>`_
and custom middlewares in Starlette format. The example below shows how to enable
`Cross-Origin Resource Sharing (CORS) <https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS>`_.
You can follow the same pattern for other Starlette middlewares.


.. code-block:: python

    from starlette.middleware import Middleware
    from starlette.middleware.cors import CORSMiddleware

    client = serve.start(
        http_options={"middlewares": [
            Middleware(
                CORSMiddleware, allow_origins=["*"], allow_methods=["*"])
        ]})

.. _serve-handle-explainer:

ServeHandle: Calling Endpoints from Python
================================================

Ray Serve enables you to query models both from HTTP and Python. This feature
enables seamless :ref:`model composition<serve-model-composition>`. You can
get a ``ServeHandle`` corresponding to an ``endpoint``, similar how you can
reach an endpoint through HTTP via a specific route. When you issue a request
to an endpoint through ``ServeHandle``, the request goes through the same code
path as an HTTP request would: choosing backends through :ref:`traffic
policies <serve-split-traffic>` and load balancing across available replicas.

To call a Ray Serve endpoint from python, use :mod:`serve.get_handle <ray.serve.api.get_handle>` 
to get a handle to the endpoint, then use 
:mod:`handle.remote <ray.serve.handle.RayServeHandle.remote>` to send requests to that
endpoint. This returns a Ray ObjectRef whose result can be waited for or retrieved using
``ray.wait`` or ``ray.get``, respectively.

.. code-block:: python

    handle = serve.get_handle("api_endpoint")
    ray.get(handle.remote(request))


Accessing data from the request
-------------------------------

When the request arrives in the model, you can access the data similarly to how
you would with an HTTP request. Here are some examples how Ray Serve's built-in 
``ServeRequest`` mirrors ```starlette.requests.request``:

.. list-table::
   :header-rows: 1

   * - HTTP
     - ServeHandle
     - | Request
       | (Starlette.Request and ServeRequest)
   * - ``requests.get(..., headers={...})``
     - ``handle.options(http_headers={...})``
     - ``request.headers``
   * - ``requests.post(...)``
     - ``handle.options(http_method="POST")``
     - ``request.method``
   * - ``requests.get(..., json={...})``
     - ``handle.remote({...})``
     - ``await request.json()``
   * - ``requests.get(..., form={...})``
     - ``handle.remote({...})``
     - ``await request.form()``
   * - ``requests.get(..., params={"a":"b"})``
     - ``handle.remote(a="b")``
     - ``request.query_params``
   * - ``requests.get(..., data="long string")``
     - ``handle.remote("long string")``
     - ``await request.body()``
   * - ``N/A``
     - ``handle.remote(python_object)``
     - ``request.data``

.. note::

    You might have noticed that the last row of the table shows that ``ServeRequest`` supports
    passing Python objects through the handle. This is not possible in HTTP. If you
    need to distinguish if the origin of the request is from Python or HTTP, you can do an ``isinstance``
    check:

    .. code-block:: python

        import starlette.requests

        if isinstance(request, starlette.requests.Request):
            print("Request coming from web!")
        elif isinstance(request, ServeRequest):
            print("Request coming from Python!")

.. note::

    One special case is when you pass a web request to a handle.

    .. code-block:: python

        handle.remote(starlette_request)

    In this case, Serve will `not` wrap it in ServeRequest. You can directly
    process the request as a ``starlette.requests.Request``.

.. _serve-sync-async-handles:

Sync and Async Handles
^^^^^^^^^^^^^^^^^^^^^^

Ray Serve offers two types of ``ServeHandle``. You can use the ``serve.get_handle(..., sync=True|False)``
flag to toggle between them.

- When you set ``sync=True`` (the default), a synchronous handle is returned.
  Calling ``handle.remote()`` should return a Ray ObjectRef.
- When you set ``sync=False``, an asyncio based handle is returned. You need to
  Call it with ``await handle.remote()`` to return a Ray ObjectRef. To use ``await``,
  you have to run ``serve.get_handle`` and ``handle.remote`` in Python asyncio event loop.

The async handle has performance advantage because it uses asyncio directly; as compared
to the sync handle, which talks to an asyncio event loop in a thread. To learn more about
the reasoning behind these, checkout our `architecture documentation <./architecture.html>`_.

.. _serve-custom-methods:

Calling methods on a Serve backend besides ``__call__``
=======================================================

By default, Ray Serve will serve the user-defined ``__call__`` method of your class, but 
other methods of your class can be served as well.

To call a custom method via HTTP, pass in the method name in the header field ``X-SERVE-CALL-METHOD``.

To call a custom method via Python, use :mod:`handle.options <ray.serve.handle.RayServeHandle.options>`:

.. code-block:: python

    class StatefulProcessor:
        def __init__(self):
            self.count = 1

        def __call__(self, request):
            return {"current": self.count}

        def other_method(self, inc):
            self.count += inc
            return True

    handle = serve.get_handle("endpoint_name")
    handle.options(method_name="other_method").remote(5)

The call is the same as a regular query except a different method is called
within the replica.

Integrating with existing web servers
=====================================

Ray Serve comes with its own HTTP server out of the box, but if you have an existing
web application, you can still plug in Ray Serve to scale up your backend computation.

Using ``ServeHandle`` makes this easy.  
For a tutorial with sample code, see :ref:`serve-web-server-integration-tutorial`.
