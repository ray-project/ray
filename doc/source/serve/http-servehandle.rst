=======================================
Calling Deployments via HTTP and Python
=======================================

This section should help you:

- understand how deployments can be called in two ways: from HTTP and from Python
- integrate Ray Serve with an existing web server

.. contents:: Calling Deployments via HTTP and Python

Calling Deployments via HTTP
============================

Basic Example
^^^^^^^^^^^^^

As shown in the :ref:`serve_quickstart`, when you create a deployment, it is exposed over HTTP by default at ``/{deployment_name}``. You can change the route by specifying the ``route_prefix`` argument to the :mod:`@serve.deployment <ray.serve.api.deployment>` decorator.

.. code-block:: python

    @serve.deployment(route_prefix="/counter")
    class Counter:
        def __call__(self, request):
            pass

When you make a request to the Serve HTTP server at ``/counter``, it will forward the request to the deployment's ``__call__`` method and provide a `Starlette Request object <https://www.starlette.io/requests/>`_ as the sole argument. The ``__call__`` method can return any JSON-serializable object or a `Starlette Response object <https://www.starlette.io/responses/>`_ (e.g., to return a custom status code).

Below, we discuss some advanced features for customizing Ray Serve's HTTP functionality.

.. _serve-fastapi-http:

FastAPI HTTP Deployments
^^^^^^^^^^^^^^^^^^^^^^^^

If you want to define more complex HTTP handling logic, Serve integrates with `FastAPI <https://fastapi.tiangolo.com/>`_. This allows you to define a Serve deployment using the :mod:`@serve.ingress <ray.serve.api.ingress>` decorator that wraps a FastAPI app with its full range of features. The most basic example of this is shown below, but for more details on all that FastAPI has to offer such as variable routes, automatic type validation, dependency injection (e.g., for database connections), and more, please check out `their documentation <https://fastapi.tiangolo.com/>`_.

.. code-block:: python

    import ray

    from fastapi import FastAPI
    from ray import serve

    app = FastAPI()
    ray.init(address="auto", namespace="summarizer")
    serve.start(detached=True)

    @serve.deployment(route_prefix="/hello")
    @serve.ingress(app)
    class MyFastAPIDeployment:
        @app.get("/")
        def root(self):
            return "Hello, world!"

    MyFastAPIDeployment.deploy()

Now if you send a request to ``/hello``, this will be routed to the ``root`` method of our deployment. We can also easily leverage FastAPI to define multiple routes with different HTTP methods:

.. code-block:: python

    import ray

    from fastapi import FastAPI
    from ray import serve

    app = FastAPI()
    ray.init(address="auto", namespace="summarizer")
    serve.start(detached=True)

    @serve.deployment(route_prefix="/hello")
    @serve.ingress(app)
    class MyFastAPIDeployment:
        @app.get("/")
        def root(self):
            return "Hello, world!"

        @app.post("/{subpath}")
        def root(self, subpath: str):
            return f"Hello from {subpath}!"

    MyFastAPIDeployment.deploy()

You can also pass in an existing FastAPI app to a deployment to serve it as-is:

.. code-block:: python

    import ray

    from fastapi import FastAPI
    from ray import serve

    app = FastAPI()
    ray.init(address="auto", namespace="summarizer")
    serve.start(detached=True)

    @app.get("/")
    def f():
        return "Hello from the root!"

    # ... add more routes, routers, etc. to `app` ...

    @serve.deployment(route_prefix="/")
    @serve.ingress(app)
    class FastAPIWrapper:
        pass

    FastAPIWrapper.deploy()

This is useful for scaling out an existing FastAPI app with no modifications necessary.
Existing middlewares, automatic OpenAPI documentation generation, and other advanced FastAPI features should work as-is.
You can also combine routes defined this way with routes defined on the deployment:

.. code-block:: python

    import ray

    from fastapi import FastAPI
    from ray import serve

    app = FastAPI()
    ray.init(address="auto", namespace="summarizer")
    serve.start(detached=True)

    @app.get("/")
    def f():
        return "Hello from the root!"

    @serve.deployment(route_prefix="/api1")
    @serve.ingress(app)
    class FastAPIWrapper1:
        @app.get("/subpath")
        def method(self):
            return "Hello 1!"

    @serve.deployment(route_prefix="/api2")
    @serve.ingress(app)
    class FastAPIWrapper2:
        @app.get("/subpath")
        def method(self):
            return "Hello 2!"

    FastAPIWrapper1.deploy()
    FastAPIWrapper2.deploy()

In this example, requests to both ``/api1`` and ``/api2`` would return ``Hello from the root!`` while a request to ``/api1/subpath`` would return ``Hello 1!`` and a request to ``/api2/subpath`` would return ``Hello 2!``.

To try it out, save a code snippet in a local python file (i.e. main.py) and in the same directory, run the following commands to start a local Ray cluster on your machine.

.. code-block:: bash

    ray start --head
    python main.py

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

ServeHandle: Calling Deployments from Python
============================================

Ray Serve enables you to query models both from HTTP and Python. This feature
enables seamless :ref:`model composition<serve-model-composition>`. You can
get a ``ServeHandle`` corresponding to deployment, similar how you can
reach a deployment through HTTP via a specific route. When you issue a request
to a deployment through ``ServeHandle``, the request is load balanced across
available replicas in the same way an HTTP request is.

To call a Ray Serve deployment from python, use :mod:`Deployment.get_handle <ray.serve.api.Deployment>`
to get a handle to the deployment, then use
:mod:`handle.remote <ray.serve.handle.RayServeHandle.remote>` to send requests
to that deployment. These requests can pass ordinary args and kwargs that are
passed directly to the method. This returns a Ray ``ObjectRef`` whose result
can be waited for or retrieved using ``ray.wait`` or ``ray.get``.

.. code-block:: python

    @serve.deployment
    class Deployment:
        def method1(self, arg):
            return f"Method1: {arg}"

        def __call__(self, arg):
            return f"__call__: {arg}"

    Deployment.deploy()

    handle = Deployment.get_handle()
    ray.get(handle.remote("hi")) # Defaults to calling the __call__ method.
    ray.get(handle.method1.remote("hi")) # Call a different method.

If you want to use the same deployment to serve both HTTP and ServeHandle traffic, the recommended best practice is to define an internal method that the HTTP handling logic will call:

.. code-block:: python

    @serve.deployment(route_prefix="/api")
    class Deployment:
        def say_hello(self, name: str):
            return f"Hello {name}!"

        def __call__(self, request):
            return self.say_hello(request.query_params["name"])

    Deployment.deploy()

Now we can invoke the same logic from both HTTP or Python:

.. code-block:: python

    print(requests.get("http://localhost:8000/api?name=Alice"))
    # Hello Alice!

    handle = Deployment.get_handle()
    print(ray.get(handle.say_hello.remote("Alice")))
    # Hello Alice!

.. _serve-sync-async-handles:

Sync and Async Handles
^^^^^^^^^^^^^^^^^^^^^^

Ray Serve offers two types of ``ServeHandle``. You can use the ``Deployment.get_handle(..., sync=True|False)``
flag to toggle between them.

- When you set ``sync=True`` (the default), a synchronous handle is returned.
  Calling ``handle.remote()`` should return a Ray ``ObjectRef``.
- When you set ``sync=False``, an asyncio based handle is returned. You need to
  Call it with ``await handle.remote()`` to return a Ray ObjectRef. To use ``await``,
  you have to run ``Deployment.get_handle`` and ``handle.remote`` in Python asyncio event loop.

The async handle has performance advantage because it uses asyncio directly; as compared
to the sync handle, which talks to an asyncio event loop in a thread. To learn more about
the reasoning behind these, checkout our `architecture documentation <./architecture.html>`_.

Integrating with existing web servers
=====================================

Ray Serve comes with its own HTTP server out of the box, but if you have an existing
web application, you can still plug in Ray Serve to scale up your compute using the ``ServeHandle``.
For a tutorial with sample code, see :ref:`serve-web-server-integration-tutorial`.
